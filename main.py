from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from fastapi.responses import PlainTextResponse, JSONResponse
from urllib.parse import parse_qs
import hmac
import json
import os
import re
import sqlite3
import urllib.request

app = FastAPI()

# CORS: dominios del panel (Webflow). Configurable con CORS_ORIGINS separado por comas.
from fastapi.middleware.cors import CORSMiddleware
CORS_ORIGINS = [o.strip() for o in os.environ.get("CORS_ORIGINS", "https://prueba-carsmid.webflow.io").split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_methods=["GET", "POST", "PATCH", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type"],
)

# Token de verificacion del webhook de Meta: se configura en Railway (VERIFY_TOKEN).
VERIFY_TOKEN = os.environ.get("VERIFY_TOKEN", "")
DB_PATH = os.environ.get("DB_PATH", "leads.db")

# Estatus del cliente (los mismos que usa el panel maestro).
ESTATUS = ["nuevo", "contactado", "en_proceso", "cita_agendada", "en_tramite",
           "no_contesta", "vendido", "cancelado"]
MOTIVOS = ["compro_otro_lugar", "no_califico_credito", "precio", "auto_vendido",
           "solo_cotizando", "otro"]
# Si un cliente en estos estatus vuelve a escribir por una campana, regresa a "nuevo".
REABREN = {"cancelado", "no_contesta"}
# Valores viejos del panel -> estatus nuevos.
MAPEO_VIEJO = {"": "nuevo", "no_interesado": "cancelado", "no_contesto": "no_contesta"}


def _add_cols(conn, table, cols):
    have = {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
    for name, decl in cols:
        if name not in have:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {decl}")


def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wa_id TEXT,
            phone TEXT,
            name TEXT,
            timestamp TEXT,
            message TEXT,
            campaign TEXT,
            result TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    # result = estatus del cliente. Migraciones seguras (solo agregan columnas).
    _add_cols(conn, "leads", [
        ("origen", "TEXT DEFAULT 'carsmid'"),
        ("numero_destino", "TEXT"),      # numero de WhatsApp al que escribio
        ("anuncio_id", "TEXT"),          # anuncio del ultimo contacto (Meta referral.source_id)
        ("ultimo_contacto", "DATETIME"),
        ("motivo", "TEXT"),              # motivo de cancelacion
        ("nota_motivo", "TEXT"),
        ("vendedor", "TEXT"),
        ("auto_interes", "TEXT"),
        ("actualizado_en", "DATETIME"),
        ("actualizado_por", "TEXT"),
    ])
    conn.execute("""
        CREATE TABLE IF NOT EXISTS lead_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lead_id INTEGER NOT NULL,
            old_result TEXT,
            new_result TEXT,
            changed_by TEXT,
            changed_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    # source: panel | panel_maestro | sistema
    _add_cols(conn, "lead_history", [("source", "TEXT"), ("motivo", "TEXT")])
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ventas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            venta_id TEXT UNIQUE,
            phone TEXT,
            auto TEXT,
            monto REAL,
            comision REAL,
            fecha TEXT,
            lead_id INTEGER,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    _add_cols(conn, "ventas", [("forma_pago", "TEXT"), ("vendedor", "TEXT")])
    # Cada vez que el cliente llega (primer mensaje o regreso por un anuncio).
    conn.execute("""
        CREATE TABLE IF NOT EXISTS entradas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lead_id INTEGER,
            phone TEXT,
            name TEXT,
            origen TEXT,
            numero_destino TEXT,
            anuncio_id TEXT,
            anuncio_titulo TEXT,
            mensaje TEXT,
            wa_id TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    # Avisos del panel maestro cuyo telefono no corresponde a ningun lead (para revisar).
    conn.execute("""
        CREATE TABLE IF NOT EXISTS avisos_sin_lead (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tipo TEXT,
            phone TEXT,
            payload TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    # Pasar los resultados viejos a los estatus nuevos.
    conn.execute("UPDATE leads SET result = 'nuevo' WHERE result IS NULL OR result = ''")
    for old, new in MAPEO_VIEJO.items():
        if old:
            conn.execute("UPDATE leads SET result = ? WHERE result = ?", (new, old))
    conn.execute("UPDATE leads SET ultimo_contacto = created_at WHERE ultimo_contacto IS NULL")
    conn.commit()
    conn.close()


def normalize_phone(phone):
    """Deja solo digitos y conserva los ultimos 10 (quita prefijos 52 / 521 de Mexico)."""
    digits = re.sub(r"\D", "", str(phone or ""))
    return digits[-10:] if len(digits) > 10 else digits


def detect_origen(value, msg):
    """Regla de origen:
    - Si el payload trae un campo explicito "origen" (en value o en el mensaje) con
      valor "carsmid" o "tuyo", se usa ese.
    - Si no, si la referencia del anuncio (msg.referral: source_url, headline, body,
      source_id) o el texto del mensaje mencionan "tuyo", el origen es "tuyo".
    - En cualquier otro caso: "carsmid".
    """
    for src in (msg, value):
        explicit = str(src.get("origen", "")).strip().lower()
        if explicit in ("carsmid", "tuyo"):
            return explicit
    referral = msg.get("referral") or {}
    text = " ".join(str(referral.get(k, "")) for k in ("source_url", "headline", "body", "source_id"))
    text += " " + str((msg.get("text") or {}).get("body", ""))
    return "tuyo" if "tuyo" in text.lower() else "carsmid"


def _env(name):
    return os.environ.get(name, "")


def _bearer_ok(request: Request, env_name: str) -> bool:
    token = _env(env_name)
    auth = request.headers.get("authorization", "")
    return bool(token) and hmac.compare_digest(auth.encode(), ("Bearer " + token).encode())


def _lead_by_phone(conn, phone):
    row = conn.execute("SELECT * FROM leads WHERE phone = ? ORDER BY id DESC LIMIT 1", (phone,)).fetchone()
    return row


def _set_result(conn, lead_id, new_result, changed_by, source="panel", motivo=None):
    """Cambia el estatus de un lead y deja registro en lead_history."""
    row = conn.execute("SELECT result FROM leads WHERE id = ?", (lead_id,)).fetchone()
    if row is None:
        return False
    old = row[0] or ""
    if old != new_result:
        conn.execute(
            "UPDATE leads SET result = ?, actualizado_en = CURRENT_TIMESTAMP, actualizado_por = ? WHERE id = ?",
            (new_result, changed_by, lead_id))
        if new_result != "cancelado":
            conn.execute("UPDATE leads SET motivo = NULL, nota_motivo = NULL WHERE id = ?", (lead_id,))
        conn.execute(
            "INSERT INTO lead_history (lead_id, old_result, new_result, changed_by, source, motivo) VALUES (?, ?, ?, ?, ?, ?)",
            (lead_id, old, new_result, changed_by, source, motivo),
        )
    return True


init_db()


# ---------- Aviso de nueva entrada al panel maestro ----------
def _avisar_maestro(payload):
    """POST al Worker del panel maestro (MAESTRO_WEBHOOK_URL). Si no esta configurado, no hace nada."""
    url = _env("MAESTRO_WEBHOOK_URL")
    if not url:
        return
    headers = {"Content-Type": "application/json"}
    if _env("MAESTRO_WEBHOOK_TOKEN"):
        headers["Authorization"] = "Bearer " + _env("MAESTRO_WEBHOOK_TOKEN")
    req = urllib.request.Request(url, data=json.dumps(payload).encode(), headers=headers, method="POST")
    for _ in range(3):
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                if 200 <= r.status < 300:
                    return
        except Exception as e:
            print("aviso maestro fallo:", e)


# ---------- WhatsApp (Meta) ----------
@app.get("/webhook")
async def verify(request: Request):
    params = request.query_params
    mode = params.get("hub.mode")
    token = params.get("hub.verify_token") or ""
    challenge = params.get("hub.challenge")
    if mode == "subscribe" and VERIFY_TOKEN and hmac.compare_digest(token.encode(), VERIFY_TOKEN.encode()):
        return PlainTextResponse(challenge)
    raise HTTPException(status_code=403, detail="Verification failed")


def _registrar_mensaje(conn, value, msg, contact):
    """Un telefono = un lead. Devuelve la entrada nueva (dict) o None si solo fue un mensaje mas."""
    phone = normalize_phone(msg.get("from"))
    if not phone:
        return None
    name = ((contact or {}).get("profile") or {}).get("name") or ""
    texto = (msg.get("text") or {}).get("body", "")
    destino = normalize_phone((value.get("metadata") or {}).get("display_phone_number")) or None
    referral = msg.get("referral") or {}
    anuncio_id = str(referral.get("source_id") or "") or None
    anuncio_titulo = str(referral.get("headline") or "") or None
    origen = detect_origen(value, msg)

    lead = _lead_by_phone(conn, phone)
    if lead is None:
        cur = conn.execute(
            """INSERT INTO leads (wa_id, phone, name, timestamp, message, origen, numero_destino,
                   anuncio_id, campaign, result, ultimo_contacto)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'nuevo', CURRENT_TIMESTAMP)""",
            (msg.get("id"), phone, name, msg.get("timestamp"), texto, origen, destino,
             anuncio_id, anuncio_titulo))
        lead_id, es_entrada = cur.lastrowid, True
    else:
        lead_id = lead["id"]
        es_entrada = anuncio_id is not None  # regreso por un anuncio
        if es_entrada:
            conn.execute(
                """UPDATE leads SET ultimo_contacto = CURRENT_TIMESTAMP, anuncio_id = ?, campaign = ?,
                       numero_destino = COALESCE(?, numero_destino), origen = ? WHERE id = ?""",
                (anuncio_id, anuncio_titulo, destino, origen, lead_id))
            if (lead["result"] or "") in REABREN:
                _set_result(conn, lead_id, "nuevo", "sistema", source="sistema")
        else:
            conn.execute("UPDATE leads SET ultimo_contacto = CURRENT_TIMESTAMP WHERE id = ?", (lead_id,))
    if not es_entrada:
        return None
    cur = conn.execute(
        """INSERT INTO entradas (lead_id, phone, name, origen, numero_destino, anuncio_id,
               anuncio_titulo, mensaje, wa_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (lead_id, phone, name, origen, destino, anuncio_id, anuncio_titulo, texto, msg.get("id")))
    return dict(conn.execute("SELECT * FROM entradas WHERE id = ?", (cur.lastrowid,)).fetchone())


@app.post("/webhook")
async def receive(request: Request, background: BackgroundTasks):
    try:
        data = await request.json()
    except Exception:
        return {"status": "ok"}
    nuevas = []
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        for entry in data.get("entry") or []:
            for change in entry.get("changes") or []:
                value = change.get("value") or {}
                contacts = {c.get("wa_id"): c for c in value.get("contacts") or []}
                first_contact = (value.get("contacts") or [None])[0]
                for msg in value.get("messages") or []:
                    contact = contacts.get(msg.get("from")) or first_contact
                    e = _registrar_mensaje(conn, value, msg, contact)
                    if e:
                        nuevas.append(e)
        conn.commit()
    except Exception as e:
        print(e)
    finally:
        conn.close()
    for e in nuevas:
        background.add_task(_avisar_maestro, {"tipo": "entrada", **e})
    return {"status": "ok"}


# ---------- Login y lectura del panel ----------
@app.post("/login")
async def login(request: Request):
    username = password = ""
    raw = await request.body()
    ctype = request.headers.get("content-type", "")
    try:
        if "application/json" in ctype:
            data = await request.json()
            username, password = str(data.get("username", "")), str(data.get("password", ""))
        else:
            form = parse_qs(raw.decode("utf-8"))
            username = form.get("username", [""])[0]
            password = form.get("password", [""])[0]
    except Exception:
        pass
    user_env, pass_env, token = _env("LOGIN_USER"), _env("LOGIN_PASS"), _env("API_TOKEN")
    if not (user_env and pass_env and token):
        return JSONResponse({"detail": "Login not configured"}, status_code=500)
    ok_user = hmac.compare_digest(username.encode(), user_env.encode())
    ok_pass = hmac.compare_digest(password.encode(), pass_env.encode())
    if not (username and password and ok_user and ok_pass):
        return JSONResponse({"detail": "Invalid credentials"}, status_code=401)
    return {"token": token}


@app.get("/leads")
async def leads(request: Request):
    """Un registro por telefono (el mas reciente), con los datos de su ultima venta."""
    if not _bearer_ok(request, "API_TOKEN"):
        return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = [dict(r) for r in conn.execute("""
        SELECT l.*,
               v.venta_id AS sale_id, v.auto AS sale_auto, v.monto AS sale_monto,
               v.comision AS sale_comision, v.vendedor AS sale_vendedor,
               v.forma_pago AS sale_forma_pago, v.fecha AS sale_fecha
        FROM leads l
        LEFT JOIN ventas v ON v.id = (SELECT MAX(id) FROM ventas WHERE lead_id = l.id)
        WHERE l.id = (SELECT MAX(id) FROM leads l2 WHERE l2.phone = l.phone)
        ORDER BY l.id DESC""")]
    conn.close()
    return rows


@app.get("/estatus")
async def catalogo():
    """Catalogo de estatus y motivos (publico, sin datos de clientes)."""
    return {"estatus": ESTATUS, "motivos": MOTIVOS}


# ---------- Panel: cambiar estatus a mano (temporal, hasta que el panel maestro este listo) ----------
@app.patch("/leads/{lead_id}")
async def update_lead(lead_id: int, request: Request):
    """Body JSON {result, password}. Bearer API_TOKEN.
    200 lead actualizado · 400 estatus invalido · 401 sin token o contrasena incorrecta · 404 no existe."""
    if not _bearer_ok(request, "API_TOKEN"):
        return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    if _env("PANEL_SOLO_LECTURA") == "1":
        return JSONResponse({"detail": "Read only"}, status_code=403)
    try:
        data = await request.json()
    except Exception:
        return JSONResponse({"detail": "Invalid JSON"}, status_code=400)
    password = str(data.get("password", ""))
    pass_env = _env("LOGIN_PASS")
    if not (password and pass_env and hmac.compare_digest(password.encode(), pass_env.encode())):
        return JSONResponse({"detail": "Invalid password"}, status_code=401)
    new_result = str(data.get("result") or "").strip().lower()
    if new_result not in ESTATUS:
        return JSONResponse({"detail": "Invalid result", "allowed": ESTATUS}, status_code=400)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        if not _set_result(conn, lead_id, new_result, _env("LOGIN_USER") or "panel", source="panel"):
            return JSONResponse({"detail": "Not found"}, status_code=404)
        conn.commit()
        return dict(conn.execute("SELECT * FROM leads WHERE id = ?", (lead_id,)).fetchone())
    finally:
        conn.close()


@app.get("/leads/{lead_id}/history")
async def lead_history(lead_id: int, request: Request):
    if not _bearer_ok(request, "API_TOKEN"):
        return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = [dict(r) for r in conn.execute(
        "SELECT * FROM lead_history WHERE lead_id = ? ORDER BY id DESC", (lead_id,))]
    conn.close()
    return rows


# ---------- Panel maestro (Cloudflare Worker) -> backend ----------
def _guardar_sin_lead(conn, tipo, phone, data):
    conn.execute("INSERT INTO avisos_sin_lead (tipo, phone, payload) VALUES (?, ?, ?)",
                 (tipo, phone, json.dumps(data, ensure_ascii=False)))


@app.post("/estatus")
async def recibir_estatus(request: Request):
    """Bearer VENTAS_TOKEN (token del panel maestro).
    Body JSON {telefono, estatus, motivo?, nota?, cambiado_por?, vendedor?, auto_interes?, nombre?, campana?}.
    Refleja el estatus del panel maestro en el lead de ese telefono."""
    if not _env("VENTAS_TOKEN"):
        return JSONResponse({"detail": "Not configured"}, status_code=500)
    if not _bearer_ok(request, "VENTAS_TOKEN"):
        return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    try:
        data = await request.json()
    except Exception:
        return JSONResponse({"detail": "Invalid JSON"}, status_code=400)
    phone = normalize_phone(data.get("telefono") or data.get("phone"))
    if len(phone) != 10:
        return JSONResponse({"detail": "telefono must have 10 digits"}, status_code=400)
    estatus = str(data.get("estatus") or "").strip().lower()
    if estatus not in ESTATUS:
        return JSONResponse({"detail": "Invalid estatus", "allowed": ESTATUS}, status_code=400)
    motivo = str(data.get("motivo") or "").strip().lower() or None
    nota = str(data.get("nota") or "").strip() or None
    if estatus == "cancelado":
        if motivo not in MOTIVOS:
            return JSONResponse({"detail": "motivo required for cancelado", "allowed": MOTIVOS}, status_code=400)
        if motivo == "otro" and not nota:
            return JSONResponse({"detail": "nota required when motivo is otro"}, status_code=400)
    else:
        motivo = nota = None
    quien = str(data.get("cambiado_por") or "").strip() or "panel_maestro"

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        lead = _lead_by_phone(conn, phone)
        if lead is None:
            _guardar_sin_lead(conn, "estatus", phone, data)
            conn.commit()
            return {"matched": False, "telefono": phone}
        lead_id = lead["id"]
        _set_result(conn, lead_id, estatus, quien, source="panel_maestro", motivo=motivo)
        extras = {"motivo": motivo, "nota_motivo": nota}
        for campo, clave in (("vendedor", "vendedor"), ("auto_interes", "auto_interes"),
                             ("name", "nombre"), ("campaign", "campana")):
            v = str(data.get(clave) or "").strip()
            if v:
                extras[campo] = v
        sets = ", ".join(f"{k} = ?" for k in extras)
        conn.execute(f"UPDATE leads SET {sets} WHERE id = ?", (*extras.values(), lead_id))
        conn.commit()
        return {"matched": True, "telefono": phone, "lead_id": lead_id, "estatus": estatus}
    finally:
        conn.close()


@app.post("/ventas")
async def create_venta(request: Request):
    """Bearer VENTAS_TOKEN. Body JSON {venta_id?, telefono|phone, auto?, precio|monto?, comision?,
    forma_pago?, vendedor?, fecha_venta|fecha?}. Marca como vendido el lead de ese telefono.
    Si llega dos veces el mismo venta_id, no se duplica."""
    if not _env("VENTAS_TOKEN"):
        return JSONResponse({"detail": "Ventas not configured"}, status_code=500)
    if not _bearer_ok(request, "VENTAS_TOKEN"):
        return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    try:
        data = await request.json()
    except Exception:
        return JSONResponse({"detail": "Invalid JSON"}, status_code=400)
    phone = normalize_phone(data.get("telefono") or data.get("phone"))
    if len(phone) != 10:
        return JSONResponse({"detail": "phone must have 10 digits"}, status_code=400)

    def num(v):
        try:
            return float(v) if v not in (None, "") else None
        except (TypeError, ValueError):
            return None

    def txt(*keys):
        for k in keys:
            v = str(data.get(k) or "").strip()
            if v:
                return v
        return None

    venta_id = txt("venta_id")
    vendedor = txt("vendedor")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        if venta_id:
            prev = conn.execute("SELECT id, lead_id FROM ventas WHERE venta_id = ?", (venta_id,)).fetchone()
            if prev:
                return {"id": prev[0], "venta_id": venta_id, "matched": prev[1] is not None,
                        "lead_id": prev[1], "duplicate": True}
        lead = _lead_by_phone(conn, phone)
        lead_id = lead["id"] if lead else None
        cur = conn.execute(
            """INSERT INTO ventas (venta_id, phone, auto, monto, comision, fecha, lead_id, forma_pago, vendedor)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (venta_id, phone, txt("auto"), num(data.get("precio", data.get("monto"))),
             num(data.get("comision")), txt("fecha_venta", "fecha"), lead_id,
             txt("forma_pago"), vendedor),
        )
        if lead_id is not None:
            _set_result(conn, lead_id, "vendido", vendedor or "panel_maestro", source="panel_maestro")
            if vendedor:
                conn.execute("UPDATE leads SET vendedor = ? WHERE id = ?", (vendedor, lead_id))
        conn.commit()
        return {"id": cur.lastrowid, "venta_id": venta_id, "matched": lead_id is not None,
                "lead_id": lead_id, "duplicate": False}
    finally:
        conn.close()


@app.get("/ventas")
async def list_ventas(request: Request):
    """Bearer API_TOKEN. Ventas registradas; lead_id vacio = venta sin lead."""
    if not _bearer_ok(request, "API_TOKEN"):
        return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = [dict(r) for r in conn.execute("SELECT * FROM ventas ORDER BY id DESC")]
    conn.close()
    return rows


@app.get("/entradas")
async def list_entradas(request: Request, desde_id: int = 0):
    """Bearer VENTAS_TOKEN. Entradas nuevas para el panel maestro (alternativa al aviso):
    devuelve las entradas con id > desde_id, maximo 500."""
    if not _bearer_ok(request, "VENTAS_TOKEN"):
        return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = [dict(r) for r in conn.execute(
        "SELECT * FROM entradas WHERE id > ? ORDER BY id LIMIT 500", (desde_id,))]
    conn.close()
    return rows
