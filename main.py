from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from fastapi.responses import PlainTextResponse, JSONResponse
from urllib.parse import parse_qs
import hmac
import json
import os
import re
import sqlite3
import urllib.request
from datetime import datetime, timezone

app = FastAPI()

# CORS: dominios del panel (Webflow). Configurable con CORS_ORIGINS separado por comas.
from fastapi.middleware.cors import CORSMiddleware
CORS_ORIGINS = [o.strip() for o in os.environ.get("CORS_ORIGINS", "https://prueba-carsmid.webflow.io").split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_methods=["GET", "POST", "PATCH", "DELETE", "OPTIONS"],
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
        ("auto_interes", "TEXT"),        # texto armado: "Nissan Versa 2020"
        ("auto_marca", "TEXT"),
        ("auto_modelo", "TEXT"),
        ("auto_anio", "INTEGER"),
        ("auto_precio", "REAL"),
        ("actualizado_en", "DATETIME"),
        ("actualizado_por", "TEXT"),
        ("estatus_maestro", "TEXT"),     # etapa tal cual del panel maestro (nuevo..perdido)
        ("fechas_estatus", "TEXT"),      # JSON {etapa: ISO} del panel maestro
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
    # Panel maestro: ultima foto del cliente (por telefono) y bitacora de eventos.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS maestro (
            phone TEXT PRIMARY KEY,
            lead_id INTEGER,
            payload TEXT,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS maestro_eventos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT UNIQUE,
            phone TEXT,
            lead_id INTEGER,
            event TEXT,
            payload TEXT,
            ts TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    # Gasto de Meta Ads capturado a mano (por campana y periodo).
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gasto (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            campana TEXT NOT NULL,
            desde TEXT NOT NULL,
            hasta TEXT NOT NULL,
            monto REAL NOT NULL,
            creado_por TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    _add_cols(conn, "ventas", [("anulada", "INTEGER DEFAULT 0")])
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


def _set_result(conn, lead_id, new_result, changed_by, source="panel", motivo=None, record=True):
    """Cambia el estatus de un lead y deja registro en lead_history (record=False si ya queda en maestro_eventos)."""
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
        if record:
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
        LEFT JOIN ventas v ON v.id = (SELECT MAX(id) FROM ventas WHERE lead_id = l.id AND COALESCE(anulada, 0) = 0)
        WHERE l.id = (SELECT MAX(id) FROM leads l2 WHERE l2.phone = l.phone)
        ORDER BY l.id DESC""")]
    # Datos del panel maestro: ultima foto (maestro) y ultimos 50 eventos (maestro_eventos).
    fotos = {r["phone"]: r["payload"] for r in conn.execute("SELECT phone, payload FROM maestro")}
    eventos = {}
    for r in conn.execute("SELECT phone, event, payload, ts FROM maestro_eventos ORDER BY ts DESC, id DESC"):
        lst = eventos.setdefault(r["phone"], [])
        if len(lst) < 50:
            lst.append({"event": r["event"], "payload": _loads(r["payload"]), "ts": r["ts"]})
    conn.close()
    for row in rows:
        if row["phone"] in fotos:
            row["maestro"] = _loads(fotos[row["phone"]])
            row["maestro_eventos"] = list(reversed(eventos.get(row["phone"], [])))
    return rows


@app.get("/estatus")
async def catalogo():
    """Catalogo de estatus y motivos (publico, sin datos de clientes)."""
    return {"estatus": ESTATUS, "motivos": MOTIVOS, "estatus_maestro": list(MAESTRO_A_RAILWAY),
            "maestro_a_railway": MAESTRO_A_RAILWAY}


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
def _num(v):
    try:
        return float(str(v).replace(",", "").replace("$", "")) if v not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _guardar_sin_lead(conn, tipo, phone, data):
    conn.execute("INSERT INTO avisos_sin_lead (tipo, phone, payload) VALUES (?, ?, ?)",
                 (tipo, phone, json.dumps(data, ensure_ascii=False)))


@app.post("/estatus")
async def recibir_estatus(request: Request):
    """Bearer VENTAS_TOKEN (token del panel maestro).
    Body JSON {telefono, estatus?, motivo?, nota?, cambiado_por?, vendedor?, nombre?, campana?,
               auto?: {marca, modelo, anio, precio}}.
    Refleja en el lead de ese telefono el estatus y el auto de interes que tiene el panel maestro.
    Sin "estatus" solo actualiza los demas datos (por ejemplo, al asignar el auto)."""
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
    estatus = str(data.get("estatus") or "").strip().lower() or None
    if estatus is not None and estatus not in ESTATUS:
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
        extras = {}
        if estatus:
            _set_result(conn, lead_id, estatus, quien, source="panel_maestro", motivo=motivo)
            extras.update({"motivo": motivo, "nota_motivo": nota})
        for campo, clave in (("vendedor", "vendedor"), ("name", "nombre"), ("campaign", "campana")):
            v = str(data.get(clave) or "").strip()
            if v:
                extras[campo] = v
        auto = data.get("auto") if isinstance(data.get("auto"), dict) else {}
        marca = str(auto.get("marca") or data.get("auto_marca") or "").strip() or None
        modelo = str(auto.get("modelo") or data.get("auto_modelo") or "").strip() or None
        anio = _num(auto.get("anio", auto.get("año", data.get("auto_anio"))))
        precio = _num(auto.get("precio", data.get("auto_precio")))
        texto = str(data.get("auto_interes") or "").strip() or None
        if marca or modelo or anio or precio or texto:
            extras.update({"auto_marca": marca, "auto_modelo": modelo,
                           "auto_anio": int(anio) if anio else None, "auto_precio": precio,
                           "auto_interes": texto or " ".join(str(x) for x in (marca, modelo, int(anio) if anio else None) if x) or None})
        if not extras:
            return JSONResponse({"detail": "Nothing to update"}, status_code=400)
        sets = ", ".join(f"{k} = ?" for k in extras)
        conn.execute(f"UPDATE leads SET {sets} WHERE id = ?", (*extras.values(), lead_id))
        conn.commit()
        return {"matched": True, "telefono": phone, "lead_id": lead_id, "estatus": estatus or lead["result"]}
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


# ---------- Panel maestro: eventos (via Cloudflare) ----------
EVENTOS = {"cliente_creado", "cliente_actualizado", "estatus_cambiado", "precio_ajustado", "venta", "venta_anulada"}
# Embudo del maestro -> estatus de Railway (para historial, filtros y metricas).
MAESTRO_A_RAILWAY = {"nuevo": "nuevo", "contactado": "contactado", "cita": "cita_agendada", "visito": "en_proceso",
                     "negociacion": "en_proceso", "credito": "en_tramite", "apartado": "en_tramite",
                     "vendido": "vendido", "perdido": "cancelado"}
PERDIDA_A_MOTIVO = {"precio": "precio", "credito_rechazado": "no_califico_credito", "compro_otro_lado": "compro_otro_lugar",
                    "no_hay_unidad": "auto_vendido", "solo_cotizaba": "solo_cotizando"}


def _loads(txt):
    try:
        return json.loads(txt) if txt else None
    except Exception:
        return None


def _maestro_ok(request: Request) -> bool:
    return _bearer_ok(request, "MAESTRO_TOKEN") or _bearer_ok(request, "VENTAS_TOKEN")


def _procesar_evento(conn, ev):
    """Guarda un evento del maestro, actualiza la foto del cliente y refleja estatus, auto y venta en el lead."""
    if not isinstance(ev, dict):
        return {"ok": False, "detail": "evento invalido"}
    event = str(ev.get("event") or ev.get("evento") or "").strip().lower()
    payload = ev.get("payload") if isinstance(ev.get("payload"), dict) else {}
    if event not in EVENTOS:
        return {"ok": False, "detail": "event invalido", "allowed": sorted(EVENTOS)}
    phone = normalize_phone(payload.get("telefono") or payload.get("phone") or ev.get("telefono"))
    if len(phone) != 10:
        return {"ok": False, "detail": "telefono must have 10 digits"}
    ts = str(ev.get("ts") or payload.get("updated_at") or "").strip() or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    event_id = str(ev.get("id") or ev.get("event_id") or payload.get("event_id") or "").strip() or None
    if event_id and conn.execute("SELECT 1 FROM maestro_eventos WHERE event_id = ?", (event_id,)).fetchone():
        return {"ok": True, "duplicate": True, "event_id": event_id}

    lead = None
    if payload.get("lead_id") not in (None, ""):
        lead = conn.execute("SELECT * FROM leads WHERE id = ?", (payload.get("lead_id"),)).fetchone()
        if lead is not None and lead["phone"] != phone:
            lead = None
    if lead is None:
        lead = _lead_by_phone(conn, phone)
    creado = False
    if lead is None:
        # Cliente capturado en el maestro sin lead previo: se crea el lead (origen del payload o carsmid).
        org = str(payload.get("origen") or "").strip().lower()
        org = org if org in ("carsmid", "tuyo") else "carsmid"
        cur = conn.execute(
            "INSERT INTO leads (phone, name, timestamp, message, campaign, result, origen, ultimo_contacto, actualizado_por) "
            "VALUES (?, ?, ?, ?, ?, 'nuevo', ?, CURRENT_TIMESTAMP, 'panel_maestro')",
            (phone, str(payload.get("nombre") or "").strip() or None, ts, "Alta desde panel maestro",
             str(payload.get("campana") or "").strip() or None, org))
        lead = conn.execute("SELECT * FROM leads WHERE id = ?", (cur.lastrowid,)).fetchone()
        creado = True
    lead_id = lead["id"] if lead else None
    conn.execute("INSERT INTO maestro_eventos (event_id, phone, lead_id, event, payload, ts) VALUES (?, ?, ?, ?, ?, ?)",
                 (event_id, phone, lead_id, event, json.dumps(payload, ensure_ascii=False), ts))

    row = conn.execute("SELECT payload FROM maestro WHERE phone = ?", (phone,)).fetchone()
    snap = _loads(row["payload"]) if row else None
    snap = snap or {}
    propios = {"de", "a", "motivo", "monto", "comision", "venta_id"}
    snap.update({k: v for k, v in payload.items() if k not in propios})
    if event == "estatus_cambiado" and "estatus" not in payload and payload.get("a"):
        snap["estatus"] = payload["a"]
    if event == "precio_ajustado" and "precio_ofrecido" not in payload and payload.get("a") not in (None, ""):
        snap["precio_ofrecido"] = _num(payload.get("a"))
    if event == "venta":
        snap["estatus"] = "vendido"
    if event == "venta_anulada":
        snap["estatus"] = payload.get("estatus") or "negociacion"
    conn.execute("INSERT INTO maestro (phone, lead_id, payload, updated_at) VALUES (?, ?, ?, CURRENT_TIMESTAMP) "
                 "ON CONFLICT(phone) DO UPDATE SET lead_id = COALESCE(excluded.lead_id, maestro.lead_id), payload = excluded.payload, updated_at = CURRENT_TIMESTAMP",
                 (phone, lead_id, json.dumps(snap, ensure_ascii=False)))

    if lead_id is None:
        _guardar_sin_lead(conn, "maestro:" + event, phone, ev)
        return {"ok": True, "matched": False, "telefono": phone, "event": event}

    em = str(snap.get("estatus") or "").strip().lower()
    if em in MAESTRO_A_RAILWAY:
        conn.execute("UPDATE leads SET estatus_maestro = ? WHERE id = ?", (em, lead_id))
    if isinstance(snap.get("fechas_estatus"), dict):
        conn.execute("UPDATE leads SET fechas_estatus = ? WHERE id = ?", (json.dumps(snap["fechas_estatus"], ensure_ascii=False), lead_id))
    asesor = str(snap.get("asesor") or "").strip() or None
    est = MAESTRO_A_RAILWAY.get(str(snap.get("estatus") or "").strip().lower())
    if est:
        motivo = None
        if est == "cancelado":
            motivo = PERDIDA_A_MOTIVO.get(str(snap.get("motivo_perdida") or ""), "otro")
        _set_result(conn, lead_id, est, asesor or "panel_maestro", source="panel_maestro", motivo=motivo, record=False)
        if motivo:
            nota = None if motivo != "otro" else str(snap.get("motivo_perdida") or "otro")
            conn.execute("UPDATE leads SET motivo = ?, nota_motivo = ? WHERE id = ?", (motivo, nota, lead_id))
    extras = {}
    if asesor:
        extras["vendedor"] = asesor
    if str(snap.get("nombre") or "").strip():
        extras["name"] = str(snap["nombre"]).strip()
    if str(snap.get("campana") or "").strip():
        extras["campaign"] = str(snap["campana"]).strip()
    auto = snap.get("auto_interes") if isinstance(snap.get("auto_interes"), dict) else None
    if auto:
        anio = _num(auto.get("anio"))
        extras.update({"auto_marca": auto.get("marca"), "auto_modelo": auto.get("modelo"), "auto_anio": int(anio) if anio else None,
                       "auto_interes": auto.get("vehiculo") or " ".join(str(x) for x in (auto.get("marca"), auto.get("modelo"), int(anio) if anio else None) if x) or None})
    precio = _num(snap.get("precio_ofrecido")) or _num(snap.get("precio_lista"))
    if precio:
        extras["auto_precio"] = precio
    if extras:
        conn.execute("UPDATE leads SET " + ", ".join(k + " = ?" for k in extras) + " WHERE id = ?", (*extras.values(), lead_id))

    if event == "venta":
        venta_id = str(payload.get("venta_id") or event_id or "").strip() or None
        if not (venta_id and conn.execute("SELECT 1 FROM ventas WHERE venta_id = ?", (venta_id,)).fetchone()):
            conn.execute("INSERT INTO ventas (venta_id, phone, auto, monto, comision, fecha, lead_id, forma_pago, vendedor) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                         (venta_id, phone, extras.get("auto_interes"), _num(payload.get("monto")) or _num(snap.get("precio_ofrecido")),
                          _num(payload.get("comision")), (ts or "")[:10] or None, lead_id, snap.get("forma_pago"), asesor))
    if event == "venta_anulada":
        vid = str(payload.get("venta_id") or "").strip()
        if vid:
            conn.execute("UPDATE ventas SET anulada = 1 WHERE venta_id = ?", (vid,))
        else:
            conn.execute("UPDATE ventas SET anulada = 1 WHERE id = (SELECT MAX(id) FROM ventas WHERE lead_id = ? AND COALESCE(anulada, 0) = 0)", (lead_id,))
    return {"ok": True, "matched": True, "lead_creado": creado, "lead_id": lead_id, "telefono": phone, "event": event}


@app.post("/maestro/evento")
@app.post("/maestro/eventos")
async def maestro_evento(request: Request):
    """Bearer MAESTRO_TOKEN o VENTAS_TOKEN. Body: un evento {event, payload, ts?, id?},
    una lista de eventos, o {"eventos": [...]}. payload.telefono (10 digitos) es la llave."""
    if not (_env("MAESTRO_TOKEN") or _env("VENTAS_TOKEN")):
        return JSONResponse({"detail": "Not configured"}, status_code=500)
    if not _maestro_ok(request):
        return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    try:
        data = await request.json()
    except Exception:
        return JSONResponse({"detail": "Invalid JSON"}, status_code=400)
    lote = data if isinstance(data, list) else (data.get("eventos") if isinstance(data, dict) and isinstance(data.get("eventos"), list) else [data])
    if len(lote) > 500:
        return JSONResponse({"detail": "max 500 eventos"}, status_code=400)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        res = [_procesar_evento(conn, ev) for ev in lote]
        conn.commit()
    finally:
        conn.close()
    if len(lote) == 1:
        return res[0] if res[0].get("ok") else JSONResponse(res[0], status_code=400)
    return {"resultados": res}


@app.get("/leads/{lead_id}/maestro")
async def lead_maestro(lead_id: int, request: Request):
    """Bearer API_TOKEN. Ultima foto del maestro para ese lead y sus eventos."""
    if not _bearer_ok(request, "API_TOKEN"):
        return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        lead = conn.execute("SELECT phone FROM leads WHERE id = ?", (lead_id,)).fetchone()
        row = lead and conn.execute("SELECT payload FROM maestro WHERE phone = ?", (lead["phone"],)).fetchone()
        if not row:
            return JSONResponse({"detail": "Not found"}, status_code=404)
        ev = [{"event": r["event"], "payload": _loads(r["payload"]), "ts": r["ts"]} for r in conn.execute(
            "SELECT event, payload, ts FROM maestro_eventos WHERE phone = ? ORDER BY ts, id", (lead["phone"],))]
        return {"payload": _loads(row["payload"]), "eventos": ev}
    finally:
        conn.close()


# ---------- Gasto de Meta Ads (captura manual desde el panel) ----------
_FECHA = re.compile(r"^\d{4}-\d{2}-\d{2}$")


@app.get("/gasto")
async def list_gasto(request: Request):
    if not _bearer_ok(request, "API_TOKEN"):
        return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = [dict(r) for r in conn.execute("SELECT * FROM gasto ORDER BY desde DESC, id DESC")]
    conn.close()
    return rows


@app.post("/gasto")
async def add_gasto(request: Request):
    """Bearer API_TOKEN. Body {campana, desde: YYYY-MM-DD, hasta: YYYY-MM-DD, monto}."""
    if not _bearer_ok(request, "API_TOKEN"):
        return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    try:
        data = await request.json()
    except Exception:
        return JSONResponse({"detail": "Invalid JSON"}, status_code=400)
    campana = str(data.get("campana") or "").strip()
    desde, hasta = str(data.get("desde") or ""), str(data.get("hasta") or "")
    monto = _num(data.get("monto"))
    if not campana or not _FECHA.match(desde) or not _FECHA.match(hasta) or desde > hasta or not monto or monto <= 0:
        return JSONResponse({"detail": "campana, desde <= hasta (YYYY-MM-DD) y monto > 0 son obligatorios"}, status_code=400)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.execute("INSERT INTO gasto (campana, desde, hasta, monto, creado_por) VALUES (?, ?, ?, ?, ?)",
                           (campana[:120], desde, hasta, monto, _env("LOGIN_USER") or "panel"))
        conn.commit()
        return dict(conn.execute("SELECT * FROM gasto WHERE id = ?", (cur.lastrowid,)).fetchone())
    finally:
        conn.close()


@app.delete("/gasto/{gasto_id}")
async def del_gasto(gasto_id: int, request: Request):
    if not _bearer_ok(request, "API_TOKEN"):
        return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    conn = sqlite3.connect(DB_PATH)
    try:
        n = conn.execute("DELETE FROM gasto WHERE id = ?", (gasto_id,)).rowcount
        conn.commit()
    finally:
        conn.close()
    if not n:
        return JSONResponse({"detail": "Not found"}, status_code=404)
    return {"deleted": gasto_id}


# ---------- Leads nuevos para importar al panel maestro ----------
@app.get("/leads/nuevos")
async def leads_nuevos(request: Request, desde: str = "", desde_id: int = 0):
    """Bearer MAESTRO_TOKEN o VENTAS_TOKEN. Leads (uno por telefono) creados despues de `desde`
    (ISO o epoch ms) o con id > desde_id. Maximo 500, del mas viejo al mas nuevo."""
    if not _maestro_ok(request):
        return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    corte = "1970-01-01 00:00:00"
    d = (desde or "").strip()
    if d:
        try:
            if d.isdigit():
                n = int(d)
                dt = datetime.fromtimestamp(n / 1000 if n > 10**11 else n, tz=timezone.utc)
            else:
                dt = datetime.fromisoformat(d.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
            corte = dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return JSONResponse({"detail": "desde invalido (ISO o epoch ms)"}, status_code=400)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = [dict(r) for r in conn.execute("""
        SELECT l.id, l.phone AS telefono, l.name AS nombre, l.origen, l.campaign AS campana,
               l.created_at AS fecha, l.message AS mensaje, l.anuncio_id, l.numero_destino,
               l.result AS estatus, l.estatus_maestro, l.auto_interes
        FROM leads l
        WHERE l.id = (SELECT MAX(id) FROM leads l2 WHERE l2.phone = l.phone)
          AND l.created_at > ? AND l.id > ? AND length(COALESCE(l.phone, '')) = 10
        ORDER BY l.created_at, l.id LIMIT 500""", (corte, desde_id))]
    conn.close()
    for r in rows:
        if r["fecha"] and "T" not in r["fecha"]:
            r["fecha"] = r["fecha"].replace(" ", "T") + "Z"
    return {"leads": rows, "total": len(rows),
            "hasta": rows[-1]["fecha"] if rows else (d or None)}


# ---------- Metricas (Bearer API_TOKEN) ----------
ETAPAS_MAESTRO = ["nuevo", "contactado", "cita", "visito", "negociacion", "credito", "apartado", "vendido", "perdido"]


def _filtro_leads(desde, hasta, campana, origen):
    w, p = ["l.id = (SELECT MAX(id) FROM leads l2 WHERE l2.phone = l.phone)"], []
    if desde:
        w.append("date(l.created_at) >= date(?)"); p.append(desde)
    if hasta:
        w.append("date(l.created_at) <= date(?)"); p.append(hasta)
    if campana:
        w.append("COALESCE(l.campaign, '') = ?"); p.append(campana)
    if origen:
        w.append("COALESCE(l.origen, '') = ?"); p.append(origen)
    return " AND ".join(w), p


def _iso(s):
    try:
        dt = datetime.fromisoformat(str(s).replace("Z", "+00:00").replace(" ", "T"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _leads_metricas(desde, hasta, campana, origen):
    where, p = _filtro_leads(desde, hasta, campana, origen)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = [dict(r) for r in conn.execute(f"SELECT l.* FROM leads l WHERE {where}", p)]
    fotos = {r["phone"]: _loads(r["payload"]) or {} for r in conn.execute("SELECT phone, payload FROM maestro")}
    ventas = [dict(r) for r in conn.execute("SELECT * FROM ventas WHERE COALESCE(anulada, 0) = 0")]
    gastos = [dict(r) for r in conn.execute("SELECT * FROM gasto")]
    conn.close()
    for r in rows:
        r["_m"] = fotos.get(r["phone"], {})
    return rows, ventas, gastos


def _etapa(r):
    em = r.get("estatus_maestro") or (r["_m"].get("estatus") if r["_m"] else None)
    if em in ETAPAS_MAESTRO:
        return em
    inv = {"cita_agendada": "cita", "en_proceso": "negociacion", "en_tramite": "credito", "cancelado": "perdido",
           "no_contesta": "contactado", "vendido": "vendido", "contactado": "contactado", "nuevo": "nuevo"}
    return inv.get(r.get("result") or "nuevo", "nuevo")


@app.get("/metricas/embudo")
async def metricas_embudo(request: Request, desde: str = "", hasta: str = "", campana: str = "", origen: str = ""):
    """Cuantos leads estan en cada etapa y cuantos alcanzaron cada etapa, mas dias promedio entre etapas."""
    if not _bearer_ok(request, "API_TOKEN"):
        return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    rows, _, _ = _leads_metricas(desde, hasta, campana, origen)
    actual = {e: 0 for e in ETAPAS_MAESTRO}
    alcanzo = {e: 0 for e in ETAPAS_MAESTRO}
    dias = {}
    for r in rows:
        et = _etapa(r)
        actual[et] += 1
        fe = r["_m"].get("fechas_estatus") if isinstance(r["_m"].get("fechas_estatus"), dict) else (_loads(r.get("fechas_estatus")) or {})
        for e in ETAPAS_MAESTRO:
            implicito = et != "perdido" and e != "perdido" and ETAPAS_MAESTRO.index(e) <= ETAPAS_MAESTRO.index(et)
            if fe.get(e) or e == et or e == "nuevo" or implicito:
                alcanzo[e] += 1
        seq = [e for e in ETAPAS_MAESTRO if fe.get(e)]
        for a, b in zip(seq, seq[1:]):
            ta, tb = _iso(fe[a]), _iso(fe[b])
            if ta and tb and tb >= ta:
                dias.setdefault(f"{a}->{b}", []).append((tb - ta).total_seconds() / 86400)
    total = len(rows)
    return {"total": total,
            "etapas": [{"etapa": e, "actual": actual[e], "alcanzaron": alcanzo[e],
                        "pct": round(alcanzo[e] * 100 / total, 1) if total else 0} for e in ETAPAS_MAESTRO],
            "dias_entre_etapas": {k: round(sum(v) / len(v), 2) for k, v in dias.items()}}


@app.get("/metricas/respuesta")
async def metricas_respuesta(request: Request, desde: str = "", hasta: str = "", campana: str = "", origen: str = ""):
    """Tiempo de primera respuesta (tiempo_respuesta_min del maestro): promedio, mediana, p90 y por asesor."""
    if not _bearer_ok(request, "API_TOKEN"):
        return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    rows, _, _ = _leads_metricas(desde, hasta, campana, origen)
    vals, por_asesor, sin = [], {}, 0
    for r in rows:
        v = _num(r["_m"].get("tiempo_respuesta_min"))
        if v is None:
            sin += 1
            continue
        vals.append(v)
        por_asesor.setdefault(r["_m"].get("asesor") or r.get("vendedor") or "sin_asesor", []).append(v)

    def st(xs):
        xs = sorted(xs)
        if not xs:
            return {"n": 0, "promedio_min": None, "mediana_min": None, "p90_min": None}
        return {"n": len(xs), "promedio_min": round(sum(xs) / len(xs), 1), "mediana_min": xs[len(xs) // 2],
                "p90_min": xs[min(len(xs) - 1, int(len(xs) * 0.9))]}
    return {**st(vals), "sin_dato": sin, "por_asesor": {k: st(v) for k, v in por_asesor.items()},
            "rangos": {"<=15": sum(1 for v in vals if v <= 15), "15-60": sum(1 for v in vals if 15 < v <= 60),
                       "60-1440": sum(1 for v in vals if 60 < v <= 1440), ">1440": sum(1 for v in vals if v > 1440)}}


@app.get("/metricas/campanas")
async def metricas_campanas(request: Request, desde: str = "", hasta: str = "", campana: str = "", origen: str = ""):
    """Por campana: leads, ventas, monto vendido, gasto (que traslapa el periodo), costo por lead y por venta."""
    if not _bearer_ok(request, "API_TOKEN"):
        return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    rows, ventas, gastos = _leads_metricas(desde, hasta, campana, origen)
    ids = {r["id"]: r for r in rows}
    phones = {r["phone"]: r for r in rows}
    out = {}

    def c(nombre):
        return out.setdefault(nombre or "sin_campana", {"campana": nombre or "sin_campana", "leads": 0, "ventas": 0,
                                                        "monto": 0.0, "gasto": 0.0})
    for r in rows:
        c(r["_m"].get("campana") or r.get("campaign"))["leads"] += 1
    for v in ventas:
        r = ids.get(v.get("lead_id")) or phones.get(v.get("phone"))
        if not r:
            continue
        x = c(r["_m"].get("campana") or r.get("campaign"))
        x["ventas"] += 1
        x["monto"] += _num(v.get("monto")) or 0
    for g in gastos:
        if desde and g["hasta"] < desde:
            continue
        if hasta and g["desde"] > hasta:
            continue
        if campana and g["campana"] != campana:
            continue
        c(g["campana"])["gasto"] += g["monto"] or 0
    res = []
    for x in out.values():
        x["costo_por_lead"] = round(x["gasto"] / x["leads"], 2) if x["leads"] and x["gasto"] else None
        x["costo_por_venta"] = round(x["gasto"] / x["ventas"], 2) if x["ventas"] and x["gasto"] else None
        x["conversion_pct"] = round(x["ventas"] * 100 / x["leads"], 1) if x["leads"] else 0
        res.append(x)
    res.sort(key=lambda x: (-x["leads"], x["campana"]))
    return {"campanas": res, "nota": "El gasto cuenta completo si su periodo se traslapa con el filtro."}
