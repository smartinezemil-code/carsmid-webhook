from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import PlainTextResponse, JSONResponse
from urllib.parse import parse_qs
import hmac
import os
import re
import sqlite3

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
    # Migracion segura: agrega la columna origen si la BD existente no la tiene
    cols = [r[1] for r in conn.execute("PRAGMA table_info(leads)")]
    if "origen" not in cols:
        conn.execute("ALTER TABLE leads ADD COLUMN origen TEXT DEFAULT 'carsmid'")
    # Historial de cambios de resultado (lo llena PATCH /leads/{id})
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
    # Ventas que llegan del panel maestro (POST /ventas)
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

init_db()

@app.get("/webhook")
async def verify(request: Request):
    params = request.query_params
    mode = params.get("hub.mode")
    token = params.get("hub.verify_token") or ""
    challenge = params.get("hub.challenge")
    if mode == "subscribe" and VERIFY_TOKEN and hmac.compare_digest(token.encode(), VERIFY_TOKEN.encode()):
        return PlainTextResponse(challenge)
    raise HTTPException(status_code=403, detail="Verification failed")

@app.post("/webhook")
async def receive(request: Request):
    data = await request.json()
    try:
        entry = data["entry"][0]
        changes = entry["changes"][0]
        value = changes["value"]
        if "messages" in value:
            msg = value["messages"][0]
            contact = value["contacts"][0]
            conn = sqlite3.connect(DB_PATH)
            conn.execute(
                "INSERT INTO leads (wa_id, phone, name, timestamp, message, origen) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    msg.get("id"),
                    normalize_phone(msg.get("from")),
                    contact["profile"]["name"],
                    msg.get("timestamp"),
                    msg.get("text", {}).get("body", ""),
                    detect_origen(value, msg),
                )
            )
            conn.commit()
            conn.close()
    except Exception as e:
        print(e)
    return {"status": "ok"}


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
    token = _env("API_TOKEN")
    auth = request.headers.get("authorization", "")
    if not token or not hmac.compare_digest(auth.encode(), ("Bearer " + token).encode()):
        return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = [dict(r) for r in conn.execute("SELECT * FROM leads ORDER BY id DESC")]
    conn.close()
    return rows


# ---------- Panel: helpers ----------
RESULTS = {"", "contactado", "cita_agendada", "vendido", "no_interesado", "no_contesto"}


def _bearer_ok(request: Request, env_name: str) -> bool:
    token = _env(env_name)
    auth = request.headers.get("authorization", "")
    return bool(token) and hmac.compare_digest(auth.encode(), ("Bearer " + token).encode())


def _set_result(conn, lead_id, new_result, changed_by):
    """Cambia el resultado de un lead y deja registro en lead_history."""
    row = conn.execute("SELECT result FROM leads WHERE id = ?", (lead_id,)).fetchone()
    if row is None:
        return False
    old = row[0] or ""
    if old != new_result:
        conn.execute("UPDATE leads SET result = ? WHERE id = ?", (new_result or None, lead_id))
        conn.execute(
            "INSERT INTO lead_history (lead_id, old_result, new_result, changed_by) VALUES (?, ?, ?, ?)",
            (lead_id, old, new_result, changed_by),
        )
    return True


# ---------- Panel: cambiar resultado ----------
@app.patch("/leads/{lead_id}")
async def update_lead(lead_id: int, request: Request):
    """Body JSON {result, password}. Bearer API_TOKEN.
    200 lead actualizado · 400 resultado invalido · 401 sin token o contrasena incorrecta · 404 no existe."""
    if not _bearer_ok(request, "API_TOKEN"):
        return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    try:
        data = await request.json()
    except Exception:
        return JSONResponse({"detail": "Invalid JSON"}, status_code=400)
    password = str(data.get("password", ""))
    pass_env = _env("LOGIN_PASS")
    if not (password and pass_env and hmac.compare_digest(password.encode(), pass_env.encode())):
        return JSONResponse({"detail": "Invalid password"}, status_code=401)
    new_result = str(data.get("result") or "").strip().lower()
    if new_result not in RESULTS:
        return JSONResponse({"detail": "Invalid result", "allowed": sorted(RESULTS)}, status_code=400)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        if not _set_result(conn, lead_id, new_result, _env("LOGIN_USER") or "panel"):
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


# ---------- Ventas (Cloudflare Worker del panel maestro) ----------
@app.post("/ventas")
async def create_venta(request: Request):
    """Bearer VENTAS_TOKEN. Body JSON {venta_id?, phone, auto?, monto?, comision?, fecha?}.
    Hace match con el lead mas reciente del mismo numero (10 digitos) y lo marca como vendido.
    Si llega dos veces el mismo venta_id, no se duplica."""
    if not _env("VENTAS_TOKEN"):
        return JSONResponse({"detail": "Ventas not configured"}, status_code=500)
    if not _bearer_ok(request, "VENTAS_TOKEN"):
        return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    try:
        data = await request.json()
    except Exception:
        return JSONResponse({"detail": "Invalid JSON"}, status_code=400)
    phone = normalize_phone(data.get("phone"))
    if len(phone) != 10:
        return JSONResponse({"detail": "phone must have 10 digits"}, status_code=400)

    def num(v):
        try:
            return float(v) if v not in (None, "") else None
        except (TypeError, ValueError):
            return None

    venta_id = str(data.get("venta_id") or "").strip() or None
    conn = sqlite3.connect(DB_PATH)
    try:
        if venta_id:
            prev = conn.execute("SELECT id, lead_id FROM ventas WHERE venta_id = ?", (venta_id,)).fetchone()
            if prev:
                return {"id": prev[0], "venta_id": venta_id, "matched": prev[1] is not None,
                        "lead_id": prev[1], "duplicate": True}
        lead = conn.execute(
            "SELECT id FROM leads WHERE phone = ? ORDER BY id DESC LIMIT 1", (phone,)).fetchone()
        lead_id = lead[0] if lead else None
        cur = conn.execute(
            "INSERT INTO ventas (venta_id, phone, auto, monto, comision, fecha, lead_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (venta_id, phone, str(data.get("auto") or "") or None, num(data.get("monto")),
             num(data.get("comision")), str(data.get("fecha") or "") or None, lead_id),
        )
        if lead_id is not None:
            _set_result(conn, lead_id, "vendido", "panel_maestro")
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
