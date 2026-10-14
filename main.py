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

VERIFY_TOKEN = "carsmid_wh_2026_x7k9"
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
    token = params.get("hub.verify_token")
    challenge = params.get("hub.challenge")
    if mode == "subscribe" and token == VERIFY_TOKEN:
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
