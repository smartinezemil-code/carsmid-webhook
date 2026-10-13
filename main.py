from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import PlainTextResponse
import sqlite3

app = FastAPI()

VERIFY_TOKEN = "carsmid_wh_2026_x7k9"
DB_PATH = "leads.db"

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
    conn.commit()
    conn.close()

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
                "INSERT INTO leads (wa_id, phone, name, timestamp, message) VALUES (?, ?, ?, ?, ?)",
                (
                    msg.get("id"),
                    msg.get("from"),
                    contact["profile"]["name"],
                    msg.get("timestamp"),
                    msg.get("text", {}).get("body", "")
                )
            )
            conn.commit()
            conn.close()
    except Exception as e:
        print(e)
    return {"status": "ok"}
