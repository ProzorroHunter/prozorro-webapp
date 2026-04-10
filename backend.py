from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Optional
import httpx
import asyncio
from datetime import datetime, timedelta
import sqlite3
import os
import re

try:
    import psycopg2
except ImportError:
    psycopg2 = None

app = FastAPI(title="ProzorroHunter API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
_search_lock     = asyncio.Lock()
_active_searches: set = set()
_queued_searches: set = set()

DATABASE_URL = os.getenv("DATABASE_URL", "")
DB_PATH = os.getenv("DB_PATH", "prozorro.db")
USE_PG = bool(DATABASE_URL and psycopg2)
P = "%s" if USE_PG else "?"

def get_conn():
    if USE_PG:
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = False
        return conn
    return sqlite3.connect(DB_PATH, timeout=10)

def log(*args):
    print(*args, flush=True)

def init_db():
    conn = get_conn()
    c = conn.cursor()
    if USE_PG:
        c.execute('''CREATE TABLE IF NOT EXISTS filters (
            id SERIAL PRIMARY KEY, name TEXT NOT NULL, keywords TEXT, cpv TEXT, region TEXT,
            procuring_entity TEXT, supplier TEXT, min_amount REAL, max_amount REAL,
            period_days INTEGER DEFAULT 30, is_active BOOLEAN DEFAULT TRUE,
            found_count INTEGER DEFAULT 0, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        c.execute('''CREATE TABLE IF NOT EXISTS tenders (
            id TEXT PRIMARY KEY, title TEXT, procuring_entity TEXT, amount REAL, cpv TEXT,
            region TEXT, date_published TIMESTAMP, url TEXT, filter_id INTEGER,
            notified BOOLEAN DEFAULT FALSE, FOREIGN KEY (filter_id) REFERENCES filters(id)
        )''')
    else:
        c.execute('''CREATE TABLE IF NOT EXISTS filters (
            id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, keywords TEXT,
            cpv TEXT, region TEXT, procuring_entity TEXT, supplier TEXT, min_amount REAL,
            max_amount REAL, period_days INTEGER DEFAULT 30, is_active BOOLEAN DEFAULT 1,
            found_count INTEGER DEFAULT 0, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        c.execute('''CREATE TABLE IF NOT EXISTS tenders (
            id TEXT PRIMARY KEY, title TEXT, procuring_entity TEXT, amount REAL, cpv TEXT,
            region TEXT, date_published TIMESTAMP, url TEXT, filter_id INTEGER,
            notified BOOLEAN DEFAULT 0, FOREIGN KEY (filter_id) REFERENCES filters(id)
        )''')
    conn.commit()
    conn.close()

init_db()

class FilterCreate(BaseModel):
    name: str
    keywords: Optional[str] = None
    cpv: Optional[str] = None
    region: Optional[str] = None
    procuringEntity: Optional[str] = None
    minAmount: Optional[float] = None
    maxAmount: Optional[float] = None
    periodDays: Optional[int] = 30

PROZORRO_API = "https://public-api.prozorro.gov.ua/api/2.5"

def get_tender_date(dd: dict) -> str:
    return dd.get("datePublished") or dd.get("date") or dd.get("tenderPeriod", {}).get("startDate") or ""

def _build_tender_response(tender_id: str, data: dict, url: str) -> dict:
    pe = data.get("procuringEntity", {})
    addr = pe.get("address", {})
    val = data.get("value", {})
    items = data.get("items", [])
    clf = items[0].get("classification", {}) if items else {}
    return {
        "id": tender_id, "limited": False, "newTender": False,
        "title": data.get("title", "Тендер без назви"),
        "procuringEntity": pe.get("name", "Невідомо"),
        "amount": val.get("amount", 0), "currency": val.get("currency", "UAH"),
        "status": data.get("status", "active"), "datePublished": get_tender_date(data),
        "region": addr.get("region", ""), "cpv": clf.get("id", ""), "url": url
    }

@app.get("/api/tender/{tender_id}")
async def get_tender_by_id(tender_id: str):
    log(f"🔍 ЗАПИТ ТЕНДЕРА: {tender_id}")
    prozorro_url = f"https://prozorro.gov.ua/tender/{tender_id}"
    is_ua_id = bool(re.match(r'^UA-\d{4}-\d{2}-\d{2}-', tender_id))

    async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
        # 1. Якщо це UA-номер, шукаємо HEX ID (внутрішній номер)
        hex_id = tender_id
        if is_ua_id:
            # Шукаємо в актуальному списку (descending=1 ставить нові тендери на початок)
            r_search = await client.get(f"{PROZORRO_API}/tenders", params={"tenderID": tender_id, "opt_fields": "id"})
            candidates = r_search.json().get("data", []) if r_search.status_code == 200 else []
            
            if not candidates:
                # Якщо пошук по ID не дав результату (новий тендер), шукаємо в стрімі останніх
                r_stream = await client.get(f"{PROZORRO_API}/tenders", params={"limit": 100, "descending": 1, "opt_fields": "id,tenderID"})
                if r_stream.status_code == 200:
                    candidates = [i for i in r_stream.json().get("data", []) if i.get("tenderID") == tender_id]
            
            if candidates:
                hex_id = candidates[0]["id"]
            else:
                # Якщо взагалі не знайшли — повертаємо заглушку з кнопкою
                return {"id": tender_id, "limited": True, "newTender": True, "url": prozorro_url, "title": "Тендер знайдено", "status": "Завантаження..."}

        # 2. Отримуємо детальні дані за HEX ID
        r_det = await client.get(f"{PROZORRO_API}/tenders/{hex_id}")
        if r_det.status_code == 200:
            data = r_det.json().get("data", {})
            # Перевірка: якщо рік тендера дуже старий (напр. 2015), а ми шукаємо 2026
            pub_date = get_tender_date(data)
            if is_ua_id and "2026" in tender_id and "2015" in pub_date:
                log("   ⚠️ Знайдено застарілий дублікат. Повертаємо посилання на сайт.")
                return {"id": tender_id, "limited": True, "newTender": True, "url": prozorro_url, "title": "Оновлення даних...", "status": "Дивіться на Prozorro"}
            
            return _build_tender_response(tender_id, data, prozorro_url)

    return {"id": tender_id, "limited": True, "url": prozorro_url, "title": "Помилка завантаження", "status": "error"}

# --- РЕШТА ЕНДПОЇНТІВ (залишаються без змін для стабільності) ---

@app.get("/")
async def root(): return FileResponse('static/index.html')

@app.get("/api/filters")
async def get_filters():
    conn = get_conn(); c = conn.cursor()
    c.execute('SELECT id, name, keywords, cpv, region, procuring_entity, found_count FROM filters WHERE is_active = TRUE')
    rows = c.fetchall(); conn.close()
    return [{"id": r[0], "name": r[1], "keywords": r[2], "cpv": r[3], "region": r[4], "procuringEntity": r[5], "foundCount": r[6]} for r in rows]

@app.post("/api/filters")
async def create_filter(filter_data: FilterCreate, background_tasks: BackgroundTasks):
    conn = get_conn(); c = conn.cursor()
    c.execute(f'INSERT INTO filters (name, keywords, cpv, region, procuring_entity, min_amount, max_amount, period_days) VALUES ({P},{P},{P},{P},{P},{P},{P},{P}) {"RETURNING id" if USE_PG else ""}',
              (filter_data.name, filter_data.keywords, filter_data.cpv, filter_data.region, filter_data.procuringEntity, filter_data.minAmount, filter_data.maxAmount, filter_data.periodDays))
    f_id = c.fetchone()[0] if USE_PG else c.lastrowid
    conn.commit(); conn.close()
    return {"id": f_id}

@app.get("/api/tenders")
async def get_tenders(limit: int = 100):
    conn = get_conn(); c = conn.cursor()
    c.execute(f'SELECT id, title, procuring_entity, amount, cpv, region, date_published, url FROM tenders ORDER BY date_published DESC LIMIT {P}', (limit,))
    rows = c.fetchall(); conn.close()
    return [{"id": r[0], "title": r[1], "procuringEntity": r[2], "amount": r[3], "cpv": r[4], "region": r[5], "datePublished": r[6], "url": r[7]} for r in rows]

@app.get("/api/stats")
async def get_stats():
    conn = get_conn(); c = conn.cursor()
    c.execute('SELECT COUNT(*) FROM tenders'); total = c.fetchone()[0]
    c.execute('SELECT COUNT(*) FROM filters WHERE is_active = TRUE'); active = c.fetchone()[0]
    conn.close()
    return {"total": total, "active": active}

app.mount("/static", StaticFiles(directory="static"), name="static")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 10000)))






