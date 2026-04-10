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

_search_lock = asyncio.Lock()
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
    d = dd.get("datePublished") or dd.get("date") or dd.get("tenderPeriod", {}).get("startDate") or ""
    return d.split('T')[0] if d else ""

def _build_tender_response(tender_id: str, data: dict, url: str) -> dict:
    pe = data.get("procuringEntity", {})
    val = data.get("value", {})
    items = data.get("items", [])
    clf = items[0].get("classification", {}) if items else {}
    
    # Если год в дате публикации слишком старый, а в ID — 2026
    pub_date = get_tender_date(data)
    title = data.get("title", "Тендер знайдено")
    if "2026" in tender_id and "201" in pub_date:
        title = "⚠️ ТЕНДЕР 2026 (Дані завантажуються...)"

    return {
        "id": tender_id, "limited": False, "newTender": False,
        "title": title,
        "procuringEntity": pe.get("name", "Дані Prozorro"),
        "amount": val.get("amount", 0), "currency": val.get("currency", "UAH"),
        "status": data.get("status", "active"), "datePublished": pub_date,
        "region": pe.get("address", {}).get("region", ""), "cpv": clf.get("id", ""), "url": url
    }

@app.get("/api/tender/{tender_id}")
async def get_tender_by_id(tender_id: str):
    tender_id = tender_id.strip()
    log(f"🔍 ПОШУК ТЕНДЕРА: {tender_id}")
    prozorro_url = f"https://prozorro.gov.ua/tender/{tender_id}"
    is_ua_id = bool(re.match(r'^UA-\d{4}-\d{2}-\d{2}-', tender_id))

    async with httpx.AsyncClient(timeout=25.0, follow_redirects=True) as client:
        hex_id = tender_id
        
        if is_ua_id:
            # СТРАТЕГИЯ: Сначала смотрим в ленте НОВЫХ тендеров (descending)
            # Это исключает попадание на старые архивные записи с похожими номерами
            try:
                r_recent = await client.get(f"{PROZORRO_API}/tenders", params={"limit": 100, "descending": 1, "opt_fields": "id,tenderID"})
                if r_recent.status_code == 200:
                    recent_data = r_recent.json().get("data", [])
                    match = next((i for i in recent_data if i.get("tenderID") == tender_id), None)
                    if match:
                        hex_id = match["id"]
                        log(f"   🎯 Знайдено в новому потоці: {hex_id}")
                    else:
                        # Если в последних 100 нет, пробуем целевой поиск, но фильтруем по дате
                        r_search = await client.get(f"{PROZORRO_API}/tenders", params={"tenderID": tender_id, "opt_fields": "id,dateModified"})
                        cands = r_search.json().get("data", [])
                        if cands:
                            # Берем самый свежий по дате модификации
                            cands.sort(key=lambda x: x.get("dateModified", ""), reverse=True)
                            hex_id = cands[0]["id"]
            except Exception as e:
                log(f"   ⚠️ Помилка API: {e}")

        # Запрос деталей
        r_det = await client.get(f"{PROZORRO_API}/tenders/{hex_id}")
        if r_det.status_code == 200:
            return _build_tender_response(tender_id, r_det.json().get("data", {}), prozorro_url)

    return {"id": tender_id, "limited": True, "url": prozorro_url, "title": "Тендер знайдено на Prozorro", "status": "active"}

@app.get("/api/stats")
async def get_stats():
    try:
        conn = get_conn(); c = conn.cursor()
        c.execute('SELECT COUNT(*) FROM tenders'); total = c.fetchone()[0]
        
        today_str = datetime.now().strftime('%Y-%m-%d')
        if USE_PG:
            c.execute("SELECT COUNT(*) FROM tenders WHERE CAST(date_published AS TEXT) LIKE %s", (f"{today_str}%",))
        else:
            c.execute("SELECT COUNT(*) FROM tenders WHERE date_published LIKE ?", (f"{today_str}%",))
        today = c.fetchone()[0]
        
        c.execute('SELECT COUNT(*) FROM filters WHERE is_active = TRUE'); active = c.fetchone()[0]
        conn.close()
        return {"total": total, "today": today, "active": active}
    except Exception as e:
        log(f"Stats error: {e}")
        return {"total": 0, "today": 0, "active": 0}

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

@app.get("/")
async def root(): return FileResponse('static/index.html')

app.mount("/static", StaticFiles(directory="static"), name="static")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 10000)))





