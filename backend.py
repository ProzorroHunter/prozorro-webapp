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
import traceback
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
_active_searches: set = set()   # filter IDs зараз в пошуку
_queued_searches: set = set()   # filter IDs в черзі (чекають lock)

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
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            keywords TEXT,
            cpv TEXT,
            region TEXT,
            procuring_entity TEXT,
            supplier TEXT,
            min_amount REAL,
            max_amount REAL,
            period_days INTEGER DEFAULT 30,
            is_active BOOLEAN DEFAULT TRUE,
            found_count INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        c.execute('''CREATE TABLE IF NOT EXISTS tenders (
            id TEXT PRIMARY KEY,
            title TEXT,
            procuring_entity TEXT,
            amount REAL,
            cpv TEXT,
            region TEXT,
            date_published TIMESTAMP,
            url TEXT,
            filter_id INTEGER,
            notified BOOLEAN DEFAULT FALSE,
            FOREIGN KEY (filter_id) REFERENCES filters(id)
        )''')
    else:
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='filters'")
        if not c.fetchone():
            c.execute('''CREATE TABLE filters (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                keywords TEXT,
                cpv TEXT,
                region TEXT,
                procuring_entity TEXT,
                supplier TEXT,
                min_amount REAL,
                max_amount REAL,
                period_days INTEGER DEFAULT 30,
                is_active BOOLEAN DEFAULT 1,
                found_count INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )''')
            c.execute('''CREATE TABLE tenders (
                id TEXT PRIMARY KEY,
                title TEXT,
                procuring_entity TEXT,
                amount REAL,
                cpv TEXT,
                region TEXT,
                date_published TIMESTAMP,
                url TEXT,
                filter_id INTEGER,
                notified BOOLEAN DEFAULT 0,
                FOREIGN KEY (filter_id) REFERENCES filters(id)
            )''')
    log("✅ БД ініціалізовано" + (" (PostgreSQL)" if USE_PG else " (SQLite)"))
    conn.commit()
    conn.close()

init_db()

class FilterCreate(BaseModel):
    name: str
    keywords: Optional[str] = None
    cpv: Optional[str] = None
    region: Optional[str] = None
    procuringEntity: Optional[str] = None
    supplier: Optional[str] = None
    minAmount: Optional[float] = None
    maxAmount: Optional[float] = None
    periodDays: Optional[int] = 30

PROZORRO_API = "https://public-api.prozorro.gov.ua/api/2.5"

# --- Логика морфологии и регионов ---

_UK_SUFFIXES = tuple(sorted([
    'ськими', 'зьких', 'зьким', 'ськими', 'ському', 'ській',
    'ними', 'ому', 'ами', 'ями', 'ків', 'ань', 'ими',
    'ого', 'ій', 'им', 'ої', 'ів', 'ях', 'ям', 'ою', 'ею', 'ую',
    'ам', 'ах', 'ий', 'их', 'їх',
    'и', 'а', 'у', 'і', 'е', 'є', 'ю', 'я', 'ь',
], key=len, reverse=True))

def uk_stem(word: str) -> str:
    for suffix in _UK_SUFFIXES:
        if word.endswith(suffix) and len(word) - len(suffix) >= 3:
            return word[:-len(suffix)]
    return word

def make_kw_list(keywords: Optional[str]):
    if not keywords: return []
    result = []
    for k in keywords.split(","):
        k = k.strip().lower()
        if not k: continue
        result.append(k)
        stem = uk_stem(k)
        if stem != k: result.append(stem)
    return list(dict.fromkeys(result))

def kw_in_text(kw: str, text: str) -> bool:
    if kw in text: return True
    for trim in range(1, 4):
        if len(kw) - trim >= 4:
            if kw[:-trim] in text: return True
    return False

OBLAST_ALIASES: dict[str, list[str]] = {
    "харківська": ["харків", "місто харків"],
    "київська": ["київ", "місто київ"],
    "дніпропетровська": ["дніпро", "місто дніпро"],
    "одеська": ["одеса", "місто одеса"],
    "запорізька": ["запоріжжя", "місто запоріжжя"],
    "львівська": ["львів", "місто львів"],
    "вінницька": ["вінниця", "місто вінниця"],
    "полтавська": ["полтава", "місто полтава"],
    "черкаська": ["черкаси", "місто черкаси"],
    "сумська": ["суми", "місто суми"],
    "миколаївська": ["миколаїв", "місто миколаїв"],
    "херсонська": ["херсон", "місто херсон"],
    "житомирська": ["житомир", "місто житомир"],
    "рівненська": ["рівне", "місто рівне"],
    "кіровоградська": ["кропивницький", "кіровоград"],
    "хмельницька": ["хмельницький", "місто хмельницький"],
    "тернопільська": ["тернопіль", "місто тернопіль"],
    "чернівецька": ["чернівці", "місто чернівці"],
    "чернігівська": ["чернігів", "місто чернігів"],
    "івано-франківська": ["івано-франківськ", "місто івано-франківськ"],
    "закарпатська": ["ужгород", "місто вжегород"],
    "волинська": ["луцьк", "місто луцьк"],
}

def region_matches(searched: str, t_region: str, t_locality: str) -> bool:
    s, tr, tl = searched.lower().strip(), t_region.lower().strip(), t_locality.lower().strip()
    full_address = f"{tr} {tl}".strip()
    if s in full_address: return True
    def norm(r: str) -> str:
        return (r.replace(" область", "").replace("область", "").replace("місто ", "").replace("м. ", "").strip())
    sn = norm(s)
    full_n = norm(full_address)
    if sn and (sn in full_n or norm(tr) in sn): return True
    for oblast_key, city_list in OBLAST_ALIASES.items():
        if oblast_key in sn:
            for city in city_list:
                if city in full_address or city in full_n: return True
    return False

def get_tender_date(dd: dict) -> str:
    return (dd.get("datePublished") or dd.get("date") or 
            dd.get("tenderPeriod", {}).get("startDate") or "")

def save_tender_now(filter_id: int, tender: dict) -> bool:
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute(f'SELECT id FROM tenders WHERE id = {P}', (tender["id"],))
        if c.fetchone():
            conn.close()
            return False
        c.execute(
            f'''INSERT INTO tenders (id, title, procuring_entity, amount, cpv, region, date_published, url, filter_id)
               VALUES ({P},{P},{P},{P},{P},{P},{P},{P},{P}) ON CONFLICT (id) DO NOTHING''',
            (tender["id"], tender["title"], tender["procuringEntity"], tender["amount"], 
             tender["cpv"], tender["region"], tender["datePublished"], tender["url"], filter_id)
        )
        saved = c.rowcount > 0
        if saved and filter_id > 0:
            c.execute(f'UPDATE filters SET found_count = found_count + 1 WHERE id = {P}', (filter_id,))
        conn.commit()
        conn.close()
        return saved
    except Exception as e:
        log(f"   ⚠️ Помилка збереження: {e}")
        return False

# --- Поиск и фоновые задачи ---

async def search_and_save(filter_id: int, keywords=None, cpv=None, region=None,
                          procuring_entity=None, supplier=None,
                          min_amount=None, max_amount=None, period_days=30):
    date_from = (datetime.now() - timedelta(days=period_days)).isoformat()
    kw_list = make_kw_list(keywords)
    log(f"\n🔍 ПОШУК (фільтр #{filter_id})")
    total_saved = 0
    offset = None

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            for page_num in range(20):
                params = {"descending": "1", "limit": "100", "opt_fields": "id,title,status,dateModified"}
                if offset: params["offset"] = offset
                resp = await client.get(f"{PROZORRO_API}/tenders", params=params)
                if resp.status_code != 200: break
                page_items = resp.json().get("data", [])
                if not page_items: break
                
                for item in page_items:
                    if item.get("dateModified", "") < date_from: return total_saved
                    if kw_list and not any(kw_in_text(kw, item.get("title","").lower()) for kw in kw_list): continue
                    
                    dr = await client.get(f"{PROZORRO_API}/tenders/{item['id']}")
                    if dr.status_code != 200: continue
                    dd = dr.json().get("data", {})
                    
                    res = check_filter_detailed(dd, keywords, cpv, region, procuring_entity, supplier, min_amount, max_amount)
                    if res["matches"]:
                        tender = {
                            "id": dd["id"], "title": dd.get("title", "Без назви"),
                            "procuringEntity": dd.get("procuringEntity", {}).get("name", "Невідомо"),
                            "amount": dd.get("value", {}).get("amount", 0),
                            "cpv": dd.get("items", [{}])[0].get("classification", {}).get("id", ""),
                            "region": dd.get("procuringEntity", {}).get("address", {}).get("region", ""),
                            "datePublished": get_tender_date(dd),
                            "url": f"https://prozorro.gov.ua/tender/{dd['id']}"
                        }
                        if save_tender_now(filter_id, tender): total_saved += 1
                        if total_saved >= 10: return total_saved
                
                offset = resp.json().get("next_page", {}).get("offset")
                if not offset: break
    except Exception as e: log(f"❌ Помилка пошуку: {e}")
    return total_saved

def check_filter_detailed(tender, keywords, cpv, region, procuring_entity, supplier, min_amount, max_amount):
    if keywords:
        text = (tender.get("title", "") + " " + tender.get("description", "")).lower()
        if not any(kw_in_text(kw, text) for kw in make_kw_list(keywords)): return {"matches": False}
    if cpv and not tender.get("items", [{}])[0].get("classification", {}).get("id", "").startswith(cpv[:3]): return {"matches": False}
    if region:
        addr = tender.get("procuringEntity", {}).get("address", {})
        if not region_matches(region, addr.get("region",""), addr.get("locality","")): return {"matches": False}
    if procuring_entity:
        if procuring_entity.lower() not in tender.get("procuringEntity", {}).get("name", "").lower(): return {"matches": False}
    
    amount = tender.get("value", {}).get("amount", 0)
    if min_amount and amount < min_amount: return {"matches": False}
    if max_amount and amount > max_amount: return {"matches": False}
    return {"matches": True}

# --- API Эндпоинты ---

@app.get("/")
async def root(): return FileResponse('static/index.html')

def _build_tender_response(tender_id: str, data: dict, url: str, limited: bool = False) -> dict:
    pe = data.get("procuringEntity", {})
    addr = pe.get("address", {})
    val = data.get("value", {})
    items = data.get("items", [])
    clf = items[0].get("classification", {}) if items else {}
    return {
        "id": tender_id, "limited": limited, "newTender": False,
        "title": data.get("title", ""), "procuringEntity": pe.get("name", ""),
        "amount": val.get("amount", 0), "currency": val.get("currency", "UAH"),
        "status": data.get("status", "active"), "datePublished": get_tender_date(data),
        "region": addr.get("region", ""), "locality": addr.get("locality", ""),
        "cpv": clf.get("id", ""), "cpvDescription": clf.get("description", ""), "url": url
    }

@app.get("/api/tender/{tender_id}")
async def get_tender_by_id(tender_id: str):
    log(f"🔍 ЗАПИТ ТЕНДЕРА: {tender_id}")
    prozorro_url = f"https://prozorro.gov.ua/tender/{tender_id}"
    is_ua_id = bool(re.match(r'^UA-\d{4}-\d{2}-\d{2}-', tender_id))

    # 1. Пошук в БД
    conn = get_conn()
    c = conn.cursor()
    c.execute('SELECT title, procuring_entity, amount, cpv, region, date_published FROM tenders WHERE id = ?', (tender_id,))
    row = c.fetchone()
    conn.close()
    if row:
        return {
            "id": tender_id, "limited": False, "title": row[0], "procuringEntity": row[1],
            "amount": row[2], "currency": "UAH", "status": "active",
            "datePublished": row[5], "region": row[4], "url": prozorro_url
        }

    async with httpx.AsyncClient(timeout=20.0) as client:
        # 2. Якщо це UUID (hex) - прямий запит
        if not is_ua_id:
            r = await client.get(f"{PROZORRO_API}/tenders/{tender_id}")
            if r.status_code == 200: return _build_tender_response(tender_id, r.json()["data"], prozorro_url)

        # 3. Пошук по TenderID (UA-...)
        # Спроба А: через фільтр API (працює для проіндексованих)
        r_list = await client.get(f"{PROZORRO_API}/tenders", params={"tenderID": tender_id, "opt_fields": "id"})
        candidates = r_list.json().get("data", []) if r_list.status_code == 200 else []

        # Спроба Б: якщо не знайдено - скануємо останні 100 (для нових тендерів)
        if not candidates:
            log("   ⚠️ Не знайдено в індексі. Скануємо стрім...")
            r_recent = await client.get(f"{PROZORRO_API}/tenders?limit=100&descending=1&opt_fields=id,tenderID")
            if r_recent.status_code == 200:
                candidates = [i for i in r_recent.json().get("data", []) if i.get("tenderID") == tender_id]

        if candidates:
            hex_id = candidates[0]["id"]
            r_det = await client.get(f"{PROZORRO_API}/tenders/{hex_id}")
            if r_det.status_code == 200:
                data = r_det.json()["data"]
                save_tender_now(0, {
                    "id": tender_id, "title": data.get("title",""), 
                    "procuringEntity": data.get("procuringEntity",{}).get("name",""),
                    "amount": data.get("value",{}).get("amount",0),
                    "cpv": data.get("items",[{}])[0].get("classification",{}).get("id",""),
                    "region": data.get("procuringEntity",{}).get("address",{}).get("region",""),
                    "datePublished": get_tender_date(data), "url": prozorro_url
                })
                return _build_tender_response(tender_id, data, prozorro_url)

    # 4. Fallback (якщо зовсім нічого)
    return {"id": tender_id, "limited": True, "newTender": True, "url": prozorro_url, "title": "Тендер знайдено (натисніть Prozorro)", "status": "unknown"}

@app.get("/api/filters")
async def get_filters():
    conn = get_conn(); c = conn.cursor()
    c.execute('SELECT id, name, keywords, cpv, region, procuring_entity, supplier, min_amount, max_amount, period_days, is_active, found_count FROM filters WHERE is_active = TRUE')
    rows = c.fetchall(); conn.close()
    return [{"id": r[0], "name": r[1], "keywords": r[2], "cpv": r[3], "region": r[4], "procuringEntity": r[5], "foundCount": r[11]} for r in rows]

@app.post("/api/filters")
async def create_filter(filter_data: FilterCreate, background_tasks: BackgroundTasks):
    conn = get_conn(); c = conn.cursor()
    c.execute(f'INSERT INTO filters (name, keywords, cpv, region, procuring_entity, min_amount, max_amount, period_days) VALUES ({P},{P},{P},{P},{P},{P},{P},{P}) {"RETURNING id" if USE_PG else ""}',
              (filter_data.name, filter_data.keywords, filter_data.cpv, filter_data.region, filter_data.procuringEntity, filter_data.minAmount, filter_data.maxAmount, filter_data.periodDays))
    f_id = c.fetchone()[0] if USE_PG else c.lastrowid
    conn.commit(); conn.close()
    background_tasks.add_task(check_filter_task, f_id)
    return {"id": f_id}

@app.get("/api/tenders")
async def get_tenders(limit: int = 200, filter_id: Optional[int] = None):
    conn = get_conn(); c = conn.cursor()
    query = f'SELECT t.id, t.title, t.procuring_entity, t.amount, t.cpv, t.region, t.date_published, t.url, f.name FROM tenders t LEFT JOIN filters f ON t.filter_id = f.id '
    if filter_id: query += f'WHERE t.filter_id = {P} '
    query += f'ORDER BY t.date_published DESC LIMIT {P}'
    c.execute(query, (filter_id, limit) if filter_id else (limit,))
    rows = c.fetchall(); conn.close()
    return [{"id": r[0], "title": r[1], "procuringEntity": r[2], "amount": r[3], "cpv": r[4], "region": r[5], "datePublished": r[6], "url": r[7], "filterName": r[8]} for r in rows]

@app.get("/api/stats")
async def get_stats():
    conn = get_conn(); c = conn.cursor()
    c.execute('SELECT COUNT(*) FROM tenders'); total = c.fetchone()[0]
    c.execute('SELECT COUNT(*) FROM filters WHERE is_active = TRUE'); active = c.fetchone()[0]
    conn.close()
    return {"total": total, "today": 0, "active": active}

@app.delete("/api/filters/{filter_id}")
async def delete_filter(filter_id: int):
    conn = get_conn(); c = conn.cursor()
    c.execute(f'UPDATE filters SET is_active = FALSE WHERE id = {P}', (filter_id,))
    conn.commit(); conn.close()
    return {"status": "ok"}

async def check_filter_task(filter_id: int):
    _queued_searches.add(filter_id)
    async with _search_lock:
        _queued_searches.discard(filter_id)
        _active_searches.add(filter_id)
        try:
            conn = get_conn(); c = conn.cursor()
            c.execute(f'SELECT keywords, cpv, region, procuring_entity, supplier, min_amount, max_amount, period_days FROM filters WHERE id = {P}', (filter_id,))
            fd = c.fetchone(); conn.close()
            if fd: await search_and_save(filter_id, *fd)
        finally: _active_searches.discard(filter_id)

app.mount("/static", StaticFiles(directory="static"), name="static")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 10000)))






