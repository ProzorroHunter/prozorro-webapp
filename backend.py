from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Optional, List
import httpx
import asyncio
from datetime import datetime, timedelta
import sqlite3
import os

app = FastAPI(title="ProzorroHunter API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def init_db():
    conn = sqlite3.connect('prozorro.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS filters (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        keywords TEXT,
        cpv TEXT,
        region TEXT,
        min_amount REAL,
        max_amount REAL,
        is_active BOOLEAN DEFAULT 1,
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
        notified BOOLEAN DEFAULT 0,
        FOREIGN KEY (filter_id) REFERENCES filters(id)
    )''')
    conn.commit()
    conn.close()

init_db()

class FilterCreate(BaseModel):
    name: str
    keywords: Optional[str] = None
    cpv: Optional[str] = None
    region: Optional[str] = None
    minAmount: Optional[float] = None
    maxAmount: Optional[float] = None

PROZORRO_API = "https://public-api.prozorro.gov.ua/api/2.5"

async def search_prozorro_tenders(keywords=None, cpv=None, region=None, min_amount=None, max_amount=None):
    params = {"descending": "1", "limit": "100"}
    date_from = (datetime.now() - timedelta(days=7)).isoformat()  # Збільшили до 7 днів
    params["offset"] = date_from
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(f"{PROZORRO_API}/tenders", params=params)
            response.raise_for_status()
            data = response.json()
            
            tenders = []
            for tender_data in data.get("data", [])[:20]:
                tender_id = tender_data.get("id")
                try:
                    detail_response = await client.get(f"{PROZORRO_API}/tenders/{tender_id}")
                    detail_data = detail_response.json().get("data", {})
                    
                    if not matches_filter(detail_data, keywords, cpv, region, min_amount, max_amount):
                        continue
                    
                    tender = {
                        "id": tender_id,
                        "title": detail_data.get("title", "Без назви"),
                        "procuringEntity": detail_data.get("procuringEntity", {}).get("name", "Невідомо"),
                        "amount": detail_data.get("value", {}).get("amount", 0),
                        "cpv": detail_data.get("items", [{}])[0].get("classification", {}).get("id", ""),
                        "region": detail_data.get("procuringEntity", {}).get("address", {}).get("region", ""),
                        "datePublished": detail_data.get("datePublished", ""),
                        "url": f"https://prozorro.gov.ua/tender/{tender_id}"
                    }
                    tenders.append(tender)
                    
                    if len(tenders) >= 5:  # Максимум 5 тендерів
                        break
                except Exception as e:
                    print(f"Помилка тендера {tender_id}: {e}")
                    continue
                    
            return tenders
    except Exception as e:
        print(f"Помилка пошуку: {e}")
        return []

def matches_filter(tender, keywords, cpv, region, min_amount, max_amount):
    if keywords:
        title = tender.get("title", "").lower()
        keywords_list = [k.strip().lower() for k in keywords.split(",")]
        if not any(keyword in title for keyword in keywords_list):
            return False
    if cpv:
        tender_cpv = tender.get("items", [{}])[0].get("classification", {}).get("id", "")
        if not tender_cpv.startswith(cpv[:3]):
            return False
    if region:
        tender_region = tender.get("procuringEntity", {}).get("address", {}).get("region", "")
        if region.lower() not in tender_region.lower():
            return False
    amount = tender.get("value", {}).get("amount", 0)
    if min_amount and amount < min_amount:
        return False
    if max_amount and amount > max_amount:
        return False
    return True

@app.get("/")
async def root():
    return FileResponse('static/index.html')

@app.get("/api/health")
async def health():
    return {"status": "ok", "service": "ProzorroHunter"}

@app.get("/api/filters")
async def get_filters():
    conn = sqlite3.connect('prozorro.db')
    c = conn.cursor()
    c.execute('SELECT id, name, keywords, cpv, region, min_amount, max_amount, is_active, found_count FROM filters WHERE is_active = 1')
    rows = c.fetchall()
    conn.close()
    
    filters = []
    for row in rows:
        filters.append({
            "id": row[0], "name": row[1], "keywords": row[2], "cpv": row[3],
            "region": row[4], "minAmount": row[5], "maxAmount": row[6],
            "isActive": bool(row[7]), "foundCount": row[8]
        })
    return filters

@app.post("/api/filters")
async def create_filter(filter_data: FilterCreate, background_tasks: BackgroundTasks):
    conn = sqlite3.connect('prozorro.db')
    c = conn.cursor()
    c.execute('''INSERT INTO filters (name, keywords, cpv, region, min_amount, max_amount)
                 VALUES (?, ?, ?, ?, ?, ?)''',
              (filter_data.name, filter_data.keywords, filter_data.cpv, 
               filter_data.region, filter_data.minAmount, filter_data.maxAmount))
    filter_id = c.lastrowid
    conn.commit()
    conn.close()
    
    background_tasks.add_task(check_filter, filter_id)
    return {"id": filter_id, "message": "Фільтр створено"}

@app.post("/api/filters/{filter_id}/search")
async def search_filter_now(filter_id: int, background_tasks: BackgroundTasks):
    """Ручний запуск пошуку для фільтра"""
    background_tasks.add_task(check_filter, filter_id)
    return {"message": "Пошук запущено"}

@app.delete("/api/filters/{filter_id}")
async def delete_filter(filter_id: int):
    conn = sqlite3.connect('prozorro.db')
    c = conn.cursor()
    c.execute('UPDATE filters SET is_active = 0 WHERE id = ?', (filter_id,))
    conn.commit()
    conn.close()
    return {"message": "Фільтр видалено"}

@app.get("/api/tenders")
async def get_tenders(limit: int = 50):
    conn = sqlite3.connect('prozorro.db')
    c = conn.cursor()
    c.execute('''SELECT id, title, procuring_entity, amount, cpv, region, date_published, url
                 FROM tenders ORDER BY date_published DESC LIMIT ?''', (limit,))
    rows = c.fetchall()
    conn.close()
    
    tenders = []
    for row in rows:
        tenders.append({
            "id": row[0], "title": row[1], "procuringEntity": row[2],
            "amount": row[3], "cpv": row[4], "region": row[5],
            "datePublished": row[6], "url": row[7]
        })
    return tenders

@app.get("/api/stats")
async def get_stats():
    conn = sqlite3.connect('prozorro.db')
    c = conn.cursor()
    c.execute('SELECT COUNT(*) FROM tenders')
    total = c.fetchone()[0]
    today = datetime.now().date().isoformat()
    c.execute('SELECT COUNT(*) FROM tenders WHERE DATE(date_published) = ?', (today,))
    today_count = c.fetchone()[0]
    c.execute('SELECT COUNT(*) FROM filters WHERE is_active = 1')
    active_filters = c.fetchone()[0]
    conn.close()
    return {"total": total, "today": today_count, "active": active_filters}

async def check_filter(filter_id: int):
    """Перевірка фільтра та пошук нових тендерів"""
    conn = sqlite3.connect('prozorro.db')
    c = conn.cursor()
    c.execute('SELECT keywords, cpv, region, min_amount, max_amount FROM filters WHERE id = ?', (filter_id,))
    filter_data = c.fetchone()
    if not filter_data:
        conn.close()
        return
    
    print(f"🔍 Шукаю тендери для фільтра {filter_id}...")
    tenders = await search_prozorro_tenders(filter_data[0], filter_data[1], filter_data[2], filter_data[3], filter_data[4])
    
    new_count = 0
    for tender in tenders:
        c.execute('SELECT id FROM tenders WHERE id = ?', (tender["id"],))
        if c.fetchone():
            continue
        c.execute('''INSERT OR IGNORE INTO tenders 
                     (id, title, procuring_entity, amount, cpv, region, date_published, url, filter_id)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                  (tender["id"], tender["title"], tender["procuringEntity"], tender["amount"],
                   tender["cpv"], tender["region"], tender["datePublished"], tender["url"], filter_id))
        new_count += 1
    
    c.execute('UPDATE filters SET found_count = found_count + ? WHERE id = ?', (new_count, filter_id))
    conn.commit()
    conn.close()
    print(f"✅ Фільтр {filter_id}: знайдено {new_count} нових тендерів")

app.mount("/static", StaticFiles(directory="static"), name="static")

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 10000))
    uvicorn.run(app, host="0.0.0.0", port=port)
