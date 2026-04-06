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
import traceback

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
    date_from = (datetime.now() - timedelta(days=7)).isoformat()
    params["offset"] = date_from
    
    print(f"🔍 Пошук з фільтром: keywords={keywords}, cpv={cpv}, region={region}, amount={min_amount}-{max_amount}")
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(f"{PROZORRO_API}/tenders", params=params)
            response.raise_for_status()
            data = response.json()
            
            total_from_api = len(data.get('data', []))
            print(f"📊 Prozorro API повернув {total_from_api} тендерів")
            
            tenders = []
            checked = 0
            
            for tender_data in data.get("data", [])[:50]:  # Перевіряємо більше
                tender_id = tender_data.get("id")
                checked += 1
                
                try:
                    detail_response = await client.get(f"{PROZORRO_API}/tenders/{tender_id}")
                    detail_data = detail_response.json().get("data", {})
                    
                    # СТРОГА перевірка - ВСІ критерії мають співпадати
                    if not matches_filter_strict(detail_data, keywords, cpv, region, min_amount, max_amount):
                        continue
                    
                    tender_title = detail_data.get("title", "Без назви")
                    tender_region = detail_data.get("procuringEntity", {}).get("address", {}).get("region", "")
                    tender_amount = detail_data.get("value", {}).get("amount", 0)
                    
                    tender = {
                        "id": tender_id,
                        "title": tender_title,
                        "procuringEntity": detail_data.get("procuringEntity", {}).get("name", "Невідомо"),
                        "amount": tender_amount,
                        "cpv": detail_data.get("items", [{}])[0].get("classification", {}).get("id", ""),
                        "region": tender_region,
                        "datePublished": detail_data.get("datePublished", ""),
                        "url": f"https://prozorro.gov.ua/tender/{tender_id}"
                    }
                    tenders.append(tender)
                    print(f"✅ ПІДХОДИТЬ: {tender_title[:50]}... | Регіон: {tender_region} | Сума: {tender_amount}")
                    
                    if len(tenders) >= 10:
                        break
                        
                except Exception as e:
                    continue
            
            print(f"📈 Результат: перевірено {checked}, знайдено {len(tenders)} підходящих")
            return tenders
            
    except Exception as e:
        print(f"❌ Помилка пошуку: {e}")
        print(traceback.format_exc())
        return []

def matches_filter_strict(tender, keywords, cpv, region, min_amount, max_amount):
    """СТРОГА перевірка - ВСІ вказані критерії мають співпадати"""
    
    # Ключові слова (якщо вказані)
    if keywords:
        title = tender.get("title", "").lower()
        description = tender.get("description", "").lower()
        full_text = title + " " + description
        
        keywords_list = [k.strip().lower() for k in keywords.split(",")]
        # Хоча б одне ключове слово має бути
        if not any(keyword in full_text for keyword in keywords_list):
            return False
    
    # CPV (якщо вказаний)
    if cpv:
        tender_cpv = tender.get("items", [{}])[0].get("classification", {}).get("id", "")
        if not tender_cpv.startswith(cpv[:3]):
            return False
    
    # Регіон (якщо вказаний)
    if region:
        tender_region = tender.get("procuringEntity", {}).get("address", {}).get("region", "")
        # Точна перевірка регіону
        if region.lower() not in tender_region.lower():
            return False
    
    # Сума (якщо вказана)
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

@app.get("/api/tender/{tender_id}")
async def get_tender_by_id(tender_id: str):
    """Отримати тендер за номером - з кращим error handling"""
    print(f"🔍 Запит тендера: {tender_id}")
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            url = f"{PROZORRO_API}/tenders/{tender_id}"
            print(f"📡 GET {url}")
            
            try:
                response = await client.get(url)
                print(f"📊 Статус: {response.status_code}")
                
                if response.status_code == 404:
                    print(f"❌ Тендер не існує: {tender_id}")
                    raise HTTPException(status_code=404, detail="Тендер не знайдено. Перевірте правильність номера.")
                
                response.raise_for_status()
                
            except httpx.HTTPStatusError as e:
                print(f"❌ HTTP помилка: {e.response.status_code}")
                if e.response.status_code == 404:
                    raise HTTPException(status_code=404, detail="Тендер не знайдено. Перевірте номер.")
                raise HTTPException(status_code=500, detail=f"Помилка Prozorro API ({e.response.status_code})")
            
            data = response.json().get("data", {})
            
            result = {
                "id": tender_id,
                "title": data.get("title", ""),
                "procuringEntity": data.get("procuringEntity", {}).get("name", ""),
                "amount": data.get("value", {}).get("amount", 0),
                "status": data.get("status", ""),
                "url": f"https://prozorro.gov.ua/tender/{tender_id}"
            }
            
            print(f"✅ Знайдено: {result['title'][:60]}...")
            return result
            
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Критична помилка: {e}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Не вдалося отримати тендер. Спробуйте пізніше.")

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
    
    print(f"✅ Створено фільтр #{filter_id}: {filter_data.name}")
    background_tasks.add_task(check_filter, filter_id)
    return {"id": filter_id, "message": "Фільтр створено"}

@app.post("/api/filters/{filter_id}/search")
async def search_filter_now(filter_id: int, background_tasks: BackgroundTasks):
    """Ручний запуск пошуку для фільтра"""
    print(f"🚀 Ручний пошук для фільтра #{filter_id}")
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
    
    print(f"🔍 Шукаю тендери для фільтра #{filter_id}...")
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
    print(f"✅ Фільтр #{filter_id}: додано {new_count} нових тендерів")

app.mount("/static", StaticFiles(directory="static"), name="static")

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 10000))
    uvicorn.run(app, host="0.0.0.0", port=port)
