from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Optional
import httpx
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
    """Ініціалізація БД БЕЗ видалення даних"""
    conn = sqlite3.connect('prozorro.db')
    c = conn.cursor()
    
    # Перевіряємо чи існують таблиці
    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='filters'")
    filters_exists = c.fetchone()
    
    if not filters_exists:
        # Створюємо таблиці ТІЛЬКИ якщо їх немає
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
        
        print("✅ Таблиці створено")
    else:
        print("✅ Таблиці вже існують")
    
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

async def search_prozorro_tenders(keywords=None, cpv=None, region=None, procuring_entity=None, 
                                  supplier=None, min_amount=None, max_amount=None, period_days=30):
    """Пошук тендерів з детальним логуванням"""
    params = {"descending": "1", "limit": "100"}
    date_from = (datetime.now() - timedelta(days=period_days)).isoformat()
    params["offset"] = date_from
    
    print(f"\n🔍 ПОШУК ТЕНДЕРІВ:")
    print(f"   Keywords: {keywords}")
    print(f"   Region: {region}")
    print(f"   Procuring: {procuring_entity}")
    print(f"   Supplier: {supplier}")
    print(f"   Period: {period_days} днів")
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(f"{PROZORRO_API}/tenders", params=params)
            response.raise_for_status()
            data = response.json()
            
            total = len(data.get('data', []))
            print(f"📊 Отримано {total} тендерів з API")
            
            tenders = []
            checked = 0
            
            for tender_data in data.get("data", [])[:100]:
                tender_id = tender_data.get("id")
                checked += 1
                
                try:
                    # Детальна інфо
                    detail_response = await client.get(f"{PROZORRO_API}/tenders/{tender_id}")
                    detail_data = detail_response.json().get("data", {})
                    
                    # Перевірка фільтра
                    passes = check_filter(detail_data, keywords, cpv, region, 
                                         procuring_entity, supplier, min_amount, max_amount)
                    
                    if passes:
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
                        print(f"✅ Знайдено #{len(tenders)}: {tender['title'][:60]}")
                    
                    if len(tenders) >= 10:
                        break
                        
                except Exception as e:
                    if checked <= 3:
                        print(f"⚠️ Помилка тендера {tender_id}: {e}")
                    continue
            
            print(f"📈 Результат: {len(tenders)} тендерів з {checked} перевірених\n")
            return tenders
            
    except Exception as e:
        print(f"❌ КРИТИЧНА ПОМИЛКА: {e}\n")
        return []

def check_filter(tender, keywords, cpv, region, procuring_entity, supplier, min_amount, max_amount):
    """Проста перевірка БЕЗ зайвої строгості"""
    
    # Keywords
    if keywords:
        text = (tender.get("title", "") + " " + tender.get("description", "")).lower()
        kw_list = [k.strip().lower() for k in keywords.split(",")]
        if not any(kw in text for kw in kw_list):
            return False
    
    # CPV
    if cpv:
        tender_cpv = tender.get("items", [{}])[0].get("classification", {}).get("id", "")
        if not tender_cpv.startswith(cpv[:3]):
            return False
    
    # Region
    if region:
        tender_region = tender.get("procuringEntity", {}).get("address", {}).get("region", "")
        tender_locality = tender.get("procuringEntity", {}).get("address", {}).get("locality", "")
        location = (tender_region + " " + tender_locality).lower()
        if region.lower() not in location:
            return False
    
    # Procuring Entity
    if procuring_entity:
        entity = tender.get("procuringEntity", {}).get("name", "").lower()
        if procuring_entity.lower() not in entity:
            return False
    
    # Supplier
    if supplier:
        found = False
        for award in tender.get("awards", []):
            if award.get("status") == "active":
                for s in award.get("suppliers", []):
                    if supplier.lower() in s.get("name", "").lower():
                        found = True
                        break
        if not found:
            return False
    
    # Amount
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
    return {"status": "ok"}

@app.get("/api/tender/{tender_id}")
async def get_tender_by_id(tender_id: str):
    """Пошук тендера за ID"""
    print(f"\n🔍 Пошук тендера: {tender_id}")
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            url = f"{PROZORRO_API}/tenders/{tender_id}"
            response = await client.get(url)
            
            print(f"📡 Статус відповіді: {response.status_code}")
            
            if response.status_code == 404:
                print(f"❌ Тендер не знайдено\n")
                return {"error": "not_found", "message": "Тендер не знайдено"}
            
            if response.status_code != 200:
                print(f"❌ Помилка API: {response.status_code}\n")
                return {"error": "api_error", "message": f"Помилка API: {response.status_code}"}
            
            data = response.json().get("data", {})
            
            result = {
                "id": tender_id,
                "title": data.get("title", ""),
                "procuringEntity": data.get("procuringEntity", {}).get("name", ""),
                "amount": data.get("value", {}).get("amount", 0),
                "status": data.get("status", ""),
                "url": f"https://prozorro.gov.ua/tender/{tender_id}"
            }
            
            print(f"✅ Знайдено: {result['title'][:50]}\n")
            return result
            
    except Exception as e:
        print(f"❌ Помилка: {e}\n")
        return {"error": "exception", "message": str(e)}

@app.get("/api/filters")
async def get_filters():
    conn = sqlite3.connect('prozorro.db')
    c = conn.cursor()
    c.execute('''SELECT id, name, keywords, cpv, region, procuring_entity, supplier,
                 min_amount, max_amount, period_days, is_active, found_count 
                 FROM filters WHERE is_active = 1''')
    rows = c.fetchall()
    conn.close()
    
    filters = []
    for row in rows:
        filters.append({
            "id": row[0], "name": row[1], "keywords": row[2], "cpv": row[3],
            "region": row[4], "procuringEntity": row[5], "supplier": row[6],
            "minAmount": row[7], "maxAmount": row[8], "periodDays": row[9],
            "isActive": bool(row[10]), "foundCount": row[11]
        })
    return filters

@app.post("/api/filters")
async def create_filter(filter_data: FilterCreate, background_tasks: BackgroundTasks):
    conn = sqlite3.connect('prozorro.db')
    c = conn.cursor()
    c.execute('''INSERT INTO filters 
                 (name, keywords, cpv, region, procuring_entity, supplier, 
                  min_amount, max_amount, period_days)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
              (filter_data.name, filter_data.keywords, filter_data.cpv, 
               filter_data.region, filter_data.procuringEntity, filter_data.supplier,
               filter_data.minAmount, filter_data.maxAmount, filter_data.periodDays))
    filter_id = c.lastrowid
    conn.commit()
    conn.close()
    
    print(f"\n✅ Створено фільтр #{filter_id}: {filter_data.name}\n")
    background_tasks.add_task(check_filter_task, filter_id)
    return {"id": filter_id, "message": "Фільтр створено"}

@app.post("/api/filters/{filter_id}/search")
async def search_filter_now(filter_id: int, background_tasks: BackgroundTasks):
    print(f"\n🚀 Ручний пошук для фільтра #{filter_id}\n")
    background_tasks.add_task(check_filter_task, filter_id)
    return {"message": "Пошук запущено"}

@app.delete("/api/filters/{filter_id}")
async def delete_filter(filter_id: int):
    conn = sqlite3.connect('prozorro.db')
    c = conn.cursor()
    c.execute('UPDATE filters SET is_active = 0 WHERE id = ?', (filter_id,))
    conn.commit()
    conn.close()
    print(f"\n🗑️ Видалено фільтр #{filter_id}\n")
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

async def check_filter_task(filter_id: int):
    """Background task для перевірки фільтра"""
    conn = sqlite3.connect('prozorro.db')
    c = conn.cursor()
    c.execute('''SELECT keywords, cpv, region, procuring_entity, supplier, 
                 min_amount, max_amount, period_days 
                 FROM filters WHERE id = ?''', (filter_id,))
    filter_data = c.fetchone()
    if not filter_data:
        conn.close()
        return
    
    tenders = await search_prozorro_tenders(
        filter_data[0], filter_data[1], filter_data[2], filter_data[3], 
        filter_data[4], filter_data[5], filter_data[6], filter_data[7] or 30
    )
    
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
