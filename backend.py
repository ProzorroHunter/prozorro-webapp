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
    
    # Видаляємо старі таблиці
    c.execute('DROP TABLE IF EXISTS filters')
    c.execute('DROP TABLE IF EXISTS tenders')
    
    # Нова структура з полями замовник і виконавець
    c.execute('''CREATE TABLE IF NOT EXISTS filters (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        keywords TEXT,
        cpv TEXT,
        region TEXT,
        procuring_entity TEXT,
        supplier TEXT,
        min_amount REAL,
        max_amount REAL,
        period_type TEXT,
        period_days INTEGER,
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
    procuringEntity: Optional[str] = None
    supplier: Optional[str] = None
    minAmount: Optional[float] = None
    maxAmount: Optional[float] = None
    periodType: Optional[str] = "month"
    periodDays: Optional[int] = 30

PROZORRO_API = "https://public-api.prozorro.gov.ua/api/2.5"

async def search_prozorro_tenders(keywords=None, cpv=None, region=None, procuring_entity=None, 
                                  supplier=None, min_amount=None, max_amount=None, period_days=30):
    params = {"descending": "1", "limit": "100"}
    date_from = (datetime.now() - timedelta(days=period_days)).isoformat()
    params["offset"] = date_from
    
    print(f"🔍 ФІЛЬТР: keywords='{keywords}', region='{region}', замовник='{procuring_entity}', виконавець='{supplier}', період={period_days}д")
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(f"{PROZORRO_API}/tenders", params=params)
            response.raise_for_status()
            data = response.json()
            
            print(f"📊 API повернув {len(data.get('data', []))} тендерів")
            
            tenders = []
            checked = 0
            
            for tender_data in data.get("data", [])[:100]:
                tender_id = tender_data.get("id")
                checked += 1
                
                try:
                    detail_response = await client.get(f"{PROZORRO_API}/tenders/{tender_id}")
                    detail_data = detail_response.json().get("data", {})
                    
                    match_result = matches_filter_with_log(
                        detail_data, keywords, cpv, region, procuring_entity, 
                        supplier, min_amount, max_amount, tender_id
                    )
                    
                    if match_result["matches"]:
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
                        print(f"✅ #{len(tenders)} ПІДХОДИТЬ: {tender_title[:50]}")
                    
                    if len(tenders) >= 10:
                        break
                    
                    if len(tenders) == 0 and checked <= 5:
                        tender_title = detail_data.get("title", "")[:80]
                        print(f"❌ {checked}. НЕ ПІДХОДИТЬ: {tender_title}")
                        print(f"   Причина: {match_result['reason']}")
                        
                except Exception as e:
                    if checked <= 3:
                        print(f"⚠️ Помилка {checked}: {e}")
                    continue
            
            print(f"📈 ПІДСУМОК: перевірено {checked}, знайдено {len(tenders)}")
            return tenders
            
    except Exception as e:
        print(f"❌ Помилка: {e}")
        return []

def matches_filter_with_log(tender, keywords, cpv, region, procuring_entity, 
                            supplier, min_amount, max_amount, tender_id):
    """Перевірка з логуванням"""
    
    # Ключові слова
    if keywords:
        title = tender.get("title", "").lower()
        description = tender.get("description", "").lower()
        full_text = title + " " + description
        
        keywords_list = [k.strip().lower() for k in keywords.split(",")]
        if not any(kw in full_text for kw in keywords_list):
            return {"matches": False, "reason": f"Немає ключових слів '{keywords}'"}
    
    # CPV
    if cpv:
        tender_cpv = tender.get("items", [{}])[0].get("classification", {}).get("id", "")
        if not tender_cpv.startswith(cpv[:3]):
            return {"matches": False, "reason": f"CPV не співпадає"}
    
    # Регіон
    if region:
        tender_region = tender.get("procuringEntity", {}).get("address", {}).get("region", "")
        tender_locality = tender.get("procuringEntity", {}).get("address", {}).get("locality", "")
        full_location = tender_region + " " + tender_locality
        
        if region.lower() not in full_location.lower():
            return {"matches": False, "reason": f"Регіон не співпадає"}
    
    # Замовник/Покупець
    if procuring_entity:
        tender_entity = tender.get("procuringEntity", {}).get("name", "").lower()
        if procuring_entity.lower() not in tender_entity:
            return {"matches": False, "reason": f"Замовник не співпадає"}
    
    # Виконавець/Продавець (перевіряємо awards)
    if supplier:
        awards = tender.get("awards", [])
        found_supplier = False
        
        for award in awards:
            if award.get("status") == "active":  # Тільки активні awards
                suppliers = award.get("suppliers", [])
                for s in suppliers:
                    supplier_name = s.get("name", "").lower()
                    if supplier.lower() in supplier_name:
                        found_supplier = True
                        break
            if found_supplier:
                break
        
        if not found_supplier:
            return {"matches": False, "reason": f"Виконавець не співпадає"}
    
    # Сума
    amount = tender.get("value", {}).get("amount", 0)
    if min_amount and amount < min_amount:
        return {"matches": False, "reason": f"Сума {amount} < {min_amount}"}
    if max_amount and amount > max_amount:
        return {"matches": False, "reason": f"Сума {amount} > {max_amount}"}
    
    return {"matches": True, "reason": "Всі критерії співпали"}

@app.get("/")
async def root():
    return FileResponse('static/index.html')

@app.get("/api/health")
async def health():
    return {"status": "ok", "service": "ProzorroHunter"}

@app.get("/api/tender/{tender_id}")
async def get_tender_by_id(tender_id: str):
    print(f"🔍 Пошук: {tender_id}")
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                response = await client.get(f"{PROZORRO_API}/tenders/{tender_id}")
                
                if response.status_code == 404:
                    raise HTTPException(status_code=404, detail="Тендер не знайдено")
                
                response.raise_for_status()
                data = response.json().get("data", {})
                
                return {
                    "id": tender_id,
                    "title": data.get("title", ""),
                    "procuringEntity": data.get("procuringEntity", {}).get("name", ""),
                    "amount": data.get("value", {}).get("amount", 0),
                    "status": data.get("status", ""),
                    "url": f"https://prozorro.gov.ua/tender/{tender_id}"
                }
                
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    raise HTTPException(status_code=404, detail="Тендер не знайдено")
                raise HTTPException(status_code=500, detail=f"Помилка API")
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail="Помилка сервера")

@app.get("/api/filters")
async def get_filters():
    conn = sqlite3.connect('prozorro.db')
    c = conn.cursor()
    c.execute('''SELECT id, name, keywords, cpv, region, procuring_entity, supplier,
                 min_amount, max_amount, period_type, period_days, is_active, found_count 
                 FROM filters WHERE is_active = 1''')
    rows = c.fetchall()
    conn.close()
    
    filters = []
    for row in rows:
        filters.append({
            "id": row[0], "name": row[1], "keywords": row[2], "cpv": row[3],
            "region": row[4], "procuringEntity": row[5], "supplier": row[6],
            "minAmount": row[7], "maxAmount": row[8],
            "periodType": row[9], "periodDays": row[10],
            "isActive": bool(row[11]), "foundCount": row[12]
        })
    return filters

@app.post("/api/filters")
async def create_filter(filter_data: FilterCreate, background_tasks: BackgroundTasks):
    conn = sqlite3.connect('prozorro.db')
    c = conn.cursor()
    c.execute('''INSERT INTO filters 
                 (name, keywords, cpv, region, procuring_entity, supplier, 
                  min_amount, max_amount, period_type, period_days)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
              (filter_data.name, filter_data.keywords, filter_data.cpv, 
               filter_data.region, filter_data.procuringEntity, filter_data.supplier,
               filter_data.minAmount, filter_data.maxAmount,
               filter_data.periodType, filter_data.periodDays))
    filter_id = c.lastrowid
    conn.commit()
    conn.close()
    
    print(f"✅ Фільтр #{filter_id}: {filter_data.name}")
    background_tasks.add_task(check_filter, filter_id)
    return {"id": filter_id, "message": "Фільтр створено"}

@app.post("/api/filters/{filter_id}/search")
async def search_filter_now(filter_id: int, background_tasks: BackgroundTasks):
    print(f"🚀 Пошук для фільтра #{filter_id}")
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
    conn = sqlite3.connect('prozorro.db')
    c = conn.cursor()
    c.execute('''SELECT keywords, cpv, region, procuring_entity, supplier, 
                 min_amount, max_amount, period_days 
                 FROM filters WHERE id = ?''', (filter_id,))
    filter_data = c.fetchone()
    if not filter_data:
        conn.close()
        return
    
    print(f"🔍 Перевірка фільтра #{filter_id}")
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
    print(f"✅ Фільтр #{filter_id}: додано {new_count} тендерів")

app.mount("/static", StaticFiles(directory="static"), name="static")

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 10000))
    uvicorn.run(app, host="0.0.0.0", port=port)
