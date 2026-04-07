from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Optional
import httpx
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

# 🔮 Зарезервовано для майбутньої функції відстеження тендерів через Telegram
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")

def init_db():
    """Ініціалізація БД БЕЗ видалення даних"""
    conn = sqlite3.connect('prozorro.db')
    c = conn.cursor()

    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='filters'")
    filters_exists = c.fetchone()

    if not filters_exists:
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
        print("✅ Таблиці існують")

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
                    detail_response = await client.get(f"{PROZORRO_API}/tenders/{tender_id}")
                    detail_data = detail_response.json().get("data", {})

                    result = check_filter_detailed(detail_data, keywords, cpv, region,
                                                   procuring_entity, supplier, min_amount, max_amount)

                    if result["matches"]:
                        items = detail_data.get("items", [])
                        tender = {
                            "id": tender_id,
                            "title": detail_data.get("title", "Без назви"),
                            "procuringEntity": detail_data.get("procuringEntity", {}).get("name", "Невідомо"),
                            "amount": detail_data.get("value", {}).get("amount", 0),
                            "cpv": items[0].get("classification", {}).get("id", "") if items else "",
                            "region": detail_data.get("procuringEntity", {}).get("address", {}).get("region", ""),
                            "datePublished": detail_data.get("datePublished", ""),
                            "url": f"https://prozorro.gov.ua/tender/{tender_id}"
                        }
                        tenders.append(tender)
                        print(f"✅ Знайдено #{len(tenders)}: {tender['title'][:60]}")
                    else:
                        if len(tenders) == 0 and checked <= 3:
                            title = detail_data.get("title", "")[:60]
                            print(f"❌ {checked}. НЕ ПІДХОДИТЬ: {title}")
                            print(f"   Причина: {result['reason']}")

                    if len(tenders) >= 10:
                        break

                except Exception as e:
                    if checked <= 3:
                        print(f"⚠️ Помилка тендера {tender_id}: {e}")
                    continue

            print(f"📈 Результат: {len(tenders)} тендерів з {checked} перевірених\n")
            return tenders

    except Exception as e:
        print(f"❌ КРИТИЧНА ПОМИЛКА: {e}")
        print(traceback.format_exc())
        return []


def check_filter_detailed(tender, keywords, cpv, region, procuring_entity, supplier, min_amount, max_amount):
    if keywords:
        title = tender.get("title", "").lower()
        description = tender.get("description", "").lower()
        text = title + " " + description
        kw_list = [k.strip().lower() for k in keywords.split(",")]
        found = [kw for kw in kw_list if kw in text]
        if not found:
            return {"matches": False, "reason": f"Немає жодного з ключових слів '{keywords}' в тексті"}

    if cpv:
        items = tender.get("items", [])
        if items:
            tender_cpv = items[0].get("classification", {}).get("id", "")
            if not tender_cpv.startswith(cpv[:3]):
                return {"matches": False, "reason": f"CPV не співпадає: потрібен {cpv[:3]}*, є {tender_cpv}"}

    if region:
        tender_region = tender.get("procuringEntity", {}).get("address", {}).get("region", "")
        tender_locality = tender.get("procuringEntity", {}).get("address", {}).get("locality", "")
        location = (tender_region + " " + tender_locality).lower()
        if region.lower() not in location:
            return {"matches": False, "reason": f"Регіон не співпадає: шукаємо '{region}', знайдено '{tender_region} {tender_locality}'"}

    if procuring_entity:
        entity = tender.get("procuringEntity", {}).get("name", "").lower()
        if procuring_entity.lower() not in entity:
            return {"matches": False, "reason": "Замовник не співпадає"}

    if supplier:
        found = False
        for award in tender.get("awards", []):
            if award.get("status") == "active":
                for s in award.get("suppliers", []):
                    if supplier.lower() in s.get("name", "").lower():
                        found = True
                        break
        if not found:
            return {"matches": False, "reason": "Виконавець не співпадає"}

    amount = tender.get("value", {}).get("amount", 0)
    if min_amount and amount < min_amount:
        return {"matches": False, "reason": f"Сума {amount} менша ніж мінімум {min_amount}"}
    if max_amount and amount > max_amount:
        return {"matches": False, "reason": f"Сума {amount} більша ніж максимум {max_amount}"}

    return {"matches": True, "reason": "OK"}


# ──────────────────────────────────────────────────────
#  ROUTES
# ──────────────────────────────────────────────────────

@app.get("/")
async def root():
    return FileResponse('static/index.html')

@app.get("/api/health")
async def health():
    return {"status": "ok"}


@app.get("/api/tender/{tender_id}")
async def get_tender_by_id(tender_id: str):
    """Пошук тендера за ID.
    Таймаут навмисно знижений до 15 сек, щоб вкластися в ліміт Render.com (30 сек на запит).
    """
    print(f"\n🔍 API REQUEST: GET /api/tender/{tender_id}")

    # Завжди формуємо посилання — воно правильне навіть якщо API нічого не дав
    prozorro_url = f"https://prozorro.gov.ua/tender/{tender_id}"

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:   # ← 15 сек, не 30

            # СПРОБА 1: Прямий доступ
            print(f"📡 Спроба 1: {PROZORRO_API}/tenders/{tender_id}")
            response = await client.get(f"{PROZORRO_API}/tenders/{tender_id}")
            print(f"📊 Статус: {response.status_code}")

            if response.status_code == 200:
                data  = response.json().get("data", {})
                items = data.get("items", [])
                print(f"✅ Знайдено прямим запитом\n")
                return {
                    "id":               tender_id,
                    "limited":          False,
                    "title":            data.get("title", ""),
                    "procuringEntity":  data.get("procuringEntity", {}).get("name", ""),
                    "amount":           data.get("value", {}).get("amount", 0),
                    "currency":         data.get("value", {}).get("currency", "UAH"),
                    "status":           data.get("status", ""),
                    "datePublished":    data.get("datePublished", ""),
                    "region":           data.get("procuringEntity", {}).get("address", {}).get("region", ""),
                    "locality":         data.get("procuringEntity", {}).get("address", {}).get("locality", ""),
                    "cpv":              items[0].get("classification", {}).get("id", "") if items else "",
                    "cpvDescription":   items[0].get("classification", {}).get("description", "") if items else "",
                    "url":              prozorro_url,
                }

            # СПРОБА 2: Пошук через список
            print(f"⚠️ Статус {response.status_code}, пробую пошук...")
            search_resp = await client.get(
                f"{PROZORRO_API}/tenders",
                params={"id": tender_id, "limit": 1}
            )

            if search_resp.status_code == 200:
                items_list = search_resp.json().get("data", [])
                if items_list:
                    t = items_list[0]
                    print(f"✅ Знайдено через пошук\n")
                    return {
                        "id":               tender_id,
                        "limited":          True,
                        "title":            t.get("title", ""),
                        "procuringEntity":  "",
                        "amount":           0,
                        "currency":         "UAH",
                        "status":           t.get("status", "unknown"),
                        "datePublished":    t.get("datePublished", ""),
                        "region":           "",
                        "locality":         "",
                        "cpv":              "",
                        "cpvDescription":   "",
                        "url":              prozorro_url,
                    }

            # СПРОБА 3: Просто посилання
            print(f"⚠️ API не знайшов даних — повертаємо посилання\n")
            return {
                "id": tender_id, "limited": True,
                "title": "", "procuringEntity": "", "amount": 0,
                "currency": "UAH", "status": "unknown",
                "datePublished": "", "region": "", "locality": "",
                "cpv": "", "cpvDescription": "", "url": prozorro_url,
            }

    except Exception as e:
        # Навіть при будь-якій помилці — повертаємо валідний JSON з посиланням
        print(f"❌ EXCEPTION в get_tender_by_id: {type(e).__name__}: {e}\n")
        return {
            "id": tender_id, "limited": True,
            "title": "", "procuringEntity": "", "amount": 0,
            "currency": "UAH", "status": "unknown",
            "datePublished": "", "region": "", "locality": "",
            "cpv": "", "cpvDescription": "", "url": prozorro_url,
        }


@app.get("/api/filters")
async def get_filters():
    conn = sqlite3.connect('prozorro.db')
    c = conn.cursor()
    c.execute('''SELECT id, name, keywords, cpv, region, procuring_entity, supplier,
                 min_amount, max_amount, period_days, is_active, found_count
                 FROM filters WHERE is_active = 1 ORDER BY id''')
    rows = c.fetchall()
    conn.close()
    return [
        {
            "id": row[0], "name": row[1], "keywords": row[2], "cpv": row[3],
            "region": row[4], "procuringEntity": row[5], "supplier": row[6],
            "minAmount": row[7], "maxAmount": row[8], "periodDays": row[9],
            "isActive": bool(row[10]), "foundCount": row[11]
        }
        for row in rows
    ]


@app.post("/api/filters")
async def create_filter(filter_data: FilterCreate, background_tasks: BackgroundTasks):
    conn = sqlite3.connect('prozorro.db')
    c = conn.cursor()
    c.execute(
        '''INSERT INTO filters (name, keywords, cpv, region, procuring_entity, supplier,
                                min_amount, max_amount, period_days)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
        (filter_data.name, filter_data.keywords, filter_data.cpv,
         filter_data.region, filter_data.procuringEntity, filter_data.supplier,
         filter_data.minAmount, filter_data.maxAmount, filter_data.periodDays)
    )
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
async def get_tenders(limit: int = 200, filter_id: Optional[int] = None):
    conn = sqlite3.connect('prozorro.db')
    c = conn.cursor()
    if filter_id:
        c.execute(
            '''SELECT t.id, t.title, t.procuring_entity, t.amount, t.cpv, t.region,
                      t.date_published, t.url, t.filter_id, f.name
               FROM tenders t LEFT JOIN filters f ON t.filter_id = f.id
               WHERE t.filter_id = ? ORDER BY t.date_published DESC LIMIT ?''',
            (filter_id, limit)
        )
    else:
        c.execute(
            '''SELECT t.id, t.title, t.procuring_entity, t.amount, t.cpv, t.region,
                      t.date_published, t.url, t.filter_id, f.name
               FROM tenders t LEFT JOIN filters f ON t.filter_id = f.id
               ORDER BY t.date_published DESC LIMIT ?''',
            (limit,)
        )
    rows = c.fetchall()
    conn.close()
    return [
        {
            "id": row[0], "title": row[1], "procuringEntity": row[2],
            "amount": row[3], "cpv": row[4], "region": row[5],
            "datePublished": row[6], "url": row[7],
            "filterId": row[8], "filterName": row[9]
        }
        for row in rows
    ]


@app.delete("/api/tenders/all")
async def clear_all_tenders():
    conn = sqlite3.connect('prozorro.db')
    c = conn.cursor()
    c.execute('DELETE FROM tenders')
    c.execute('UPDATE filters SET found_count = 0')
    conn.commit()
    conn.close()
    print(f"\n🗑️ Всі тендери очищено\n")
    return {"message": "Всі тендери очищено"}


@app.delete("/api/tenders/filter/{filter_id}")
async def clear_filter_tenders(filter_id: int):
    conn = sqlite3.connect('prozorro.db')
    c = conn.cursor()
    c.execute('DELETE FROM tenders WHERE filter_id = ?', (filter_id,))
    c.execute('UPDATE filters SET found_count = 0 WHERE id = ?', (filter_id,))
    conn.commit()
    conn.close()
    print(f"\n🗑️ Тендери фільтра #{filter_id} очищено\n")
    return {"message": f"Тендери фільтра #{filter_id} очищено"}


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
    conn = sqlite3.connect('prozorro.db')
    c = conn.cursor()
    c.execute(
        '''SELECT keywords, cpv, region, procuring_entity, supplier,
                  min_amount, max_amount, period_days
           FROM filters WHERE id = ?''',
        (filter_id,)
    )
    fd = c.fetchone()
    if not fd:
        conn.close()
        return

    tenders = await search_prozorro_tenders(
        fd[0], fd[1], fd[2], fd[3], fd[4], fd[5], fd[6], fd[7] or 30
    )

    new_count = 0
    for tender in tenders:
        c.execute('SELECT id FROM tenders WHERE id = ?', (tender["id"],))
        if c.fetchone():
            continue
        c.execute(
            '''INSERT OR IGNORE INTO tenders
               (id, title, procuring_entity, amount, cpv, region, date_published, url, filter_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (tender["id"], tender["title"], tender["procuringEntity"], tender["amount"],
             tender["cpv"], tender["region"], tender["datePublished"], tender["url"], filter_id)
        )
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
