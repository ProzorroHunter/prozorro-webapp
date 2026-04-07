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

app = FastAPI(title="ProzorroHunter API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")

# Один пошук за раз — щоб не блокувати SQLite
_search_lock = asyncio.Lock()


def log(*args):
    print(*args, flush=True)


def init_db():
    conn = sqlite3.connect('prozorro.db')
    c = conn.cursor()
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
        log("✅ Таблиці створено")
    else:
        log("✅ Таблиці існують")
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


def make_kw_list(keywords: Optional[str]):
    if not keywords:
        return []
    return [k.strip().lower() for k in keywords.split(",") if k.strip()]


def normalize_region(r: str) -> str:
    return (r.lower()
             .replace("місто ", "").replace("м. ", "")
             .replace(" область", "").replace("область", "")
             .strip())


def save_tender_now(filter_id: int, tender: dict) -> bool:
    """
    Зберігає один тендер у БД одразу після знаходження.
    Повертає True якщо тендер новий (раніше не зустрічався).
    """
    try:
        conn = sqlite3.connect('prozorro.db', timeout=10)
        c = conn.cursor()
        c.execute('SELECT id FROM tenders WHERE id = ?', (tender["id"],))
        if c.fetchone():
            conn.close()
            return False   # вже є
        c.execute(
            '''INSERT OR IGNORE INTO tenders
               (id, title, procuring_entity, amount, cpv, region,
                date_published, url, filter_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (tender["id"], tender["title"], tender["procuringEntity"],
             tender["amount"], tender["cpv"], tender["region"],
             tender["datePublished"], tender["url"], filter_id)
        )
        saved = c.rowcount > 0
        if saved:
            c.execute(
                'UPDATE filters SET found_count = found_count + 1 WHERE id = ?',
                (filter_id,)
            )
        conn.commit()
        conn.close()
        return saved
    except Exception as e:
        log(f"   ⚠️ Помилка збереження тендера: {e}")
        return False


async def search_and_save(filter_id: int, keywords=None, cpv=None, region=None,
                          procuring_entity=None, supplier=None,
                          min_amount=None, max_amount=None, period_days=30):
    """
    Ключова зміна: тендер зберігається в БД ОДРАЗУ як знайдено,
    а не після завершення всього пошуку.
    Це дозволяє UI показувати результати в реальному часі через полінг.
    """
    date_from = (datetime.now() - timedelta(days=period_days)).isoformat()
    kw_list   = make_kw_list(keywords)

    log(f"\n🔍 ПОШУК (фільтр #{filter_id}):")
    log(f"   Keywords : {keywords}")
    log(f"   Region   : {region}")
    log(f"   CPV      : {cpv}")
    log(f"   Period   : {period_days} днів (від {date_from[:10]})")

    total_listed  = 0
    total_details = 0
    total_saved   = 0
    offset        = None   # Перша сторінка БЕЗ offset — найсвіжіші тендери

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:

            for page_num in range(15):

                params = {
                    "descending": "1",
                    "limit":      "100",
                    "opt_fields": "id,title,status,dateModified",
                }
                if offset:
                    params["offset"] = offset

                try:
                    resp = await client.get(f"{PROZORRO_API}/tenders", params=params)
                    resp.raise_for_status()
                except Exception as e:
                    log(f"   ⚠️ Помилка запиту сторінки {page_num+1}: {e}")
                    break

                body       = resp.json()
                page_items = body.get("data", [])

                if not page_items:
                    log(f"   ⏹ Порожня відповідь API")
                    break

                total_listed += len(page_items)
                next_offset   = body.get("next_page", {}).get("offset")

                log(f"   📄 Стор.{page_num+1}: {len(page_items)} тендерів "
                    f"(переглянуто {total_listed})")

                reached_boundary = False

                for item in page_items:
                    tender_id  = item.get("id", "")
                    item_date  = item.get("dateModified", "")
                    list_title = item.get("title", "")

                    # Вийшли за межу часового вікна
                    if item_date and item_date < date_from:
                        log(f"   ⏹ Межа {period_days} днів досягнута "
                            f"({item_date[:10]} < {date_from[:10]})")
                        reached_boundary = True
                        break

                    # Швидкий pre-filter по title (без detail-запиту)
                    if list_title and kw_list:
                        if not any(kw in list_title.lower() for kw in kw_list):
                            continue

                    # Detail-запит
                    total_details += 1
                    try:
                        dr = await client.get(f"{PROZORRO_API}/tenders/{tender_id}")
                        if dr.status_code != 200:
                            continue
                        dd = dr.json().get("data", {})
                    except Exception:
                        continue

                    detail_date = dd.get("dateModified", dd.get("datePublished", ""))
                    if detail_date and detail_date < date_from:
                        reached_boundary = True
                        break

                    result = check_filter_detailed(
                        dd, keywords, cpv, region,
                        procuring_entity, supplier, min_amount, max_amount
                    )

                    if result["matches"]:
                        items_list = dd.get("items", [])
                        tender = {
                            "id":              tender_id,
                            "title":           dd.get("title", "Без назви"),
                            "procuringEntity": dd.get("procuringEntity", {}).get("name", "Невідомо"),
                            "amount":          dd.get("value", {}).get("amount", 0),
                            "cpv":             items_list[0].get("classification", {}).get("id", "") if items_list else "",
                            "region":          dd.get("procuringEntity", {}).get("address", {}).get("region", ""),
                            "datePublished":   dd.get("datePublished", ""),
                            "url":             f"https://prozorro.gov.ua/tender/{tender_id}",
                        }

                        # ── ЗБЕРІГАЄМО ОДРАЗУ ─────────────────────────────
                        # Не чекаємо кінця пошуку — UI побачить через 5 сек
                        if save_tender_now(filter_id, tender):
                            total_saved += 1
                            log(f"   ✅ #{total_saved} збережено: {tender['title'][:65]}")
                        else:
                            log(f"   ⏭ #{tender_id[:20]}... вже є в БД")

                        if total_saved >= 10:
                            log(f"   ⏹ Знайдено максимум (10 тендерів)")
                            break
                    else:
                        if total_details <= 5:
                            log(f"   ❌ detail #{total_details}: {dd.get('title','')[:50]}")
                            log(f"      → {result['reason']}")

                if total_saved >= 10 or reached_boundary:
                    break

                if not next_offset:
                    log(f"   ⏹ Немає наступної сторінки")
                    break

                offset = next_offset

    except Exception as e:
        log(f"❌ КРИТИЧНА ПОМИЛКА: {e}")
        log(traceback.format_exc())

    log(f"   📊 Переглянуто: {total_listed} | Detail-запитів: {total_details}")
    log(f"   📈 Збережено нових тендерів: {total_saved}\n")
    return total_saved


def check_filter_detailed(tender, keywords, cpv, region, procuring_entity,
                           supplier, min_amount, max_amount):
    if keywords:
        text    = (tender.get("title", "") + " " + tender.get("description", "")).lower()
        kw_list = make_kw_list(keywords)
        if not any(kw in text for kw in kw_list):
            return {"matches": False,
                    "reason": f"Немає жодного з ключових слів '{keywords}' в тексті"}

    if cpv:
        items = tender.get("items", [])
        if items:
            tender_cpv = items[0].get("classification", {}).get("id", "")
            if not tender_cpv.startswith(cpv[:3]):
                return {"matches": False,
                        "reason": f"CPV: потрібен {cpv[:3]}*, є {tender_cpv}"}

    if region:
        addr     = tender.get("procuringEntity", {}).get("address", {})
        t_region = addr.get("region",   "")
        t_local  = addr.get("locality", "")
        full_loc = (t_region + " " + t_local).lower()
        r_lower  = region.lower()
        r_norm   = normalize_region(region)
        loc_norm = normalize_region(t_region) + " " + normalize_region(t_local)
        if r_lower not in full_loc and r_norm not in loc_norm:
            return {"matches": False,
                    "reason": f"Регіон: шукаємо '{region}', є '{t_region} {t_local}'"}

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
        return {"matches": False, "reason": f"Сума {amount} < мін. {min_amount}"}
    if max_amount and amount > max_amount:
        return {"matches": False, "reason": f"Сума {amount} > макс. {max_amount}"}

    return {"matches": True, "reason": "OK"}


# ══════════════════════════════════════════════════════
#  ROUTES
# ══════════════════════════════════════════════════════

@app.get("/")
async def root():
    return FileResponse('static/index.html')

@app.get("/api/health")
async def health():
    return {"status": "ok"}


@app.get("/api/tender/{tender_id}")
async def get_tender_by_id(tender_id: str):
    log(f"\n🔍 ПОШУК ТЕНДЕРА: {tender_id}")
    prozorro_url = f"https://prozorro.gov.ua/tender/{tender_id}"
    fallback = {
        "id": tender_id, "limited": True, "newTender": False,
        "title": "", "procuringEntity": "", "amount": 0,
        "currency": "UAH", "status": "unknown",
        "datePublished": "", "region": "", "locality": "",
        "cpv": "", "cpvDescription": "", "url": prozorro_url,
    }
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            r1 = await client.get(f"{PROZORRO_API}/tenders/{tender_id}")
            log(f"   Спроба 1 → {r1.status_code}")
            if r1.status_code == 200:
                data  = r1.json().get("data", {})
                items = data.get("items", [])
                return {
                    "id": tender_id, "limited": False, "newTender": False,
                    "title":          data.get("title", ""),
                    "procuringEntity":data.get("procuringEntity", {}).get("name", ""),
                    "amount":         data.get("value", {}).get("amount", 0),
                    "currency":       data.get("value", {}).get("currency", "UAH"),
                    "status":         data.get("status", ""),
                    "datePublished":  data.get("datePublished", ""),
                    "region":         data.get("procuringEntity", {}).get("address", {}).get("region", ""),
                    "locality":       data.get("procuringEntity", {}).get("address", {}).get("locality", ""),
                    "cpv":            items[0].get("classification", {}).get("id", "") if items else "",
                    "cpvDescription": items[0].get("classification", {}).get("description", "") if items else "",
                    "url":            prozorro_url,
                }
            r2 = await client.get(f"{PROZORRO_API}/tenders",
                                   params={"id": tender_id, "limit": 1})
            if r2.status_code == 200:
                lst = r2.json().get("data", [])
                if lst:
                    t = lst[0]
                    return {**fallback, "limited": True, "newTender": False,
                            "title": t.get("title", ""), "status": t.get("status", "unknown"),
                            "datePublished": t.get("datePublished", "")}
            r3 = await client.get(f"{PROZORRO_API}/tenders",
                                   params={"descending": "1", "limit": "50",
                                           "opt_fields": "id,title,status,dateModified"})
            if r3.status_code == 200:
                for t in r3.json().get("data", []):
                    if t.get("id") == tender_id:
                        return {**fallback, "limited": True, "newTender": False,
                                "title": t.get("title", ""), "status": t.get("status", "unknown")}
            fallback["newTender"] = True
            return fallback
    except Exception as e:
        log(f"❌ EXCEPTION get_tender: {type(e).__name__}: {e}")
        return fallback


@app.get("/api/filters")
async def get_filters():
    conn = sqlite3.connect('prozorro.db')
    c = conn.cursor()
    c.execute('''SELECT id, name, keywords, cpv, region, procuring_entity, supplier,
                 min_amount, max_amount, period_days, is_active, found_count
                 FROM filters WHERE is_active = 1 ORDER BY id''')
    rows = c.fetchall()
    conn.close()
    return [{"id": r[0], "name": r[1], "keywords": r[2], "cpv": r[3],
             "region": r[4], "procuringEntity": r[5], "supplier": r[6],
             "minAmount": r[7], "maxAmount": r[8], "periodDays": r[9],
             "isActive": bool(r[10]), "foundCount": r[11]}
            for r in rows]


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
    log(f"\n✅ Створено фільтр #{filter_id}: {filter_data.name}\n")
    background_tasks.add_task(check_filter_task, filter_id)
    return {"id": filter_id, "message": "Фільтр створено"}


@app.post("/api/filters/{filter_id}/search")
async def search_filter_now(filter_id: int, background_tasks: BackgroundTasks):
    log(f"\n🚀 Ручний пошук для фільтра #{filter_id}\n")
    background_tasks.add_task(check_filter_task, filter_id)
    return {"message": "Пошук запущено"}


@app.delete("/api/filters/{filter_id}")
async def delete_filter(filter_id: int):
    conn = sqlite3.connect('prozorro.db')
    c = conn.cursor()
    c.execute('UPDATE filters SET is_active = 0 WHERE id = ?', (filter_id,))
    conn.commit()
    conn.close()
    log(f"\n🗑️ Видалено фільтр #{filter_id}\n")
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
            (filter_id, limit))
    else:
        c.execute(
            '''SELECT t.id, t.title, t.procuring_entity, t.amount, t.cpv, t.region,
                      t.date_published, t.url, t.filter_id, f.name
               FROM tenders t LEFT JOIN filters f ON t.filter_id = f.id
               ORDER BY t.date_published DESC LIMIT ?''',
            (limit,))
    rows = c.fetchall()
    conn.close()
    return [{"id": r[0], "title": r[1], "procuringEntity": r[2],
             "amount": r[3], "cpv": r[4], "region": r[5],
             "datePublished": r[6], "url": r[7],
             "filterId": r[8], "filterName": r[9]}
            for r in rows]


@app.delete("/api/tenders/all")
async def clear_all_tenders():
    conn = sqlite3.connect('prozorro.db')
    c = conn.cursor()
    c.execute('DELETE FROM tenders')
    c.execute('UPDATE filters SET found_count = 0')
    conn.commit()
    conn.close()
    return {"message": "Всі тендери очищено"}


@app.delete("/api/tenders/filter/{filter_id}")
async def clear_filter_tenders(filter_id: int):
    conn = sqlite3.connect('prozorro.db')
    c = conn.cursor()
    c.execute('DELETE FROM tenders WHERE filter_id = ?', (filter_id,))
    c.execute('UPDATE filters SET found_count = 0 WHERE id = ?', (filter_id,))
    conn.commit()
    conn.close()
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
    log(f"⏳ Фільтр #{filter_id}: очікую черги...")
    async with _search_lock:
        log(f"🔒 Фільтр #{filter_id}: починаю")
        try:
            conn = sqlite3.connect('prozorro.db')
            c = conn.cursor()
            c.execute(
                '''SELECT keywords, cpv, region, procuring_entity, supplier,
                          min_amount, max_amount, period_days
                   FROM filters WHERE id = ? AND is_active = 1''',
                (filter_id,))
            fd = c.fetchone()
            conn.close()

            if not fd:
                log(f"⚠️ Фільтр #{filter_id} не знайдено")
                return

            saved = await search_and_save(
                filter_id,
                fd[0], fd[1], fd[2], fd[3], fd[4], fd[5], fd[6], fd[7] or 30
            )
            log(f"🏁 Фільтр #{filter_id}: завершено, збережено {saved} тендерів")

        except Exception as e:
            log(f"❌ ПОМИЛКА check_filter_task #{filter_id}: {e}")
            log(traceback.format_exc())


app.mount("/static", StaticFiles(directory="static"), name="static")

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 10000))
    uvicorn.run(app, host="0.0.0.0", port=port)
