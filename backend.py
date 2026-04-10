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
from urllib.parse import quote

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

# DATABASE_URL — рядок підключення PostgreSQL (для Render/Neon).
# Якщо не задано — використовується локальний SQLite файл.
DATABASE_URL = os.getenv("DATABASE_URL", "")
DB_PATH = os.getenv("DB_PATH", "prozorro.db")
USE_PG = bool(DATABASE_URL and psycopg2)

# Плейсхолдер параметрів: %s для PostgreSQL, ? для SQLite
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


# ── Простий стемер для українських слів ─────────────────────────────────────
# Дозволяє знаходити "ноутбук" при запиті "ноутбуки" і навпаки.
_UK_SUFFIXES = tuple(sorted([
    'ськими', 'зьких', 'зьким', 'ськими', 'ському', 'ській',
    'ними', 'ому', 'ами', 'ями', 'ків', 'ань', 'ими',
    'ого', 'ій', 'им', 'ої', 'ів', 'ях', 'ям', 'ою', 'ею', 'ую',
    'ам', 'ах', 'ий', 'их', 'їх',
    'и', 'а', 'у', 'і', 'е', 'є', 'ю', 'я', 'ь',
], key=len, reverse=True))


def uk_stem(word: str) -> str:
    """Видаляє типове українське закінчення → нормалізований корінь."""
    for suffix in _UK_SUFFIXES:
        if word.endswith(suffix) and len(word) - len(suffix) >= 3:
            return word[:-len(suffix)]
    return word


def make_kw_list(keywords: Optional[str]):
    if not keywords:
        return []
    result = []
    for k in keywords.split(","):
        k = k.strip().lower()
        if not k:
            continue
        result.append(k)
        stem = uk_stem(k)
        if stem != k:          # додаємо корінь для відмінкових форм
            result.append(stem)
    return list(dict.fromkeys(result))  # унікальні, зберігаємо порядок


def kw_in_text(kw: str, text: str) -> bool:
    """Перевірка з морфологічною толерантністю.
    'ноутбуки' знайде 'ноутбук', 'комп'ютерів' знайде 'комп'ютер' і т.д.
    Відрізає до 3 символів з кінця, залишаючи мінімум 4 символи стему.
    """
    if kw in text:
        return True
    for trim in range(1, 4):
        if len(kw) - trim >= 4:
            if kw[:-trim] in text:
                return True
    return False


# ── Таблиця відповідності: область ↔ місто ───────────────────────────────────
# Prozorro зберігає тендери міста Харків як region="місто Харків",
# а тендери Харківської ОБЛАСТІ як region="Харківська область".
# Ця таблиця дозволяє фільтру "Харківська область" знаходити обидва типи.

OBLAST_ALIASES: dict[str, list[str]] = {
    "харківська":       ["харків",            "місто харків"],
    "київська":         ["київ",              "місто київ"],
    "дніпропетровська": ["дніпро",            "місто дніпро"],
    "одеська":          ["одеса",             "місто одеса"],
    "запорізька":       ["запоріжжя",         "місто запоріжжя"],
    "львівська":        ["львів",             "місто львів"],
    "донецька":         ["донецьк",           "місто донецьк"],
    "луганська":        ["луганськ",          "місто луганськ"],
    "вінницька":        ["вінниця",           "місто вінниця"],
    "полтавська":       ["полтава",           "місто полтава"],
    "черкаська":        ["черкаси",           "місто черкаси"],
    "сумська":          ["суми",              "місто суми"],
    "миколаївська":     ["миколаїв",          "місто миколаїв"],
    "херсонська":       ["херсон",            "місто херсон"],
    "житомирська":      ["житомир",           "місто житомир"],
    "рівненська":       ["рівне",             "місто рівне"],
    "кіровоградська":   ["кропивницький",     "кіровоград"],
    "хмельницька":      ["хмельницький",      "місто хмельницький"],
    "тернопільська":    ["тернопіль",         "місто тернопіль"],
    "чернівецька":      ["чернівці",          "місто чернівці"],
    "чернігівська":     ["чернігів",          "місто чернігів"],
    "івано-франківська":["івано-франківськ",  "місто івано-франківськ"],
    "закарпатська":     ["ужгород",           "місто ужгород"],
    "волинська":        ["луцьк",             "місто луцьк"],
}

# Зворотня: місто → область (для пошуку по місту — знаходить і область)
CITY_TO_OBLAST: dict[str, str] = {}
for oblast_key, cities in OBLAST_ALIASES.items():
    for city in cities:
        CITY_TO_OBLAST[city] = oblast_key


def region_matches(searched: str, t_region: str, t_locality: str) -> bool:
    """
    Гнучкий матчинг регіону з урахуванням того що Prozorro зберігає:
      - Тендери великих міст: region="місто Харків"
      - Тендери решти області: region="Харківська область"

    Фільтр "Харківська область" знаходить обидва типи.
    Фільтр "Харків" (місто) теж знаходить обидва типи.
    """
    s  = searched.lower().strip()
    tr = t_region.lower().strip()
    tl = t_locality.lower().strip()
    full_address = f"{tr} {tl}".strip()

    # 1. Пряме входження
    if s in full_address:
        return True

    # Нормалізована версія (без "область", "місто", "м.")
    def norm(r: str) -> str:
        return (r.replace(" область", "").replace("область", "")
                  .replace("місто ", "").replace("м. ", "")
                  .replace("місто", "").strip())

    sn       = norm(s)
    full_n   = norm(full_address)

    # 2. Нормалізоване входження
    if sn and sn in full_n:
        return True
    if norm(tr) and norm(tr) in sn:
        return True

    # 3. Oblast → City aliases (головний виправлений баг)
    #    "Харківська область" → шукаємо "харків" у повній адресі
    for oblast_key, city_list in OBLAST_ALIASES.items():
        if oblast_key in sn:                    # шукаємо по "харківська"
            for city in city_list:
                if city in full_address or city in full_n:
                    return True
            break

    # 4. City → Oblast (зворотній)
    #    "Харків" → перевіряємо чи адреса містить "харківська"
    for city_key, oblast_key in CITY_TO_OBLAST.items():
        if city_key == sn or sn in city_key:
            if oblast_key in full_n or oblast_key in norm(tr):
                return True

    return False


def get_tender_date(dd: dict) -> str:
    """Повертає найкращу дату публікації тендера з кількох можливих полів API."""
    return (dd.get("datePublished")
            or dd.get("date")
            or dd.get("tenderPeriod", {}).get("startDate")
            or dd.get("enquiryPeriod", {}).get("startDate")
            or "")


def save_tender_now(filter_id: int, tender: dict) -> bool:
    """Зберігає тендер в БД одразу. Повертає True якщо новий."""
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute(f'SELECT id FROM tenders WHERE id = {P}', (tender["id"],))
        if c.fetchone():
            conn.close()
            return False
        c.execute(
            f'''INSERT INTO tenders
               (id, title, procuring_entity, amount, cpv, region,
                date_published, url, filter_id)
               VALUES ({P},{P},{P},{P},{P},{P},{P},{P},{P})
               ON CONFLICT (id) DO NOTHING''',
            (tender["id"], tender["title"], tender["procuringEntity"],
             tender["amount"], tender["cpv"], tender["region"],
             tender["datePublished"], tender["url"], filter_id)
        )
        saved = c.rowcount > 0
        if saved:
            c.execute(
                f'UPDATE filters SET found_count = found_count + 1 WHERE id = {P}',
                (filter_id,)
            )
        conn.commit()
        conn.close()
        return saved
    except Exception as e:
        log(f"   ⚠️ Помилка збереження: {e}")
        return False


async def search_and_save(filter_id: int, keywords=None, cpv=None, region=None,
                          procuring_entity=None, supplier=None,
                          min_amount=None, max_amount=None, period_days=30):
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
    region_rejects = 0
    offset = None

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:

            for page_num in range(20):

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
                    log(f"   ⚠️ Помилка стор.{page_num+1}: {e}")
                    break

                body       = resp.json()
                page_items = body.get("data", [])
                if not page_items:
                    log(f"   ⏹ Порожня відповідь")
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

                    if item_date and item_date < date_from:
                        log(f"   ⏹ Межа {period_days} днів "
                            f"({item_date[:10]} < {date_from[:10]})")
                        reached_boundary = True
                        break

                    # Pre-filter по title (з морфологічною толерантністю)
                    if list_title and kw_list:
                        lt_lower = list_title.lower()
                        if not any(kw_in_text(kw, lt_lower) for kw in kw_list):
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
                        addr       = dd.get("procuringEntity", {}).get("address", {})
                        date_pub   = get_tender_date(dd)
                        tender = {
                            "id":              tender_id,
                            "title":           dd.get("title", "Без назви"),
                            "procuringEntity": dd.get("procuringEntity", {}).get("name", "Невідомо"),
                            "amount":          dd.get("value", {}).get("amount", 0),
                            "cpv":             items_list[0].get("classification", {}).get("id", "") if items_list else "",
                            "region":          addr.get("region", ""),
                            "datePublished":   date_pub,
                            "url":             f"https://prozorro.gov.ua/tender/{tender_id}",
                        }
                        if save_tender_now(filter_id, tender):
                            total_saved += 1
                            log(f"   ✅ #{total_saved} збережено: {tender['title'][:65]}")
                            log(f"      Регіон={addr.get('region','')} | Дата={date_pub[:10] if date_pub else '?'}")
                        else:
                            log(f"   ⏭ вже є в БД: {tender_id[:20]}...")

                        if total_saved >= 10:
                            log(f"   ⏹ Знайдено максимум (10)")
                            break

                    else:
                        # Детальне логування регіон-відмов — всі, без ліміту
                        if region and "Регіон" in result["reason"]:
                            region_rejects += 1
                            addr    = dd.get("procuringEntity", {}).get("address", {})
                            t_reg   = addr.get("region",   "—")
                            t_loc   = addr.get("locality", "—")
                            log(f"   🗺 Регіон-відмова #{region_rejects}: "
                                f"шукали='{region}' | API: region='{t_reg}', locality='{t_loc}'")

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

    log(f"   📊 Переглянуто: {total_listed} | Detail-запитів: {total_details} "
        f"| Регіон-відмов: {region_rejects}")
    log(f"   📈 Збережено: {total_saved} нових тендерів\n")
    return total_saved


def check_filter_detailed(tender, keywords, cpv, region, procuring_entity,
                           supplier, min_amount, max_amount):
    if keywords:
        text    = (tender.get("title", "") + " " + tender.get("description", "")).lower()
        kw_list = make_kw_list(keywords)
        if not any(kw_in_text(kw, text) for kw in kw_list):
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
        if not region_matches(region, t_region, t_local):
            return {"matches": False,
                    "reason": f"Регіон: шукаємо '{region}', є '{t_region} / {t_local}'"}

    if procuring_entity:
        entity = tender.get("procuringEntity", {}).get("name", "").lower()
        # Пошук по кожному слову окремо (Prozorro зберігає назви у різних форматах:
        # великі літери, різний порядок слів, скорочення тощо).
        # Слова коротші за 3 символи ігноруємо (прийменники, артиклі).
        search_words = [w for w in procuring_entity.lower().split() if len(w) >= 3]
        if search_words and not all(kw_in_text(w, entity) for w in search_words):
            return {"matches": False, "reason": f"Замовник не співпадає: шукали '{procuring_entity}', є '{entity[:80]}'"}
        # Також перевіряємо identifier (ЄДРПОУ) якщо введено тільки цифри
        if procuring_entity.strip().isdigit():
            edrpou = tender.get("procuringEntity", {}).get("identifier", {}).get("id", "")
            if procuring_entity.strip() not in edrpou:
                return {"matches": False, "reason": f"ЄДРПОУ не співпадає"}

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


def _build_tender_response(tender_id: str, data: dict, prozorro_url: str, limited: bool = False) -> dict:
    # Безпечне розпакування — Prozorro API може повертати null для деяких полів
    items = data.get("items") or []
    pe    = data.get("procuringEntity") or {}
    addr  = pe.get("address") or {}
    value = data.get("value") or {}
    clf   = (items[0].get("classification") or {}) if items else {}
    title  = data.get("title", "")
    status = data.get("status", "")
    log(f"   _build: title='{title[:60]}' status='{status}' pe='{pe.get('name','')[:40]}'")
    return {
        "id":              tender_id,
        "limited":         limited,
        "newTender":       False,
        "title":           title,
        "procuringEntity": pe.get("name", ""),
        "amount":          value.get("amount", 0),
        "currency":        value.get("currency", "UAH"),
        "status":          status,
        "datePublished":   get_tender_date(data),
        "region":          addr.get("region", ""),
        "locality":        addr.get("locality", ""),
        "cpv":             clf.get("id", ""),
        "cpvDescription":  clf.get("description", ""),
        "url":             prozorro_url,
    }


@app.get("/api/tender/{tender_id}")
async def get_tender_by_id(tender_id: str):
    import re
    log(f"\n🔍 ПОШУК ТЕНДЕРА: {tender_id}")
    prozorro_url = f"https://prozorro.gov.ua/tender/{tender_id}"
    
    # Базовый fallback - минимальная информация
    fallback = {
        "id": tender_id, 
        "limited": True, 
        "newTender": True,  # Предполагаем, что это новый тендер
        "title": "", 
        "procuringEntity": "", 
        "amount": 0,
        "currency": "UAH", 
        "status": "unknown",
        "datePublished": "", 
        "region": "", 
        "locality": "",
        "cpv": "", 
        "cpvDescription": "", 
        "url": prozorro_url,
        "message": "Тендер не знайдено в API (можливо занадто новий або архівний). Перейдіть на сайт Prozorro для перегляду."
    }
    
    is_ua_id = bool(re.match(r'^UA-\d{4}-\d{2}-\d{2}-', tender_id))

    # ── Крок 0: перевіряємо локальний кеш (БД) ───────────────────────────────
    try:
        conn = get_conn()
        c = conn.cursor()
        c.execute(
            '''SELECT id, title, procuring_entity, amount, cpv, region,
               date_published, url
               FROM tenders WHERE id = ?''',
            (tender_id,)
        )
        row = c.fetchone()
        conn.close()
        if row:
            log(f"   ✅ Знайдено в локальній БД")
            return {
                "id": row[0], "limited": False, "newTender": False,
                "title": row[1] or "", "procuringEntity": row[2] or "",
                "amount": row[3] or 0, "currency": "UAH",
                "status": "active",
                "datePublished": row[6] or "", "region": row[5] or "",
                "locality": "", "cpv": row[4] or "", "cpvDescription": "",
                "url": row[7] or prozorro_url,
            }
    except Exception as e:
        log(f"   DB check error: {e}")

    async def fetch_detail(client, hex_id: str, timeout: float = 10.0) -> dict:
        """Повертає повний об'єкт тендера за внутрішнім hex UUID або {}."""
        try:
            r = await client.get(f"{PROZORRO_API}/tenders/{hex_id}", timeout=timeout)
            if r.status_code == 200:
                return r.json().get("data") or {}
            elif r.status_code == 404:
                log(f"  → 404 для {hex_id[:20]}...")
            else:
                log(f"  → статус {r.status_code} для {hex_id[:20]}...")
        except httpx.TimeoutException:
            log(f"  → timeout для {hex_id[:20]}...")
        except Exception as e:
            log(f"  → error: {e}")
        return {}

    try:
        # Увеличиваем таймаут для всего клиента
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:

            # ── Крок 1: прямий запит для внутрішніх hex UUID ─────────────────
            if not is_ua_id:
                log(f"   Крок 1 (hex direct)...")
                data = await fetch_detail(client, tender_id, timeout=15.0)
                if data:
                    log(f"   ✅ Знайдено за hex UUID")
                    return _build_tender_response(tender_id, data, prozorro_url)
                # Для hex ID нет смысла пробовать дальше - это уже прямой запрос
                return fallback

            # ── Крок 2: «Пошуковий міст» — офіційний індекс tenderID ─────────
            # Пробуем несколько раз с увеличенным таймаутом
            for attempt in range(3):
                try:
                    log(f"   Крок 2 (tenderID index), спроба {attempt+1}...")
                    search_timeout = 10.0 + attempt * 5.0  # 10, 15, 20 сек
                    
                    r_list = await client.get(
                        f"{PROZORRO_API}/tenders",
                        params={
                            "tenderID": tender_id, 
                            "opt_fields": "id,tenderID,dateModified", 
                            "limit": 20
                        },
                        timeout=search_timeout,
                    )
                    
                    log(f"   → статус {r_list.status_code}")
                    
                    if r_list.status_code == 200:
                        body = r_list.json()
                        candidates = body.get("data", [])
                        log(f"   Кандидатів у списку: {len(candidates)}")
                        
                        # Проверяем каждого кандидата
                        for item in candidates:
                            hex_id = item.get("id", "")
                            item_tender_id = item.get("tenderID", "")
                            
                            if not hex_id:
                                continue
                                
                            # Если в списке уже есть tenderID и он совпадает - отлично
                            if item_tender_id == tender_id:
                                log(f"   ✅ Знайдено в списку: {hex_id}")
                                data = await fetch_detail(client, hex_id, timeout=15.0)
                                if data:
                                    return _build_tender_response(tender_id, data, prozorro_url)
                            
                            # Иначе получаем детали и проверяем
                            data = await fetch_detail(client, hex_id, timeout=10.0)
                            if not data:
                                continue
                                
                            actual_tid = data.get("tenderID", "")
                            if actual_tid == tender_id:
                                log(f"   ✅ tenderID збігається! UUID={hex_id}")
                                # Сохраняем в кеш
                                try:
                                    items_list = data.get("items", [])
                                    addr = (data.get("procuringEntity") or {}).get("address") or {}
                                    date_pub = get_tender_date(data)
                                    save_tender_now(0, {
                                        "id": tender_id,
                                        "title": data.get("title", ""),
                                        "procuringEntity": (data.get("procuringEntity") or {}).get("name", ""),
                                        "amount": (data.get("value") or {}).get("amount", 0),
                                        "cpv": (items_list[0].get("classification") or {}).get("id", "") if items_list else "",
                                        "region": addr.get("region", ""),
                                        "datePublished": date_pub,
                                        "url": prozorro_url,
                                    })
                                    log(f"   💾 Збережено в кеш БД")
                                except Exception as e:
                                    log(f"   Кеш error (non-fatal): {e}")
                                return _build_tender_response(tender_id, data, prozorro_url)
                            else:
                                log(f"   ❌ Mismatch: API={actual_tid!r} ≠ {tender_id!r}")
                        
                        # Если кандидаты есть, но ни один не подошел - это странно, 
                        # но продолжим следующую попытку
                        if candidates:
                            log(f"   ⚠️ {len(candidates)} кандидатів, але жоден не підходить")
                        else:
                            log(f"   ⚠️ Порожній список кандидатів (тендер ще не проіндексовано?)")
                            
                    elif r_list.status_code == 404:
                        log(f"   → індекс повернув 404")
                    else:
                        log(f"   → невідома відповідь: {r_list.text[:100]}")
                        
                except httpx.TimeoutException:
                    log(f"   → timeout на спробі {attempt+1}")
                except Exception as e:
                    log(f"   → помилка на спробі {attempt+1}: {e}")
                
                # Небольшая задержка перед следующей попыткой
                if attempt < 2:
                    await asyncio.sleep(1.0)

            # ── Крок 3: Пошук через сканування останніх тендерів ─────────────
            # Иногда API не индексирует tenderID сразу, но тендер уже в списке
            log(f"   Крок 3 (сканування останніх 200 тендерів)...")
            
            try:
                # Получаем последние 200 тендеров
                r_recent = await client.get(
                    f"{PROZORRO_API}/tenders",
                    params={
                        "descending": "1",
                        "limit": "200",
                        "opt_fields": "id,tenderID,title,status,dateModified"
                    },
                    timeout=20.0,
                )
                
                if r_recent.status_code == 200:
                    recent_data = r_recent.json().get("data", [])
                    log(f"   Отримано {len(recent_data)} недавніх тендерів")
                    
                    for item in recent_data:
                        item_tid = item.get("tenderID", "")
                        if item_tid == tender_id:
                            hex_id = item.get("id", "")
                            log(f"   ✅ Знайдено в недавніх: {hex_id}")
                            
                            # Получаем полные детали
                            data = await fetch_detail(client, hex_id, timeout=15.0)
                            if data:
                                return _build_tender_response(tender_id, data, prozorro_url)
                            else:
                                # Если не получили детали, возвращаем хотя бы базовую инфу из списка
                                log(f"   ⚠️ Немає деталей, повертаємо базову інформацію")
                                return {
                                    "id": tender_id,
                                    "limited": True,
                                    "newTender": False,
                                    "title": item.get("title", "Невідомо"),
                                    "procuringEntity": "",
                                    "amount": 0,
                                    "currency": "UAH",
                                    "status": item.get("status", "unknown"),
                                    "datePublished": item.get("dateModified", ""),
                                    "region": "",
                                    "locality": "",
                                    "cpv": "",
                                    "cpvDescription": "",
                                    "url": prozorro_url,
                                    "message": "Знайдено в списку, але деталі недоступні через API"
                                }
                    
                    log(f"   ❌ Не знайдено в недавніх тендерах")
                    
            except Exception as e:
                log(f"   → помилка сканування: {e}")

            # ── Крок 4: Прямий запит з форматом UA-XXXX ─────────────────────
            # Иногда API принимает UA-ID как hex (странно, но пробуем)
            log(f"   Крок 4 (прямий запит з UA-ID)...")
            try:
                # Кодируем tender_id для URL
                encoded_id = quote(tender_id, safe='')
                r_direct = await client.get(
                    f"{PROZORRO_API}/tenders/{encoded_id}",
                    timeout=15.0,
                )
                if r_direct.status_code == 200:
                    data = r_direct.json().get("data")
                    if data:
                        log(f"   ✅ Прямий запит успішний!")
                        return _build_tender_response(tender_id, data, prozorro_url)
            except Exception as e:
                log(f"   → не вдалося: {e}")

            # ── Fallback ──────────────────────────────────────────────────────
            log(f"   ⚠️ Всі методи вичерпано — повертаємо fallback")
            return fallback

    except Exception as e:
        log(f"❌ EXCEPTION get_tender: {type(e).__name__}: {e}")
        log(traceback.format_exc())
        return fallback


@app.get("/api/filters")
async def get_filters():
    conn = get_conn()
    c = conn.cursor()
    c.execute('''SELECT id, name, keywords, cpv, region, procuring_entity, supplier,
                 min_amount, max_amount, period_days, is_active, found_count
                 FROM filters WHERE is_active = TRUE ORDER BY id''')
    rows = c.fetchall()
    conn.close()
    return [{"id": r[0], "name": r[1], "keywords": r[2], "cpv": r[3],
             "region": r[4], "procuringEntity": r[5], "supplier": r[6],
             "minAmount": r[7], "maxAmount": r[8], "periodDays": r[9],
             "isActive": bool(r[10]), "foundCount": r[11]}
            for r in rows]


@app.put("/api/filters/{filter_id}")
async def update_filter(filter_id: int, filter_data: FilterCreate, background_tasks: BackgroundTasks):
    conn = get_conn()
    c = conn.cursor()
    c.execute(
        f'''UPDATE filters SET name={P}, keywords={P}, cpv={P}, region={P}, procuring_entity={P},
           supplier={P}, min_amount={P}, max_amount={P}, period_days={P}
           WHERE id={P} AND is_active = TRUE''',
        (filter_data.name, filter_data.keywords, filter_data.cpv,
         filter_data.region, filter_data.procuringEntity, filter_data.supplier,
         filter_data.minAmount, filter_data.maxAmount, filter_data.periodDays,
         filter_id)
    )
    conn.commit()
    conn.close()
    log(f"\n✏️ Оновлено фільтр #{filter_id}: {filter_data.name}\n")
    background_tasks.add_task(check_filter_task, filter_id)
    return {"id": filter_id, "message": "Фільтр оновлено"}


@app.post("/api/filters")
async def create_filter(filter_data: FilterCreate, background_tasks: BackgroundTasks):
    conn = get_conn()
    c = conn.cursor()
    if USE_PG:
        c.execute(
            f'''INSERT INTO filters (name, keywords, cpv, region, procuring_entity, supplier,
                                    min_amount, max_amount, period_days)
               VALUES ({P},{P},{P},{P},{P},{P},{P},{P},{P}) RETURNING id''',
            (filter_data.name, filter_data.keywords, filter_data.cpv,
             filter_data.region, filter_data.procuringEntity, filter_data.supplier,
             filter_data.minAmount, filter_data.maxAmount, filter_data.periodDays)
        )
        filter_id = c.fetchone()[0]
    else:
        c.execute(
            f'''INSERT INTO filters (name, keywords, cpv, region, procuring_entity, supplier,
                                    min_amount, max_amount, period_days)
               VALUES ({P},{P},{P},{P},{P},{P},{P},{P},{P})''',
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
    conn = get_conn()
    c = conn.cursor()
    c.execute(f'UPDATE filters SET is_active = FALSE WHERE id = {P}', (filter_id,))
    conn.commit()
    conn.close()
    log(f"\n🗑️ Видалено фільтр #{filter_id}\n")
    return {"message": "Фільтр видалено"}


@app.get("/api/tenders")
async def get_tenders(limit: int = 200, filter_id: Optional[int] = None):
    conn = get_conn()
    c = conn.cursor()
    if filter_id:
        c.execute(
            f'''SELECT t.id, t.title, t.procuring_entity, t.amount, t.cpv, t.region,
                      t.date_published, t.url, t.filter_id, f.name
               FROM tenders t LEFT JOIN filters f ON t.filter_id = f.id
               WHERE t.filter_id = {P} ORDER BY t.date_published DESC LIMIT {P}''',
            (filter_id, limit))
    else:
        c.execute(
            f'''SELECT t.id, t.title, t.procuring_entity, t.amount, t.cpv, t.region,
                      t.date_published, t.url, t.filter_id, f.name
               FROM tenders t LEFT JOIN filters f ON t.filter_id = f.id
               ORDER BY t.date_published DESC LIMIT {P}''',
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
    conn = get_conn()
    c = conn.cursor()
    c.execute('DELETE FROM tenders')
    c.execute('UPDATE filters SET found_count = 0')
    conn.commit()
    conn.close()
    return {"message": "Всі тендери очищено"}


@app.delete("/api/tenders/filter/{filter_id}")
async def clear_filter_tenders(filter_id: int):
    conn = get_conn()
    c = conn.cursor()
    c.execute(f'DELETE FROM tenders WHERE filter_id = {P}', (filter_id,))
    c.execute(f'UPDATE filters SET found_count = 0 WHERE id = {P}', (filter_id,))
    conn.commit()
    conn.close()
    return {"message": f"Тендери фільтра #{filter_id} очищено"}


@app.get("/api/stats")
async def get_stats():
    conn = get_conn()
    c = conn.cursor()
    c.execute('SELECT COUNT(*) FROM tenders')
    total = c.fetchone()[0]
    today = datetime.now().date().isoformat()
    c.execute(f'SELECT COUNT(*) FROM tenders WHERE DATE(date_published) = {P}', (today,))
    today_count = c.fetchone()[0]
    c.execute('SELECT COUNT(*) FROM filters WHERE is_active = TRUE')
    active_filters = c.fetchone()[0]
    conn.close()
    return {"total": total, "today": today_count, "active": active_filters}


@app.get("/api/filters/{filter_id}/searching")
async def is_filter_searching(filter_id: int):
    return {"searching": filter_id in _active_searches or filter_id in _queued_searches}


async def check_filter_task(filter_id: int):
    _queued_searches.add(filter_id)
    log(f"⏳ Фільтр #{filter_id}: очікую черги...")
    async with _search_lock:
        _queued_searches.discard(filter_id)
        _active_searches.add(filter_id)
        log(f"🔒 Фільтр #{filter_id}: починаю")
        try:
            conn = get_conn()
            c = conn.cursor()
            c.execute(
                f'''SELECT keywords, cpv, region, procuring_entity, supplier,
                          min_amount, max_amount, period_days
                   FROM filters WHERE id = {P} AND is_active = TRUE''',
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
        finally:
            _active_searches.discard(filter_id)


app.mount("/static", StaticFiles(directory="static"), name="static")

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 10000))
    uvicorn.run(app, host="0.0.0.0", port=port)



