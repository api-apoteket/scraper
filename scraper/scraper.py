#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PostgreSQL-based multi-site scraper - Production version
with proxy support, retry/backoff and periodic flush
"""

import asyncio
import csv
import json
import datetime
import os
import re
import logging
import sys
import random
import signal
from io import StringIO
from urllib.parse import urljoin
from playwright.async_api import async_playwright, Error as PlaywrightError
from playwright_stealth import Stealth
from flask import Flask, Response, request, jsonify
import threading
import psycopg2
import psycopg2.extras
from psycopg2.pool import ThreadedConnectionPool

app = Flask(__name__)

# === Configuration ===
LOG_DIR = "/logs"

SETTINGS_META = {
    'concurrent_pages': {
        'label': 'Concurrent pages', 'type': 'int', 'default': 2, 'unit': 'pages', 'min': 1, 'max': 10,
        'description': 'Number of pages scraped simultaneously.',
        'why': 'Increase for faster scraping; decrease if sites block requests or memory is low.',
    },
    'headless': {
        'label': 'Headless browser', 'type': 'bool', 'default': True,
        'description': 'Run the browser without a visible window.',
        'why': 'Disable only for debugging — requires a display, not suitable for production.',
    },
    'scrape_interval': {
        'label': 'Scrape interval', 'type': 'int', 'default': 3600, 'unit': 's', 'min': 300,
        'description': 'Seconds between full scraping runs.',
        'why': 'Lower for fresher prices; higher to reduce server load and avoid rate-limiting.',
    },
    'proxy_url': {
        'label': 'Proxy URL', 'type': 'str', 'default': '',
        'placeholder': 'socks5://user:pass@host:1080',
        'description': 'SOCKS5 or HTTP proxy for all scraping requests.',
        'why': 'Use if your IP is blocked by a site.',
    },
    'check_interval': {
        'label': 'Alert check interval', 'type': 'int', 'default': 1800, 'unit': 's', 'min': 60,
        'description': 'Seconds between price-drop checks.',
        'why': 'Lower for faster alerts; higher to reduce database load.',
    },
    'min_drop_percent': {
        'label': 'Minimum drop (%)', 'type': 'float', 'default': 5.0, 'unit': '%', 'min': 0.1,
        'description': 'Smallest percentage price drop that triggers an alert.',
        'why': 'Lower to catch small deals; raise to reduce noise.',
    },
    'min_drop_amount': {
        'label': 'Minimum drop (kr)', 'type': 'int', 'default': 100, 'unit': 'kr', 'min': 1,
        'description': 'Smallest absolute price drop in kr that triggers an alert.',
        'why': 'Prevents alerts on cheap items with trivial drops. Raise to focus on expensive products.',
    },
    'cooldown_hours': {
        'label': 'Alert cooldown', 'type': 'int', 'default': 24, 'unit': 'h', 'min': 1,
        'description': 'Hours before the same product can trigger another alert.',
        'why': 'Prevents repeated alerts when a price stays low. Lower if you want every change notified.',
    },
}

os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(f"{LOG_DIR}/scraper.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

BROWSER_ARGS = [
    '--no-sandbox',
    '--disable-dev-shm-usage',
    '--disable-http2',
    '--ignore-certificate-errors',
    '--disable-blink-features=AutomationControlled',
    '--disable-web-security',
    '--disable-zygote',
]

stats = {"products": 0, "updated": 0, "skipped": 0, "errors": 0, "retries": 0}
shutdown_event = asyncio.Event()
scraping_active = False
write_buffer = []
write_lock = asyncio.Lock()


CREDENTIALS_DIR = os.getenv('CREDENTIALS_DIR', '/credentials')


def read_secret(env_var, default=""):
    path = os.getenv(f"{env_var}_FILE")
    if path and os.path.exists(path):
        with open(path) as f:
            return f.read().strip()
    return os.getenv(env_var, default)


def read_credential(name, default=""):
    path = os.path.join(CREDENTIALS_DIR, name)
    if os.path.exists(path):
        with open(path) as f:
            return f.read().strip()
    return default


def write_credential(name, value):
    os.makedirs(CREDENTIALS_DIR, exist_ok=True)
    with open(os.path.join(CREDENTIALS_DIR, name), 'w') as f:
        f.write(value)


def get_db_user():
    return read_credential('db_user') or os.getenv('DB_USER', 'scraper')


def init_credentials():
    import secrets as _secrets
    api_key_path = os.path.join(CREDENTIALS_DIR, 'api_key')
    if not os.path.exists(api_key_path):
        key = _secrets.token_urlsafe(32)
        write_credential('api_key', key)
        logger.info("=" * 50)
        logger.info("  GENERATED API KEY: %s", key)
        logger.info("  Save this — it is required to access the API")
        logger.info("=" * 50)


db_pool = None


def init_db_pool():
    global db_pool
    db_password = read_secret("DB_PASSWORD")
    db_pool = ThreadedConnectionPool(
        minconn=1, maxconn=10,
        host=os.getenv("DB_HOST", "postgres"),
        database=os.getenv("DB_NAME", "scraper"),
        user=get_db_user(),
        password=db_password,
        connect_timeout=10
    )
    logger.info("Database connection pool initialized")


def reinit_db_pool():
    global db_pool
    old = db_pool
    db_pool = None
    if old:
        try:
            old.closeall()
        except psycopg2.Error as e:
            logger.warning(f"Error closing old db pool: {e}")
    init_db_pool()


def get_setting(key):
    meta = SETTINGS_META.get(key, {})
    default = meta.get('default', '')
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT value FROM settings WHERE key = %s", (key,))
        row = cur.fetchone()
        raw = row[0] if row else None
    finally:
        return_db(conn)
    if raw is None:
        return default
    t = meta.get('type', 'str')
    if t == 'int':
        return int(raw)
    if t == 'float':
        return float(raw)
    if t == 'bool':
        return raw.lower() in ('true', '1', 'yes')
    return raw



def get_db():
    """Get connection from pool"""
    return db_pool.getconn()


def return_db(conn):
    """Return connection to pool"""
    db_pool.putconn(conn)


def init_db():
    """Initialize PostgreSQL database"""
    conn = get_db()
    cur = conn.cursor()
    
    cur.execute("""
    CREATE TABLE IF NOT EXISTS products (
        id SERIAL PRIMARY KEY,
        url TEXT UNIQUE,
        title TEXT,
        current_price INTEGER,
        first_seen TIMESTAMP DEFAULT NOW(),
        last_updated TIMESTAMP DEFAULT NOW(),
        site_config_id INTEGER
    )
    """)
    
    cur.execute("""
    CREATE TABLE IF NOT EXISTS price_history (
        id SERIAL PRIMARY KEY,
        product_id INTEGER REFERENCES products(id) ON DELETE CASCADE,
        price INTEGER,
        timestamp TIMESTAMP DEFAULT NOW()
    )
    """)
    
    cur.execute("""
    CREATE TABLE IF NOT EXISTS scraper_config (
        id SERIAL PRIMARY KEY,
        name TEXT UNIQUE NOT NULL,
        base_url TEXT NOT NULL,
        product_selector TEXT NOT NULL,
        title_selector TEXT NOT NULL,
        price_selector TEXT NOT NULL,
        link_selector TEXT NOT NULL,
        pagination_type TEXT DEFAULT 'query',
        pagination_selector TEXT,
        max_pages INTEGER DEFAULT 10,
        enabled INTEGER DEFAULT 1,
        min_price INTEGER DEFAULT 0,
        max_price INTEGER DEFAULT 999999,
        categories TEXT DEFAULT '[]',
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
    )
    """)
    
    cur.execute("""
    CREATE TABLE IF NOT EXISTS alert_cooldown (
        product_id INTEGER REFERENCES products(id) ON DELETE CASCADE,
        last_alert TIMESTAMP DEFAULT NOW(),
        PRIMARY KEY (product_id)
    )
    """)
    
    cur.execute("""
    CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        updated_at TIMESTAMPTZ DEFAULT NOW()
    )
    """)

    cur.execute("ALTER TABLE scraper_config ADD COLUMN IF NOT EXISTS use_stealth INTEGER DEFAULT 0")
    cur.execute("ALTER TABLE scraper_config ADD COLUMN IF NOT EXISTS proxy_url TEXT DEFAULT ''")
    cur.execute("ALTER TABLE scraper_config ADD COLUMN IF NOT EXISTS exclude_link_pattern TEXT DEFAULT ''")
    cur.execute("ALTER TABLE scraper_config ADD COLUMN IF NOT EXISTS url_scope TEXT DEFAULT ''")

    cur.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS description TEXT")
    cur.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS description_why TEXT")
    cur.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS description_updated_at TIMESTAMP")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_products_missing_description ON products(id) WHERE description IS NULL")

    cur.execute("CREATE INDEX IF NOT EXISTS idx_products_url ON products(url)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_products_last_updated ON products(last_updated)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_price_history_product ON price_history(product_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_price_history_time ON price_history(timestamp DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_price_history_product_time ON price_history(product_id, timestamp DESC)")
    
    cur.execute("SELECT COUNT(*) FROM scraper_config")
    if cur.fetchone()[0] == 0:
        cur.execute("""
        INSERT INTO scraper_config
        (name, base_url, product_selector, title_selector, price_selector, link_selector,
         pagination_type, pagination_selector, max_pages)
        VALUES
        ('Inet.se', 'https://www.inet.se/kategori/31/datorkomponenter',
         'li[data-test-id^=''search_product_'']', 'h3',
         'span[data-test-is-discounted-price]', 'a[href*=''/produkt/'']',
         'subcategory', 'a[href*=''/kategori/'']', 999)
        """)
        cur.execute("""
        INSERT INTO scraper_config 
        (name, base_url, product_selector, title_selector, price_selector, link_selector, max_pages)
        VALUES 
        ('Komplett.se', 'https://www.komplett.se/category/10000/datorkomponenter',
         'div.product', 'h2', 'span.product-price', 'a', 5)
        """)
        cur.execute("""
        INSERT INTO scraper_config 
        (name, base_url, product_selector, title_selector, price_selector, link_selector, max_pages)
        VALUES 
        ('Webhallen', 'https://www.webhallen.com/se/category/3-Datorkomponenter',
         'div.product-item', 'h2.product-title', 'span.price', 'a.product-link', 5)
        """)
        cur.execute("""
        INSERT INTO scraper_config 
        (name, base_url, product_selector, title_selector, price_selector, link_selector, max_pages)
        VALUES 
        ('Bookstore', 'https://books.toscrape.com',
         'article.product_pod', 'h3 a', 'p.price_color', 'h3 a', 50)
        """)
        logger.info("Created default configs")
    
    conn.commit()
    return_db(conn)
    logger.info("PostgreSQL database initialized")


def load_configs():
    """Load active configurations"""
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM scraper_config WHERE enabled = 1 ORDER BY name")
    columns = [desc[0] for desc in cur.description]
    configs = [dict(zip(columns, row)) for row in cur.fetchall()]
    return_db(conn)
    return configs


def extract_price(price_text, pattern=None):
    if not price_text:
        return 0
    if pattern and pattern.startswith('text=/'):
        # Use a fixed price pattern instead of user-supplied regex to prevent ReDoS
        match = re.search(r'\d[\d\s]*(?:kr|:-|\.\d{2})?', str(price_text))
        if match:
            price_text = match.group(0)
    digits = re.sub(r"[^\d]", "", str(price_text))
    return int(digits) if digits else 0


async def accept_cookies(page):
    """Accept cookie consent dialogs — tries common button texts."""
    for text in ['Jag förstår', 'Acceptera alla', 'Acceptera', 'Accept all', 'Accept']:
        try:
            btn = await page.query_selector(f"button:has-text('{text}')")
            if btn and await btn.is_visible():
                await btn.click()
                await asyncio.sleep(1.5)
                return True
        except PlaywrightError as e:
            logger.debug(f"Cookie button '{text}' not clickable: {e}")
    return False


async def extract_product(page, element, config):
    try:
        title_el = await element.query_selector(config['title_selector']) if config['title_selector'] else None
        price_el = await element.query_selector(config['price_selector']) if config['price_selector'] else None
        link_el = await element.query_selector(config['link_selector']) if config['link_selector'] else None
        
        title = (await title_el.inner_text()).strip() if title_el else ""
        if not title and config['title_selector'] == '':
            title = (await element.inner_text()).strip()
        
        price_text = (await price_el.inner_text()).strip() if price_el else ""
        if not price_text and config['price_selector'].startswith('text=/'):
            parent_text = await element.evaluate("el => el.closest('article, div')?.innerText || ''")
            # Use a fixed price pattern instead of user-supplied regex to prevent ReDoS
            match = re.search(r'\d[\d\s]*(?:kr|:-|\.\d{2})?', parent_text)
            price_text = match.group(0) if match else ""
        
        link = await link_el.get_attribute("href") if link_el else await element.get_attribute("href")
        
        if not (title and price_text and link):
            return None
        
        price = extract_price(price_text, config['price_selector'])
        if price == 0:
            return None
        
        if price < config.get('min_price', 0) or price > config.get('max_price', 999999):
            return None
        
        url = urljoin(page.url, link)

        exclude = config.get('exclude_link_pattern', '')
        if exclude and exclude in url:
            return None

        return {'url': url, 'title': title[:200], 'price': price, 'site_config_id': config['id']}
    except PlaywrightError as e:
        logger.debug(f"Extraction error: {e}")
        return None


async def scrape_page_with_retry(context, url, max_retries=3, use_stealth=False):
    """Scrape page with exponential backoff - always closes page on failure"""
    for attempt in range(max_retries):
        page = None
        try:
            page = await context.new_page()
            if use_stealth:
                await Stealth().apply_stealth_async(page)
            await page.goto(url, timeout=60000, wait_until="domcontentloaded")
            page.set_default_timeout(30000)
            await page.wait_for_timeout(random.randint(2000, 5000))
            return page
        except PlaywrightError as e:
            if page:
                await page.close()
            if attempt < max_retries - 1:
                wait_time = (2 ** attempt) * 5 + random.uniform(1, 3)
                logger.warning(f"Retry {attempt+1}/{max_retries} for {url} after {wait_time:.1f}s: {e}")
                stats['retries'] += 1
                await asyncio.sleep(wait_time)
            else:
                logger.error(f"All retries failed for {url}: {e}")
                return None

    return None


async def scrape_site(context, config, page_sem=None):
    products_found = 0
    cookies_done = [False]
    _page_sem = page_sem if page_sem is not None else asyncio.Semaphore(999)

    logger.info(f"Starting: {config['name']}")

    async def _cookies_once(page):
        if not cookies_done[0]:
            await accept_cookies(page)
            cookies_done[0] = True

    async def _infinite_scroll(page, rounds=30):
        sel_js = json.dumps(config['product_selector'])
        prev = 0
        for _ in range(rounds):
            try:
                await asyncio.wait_for(
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)"),
                    timeout=10
                )
                await asyncio.sleep(random.uniform(1.5, 2.5))
                count = await asyncio.wait_for(
                    page.evaluate(f"document.querySelectorAll({sel_js}).length"),
                    timeout=15
                )
            except asyncio.TimeoutError:
                logger.warning("evaluate() timed out during scroll, stopping early")
                break
            if count == prev:
                break
            prev = count

    start_urls = [u.strip().rstrip('/') for u in config['base_url'].splitlines() if u.strip()]

    if config.get('pagination_type') == 'subcategory':
        visited = set()
        queued = set(start_urls)
        queue = list(start_urls)
        seen_product_urls = set()
        url_scope = (config.get('url_scope') or '').strip()
        max_pages = config.get('max_pages', 10)

        while queue and not shutdown_event.is_set():
            base_url = queue.pop(0)
            if base_url in visited:
                continue
            visited.add(base_url)

            for page_num in range(1, max_pages + 1):
                if shutdown_event.is_set():
                    break
                if page_num == 1:
                    url = base_url
                elif '?' in base_url:
                    url = f"{base_url}&page={page_num}"
                else:
                    url = f"{base_url}?page={page_num}"

                async with _page_sem:
                    label = base_url.split('/')[-1]
                    logger.info(f"  Category: {label}" + (f" (p{page_num})" if page_num > 1 else ""))
                    page = await scrape_page_with_retry(context, url, use_stealth=config.get('use_stealth', 0))
                    if not page:
                        break

                    page_new = 0
                    try:
                        await _cookies_once(page)

                        if page_num == 1 and config.get('pagination_selector'):
                            try:
                                links = await page.eval_on_selector_all(
                                    config['pagination_selector'],
                                    "els => [...new Set(els.map(e => e.href))]"
                                )
                                for link in links:
                                    link = link.rstrip('/')
                                    if url_scope and url_scope not in link:
                                        continue
                                    if link not in visited and link not in queued:
                                        queued.add(link)
                                        queue.append(link)
                            except PlaywrightError as e:
                                logger.debug(f"Pagination selector failed: {e}")

                        await _infinite_scroll(page)
                        elements = await page.query_selector_all(config['product_selector'])
                        logger.info(f"  {len(elements)} elements after scroll")

                        for elem in elements:
                            product = await extract_product(page, elem, config)
                            if product and product['url'] not in seen_product_urls:
                                seen_product_urls.add(product['url'])
                                async with write_lock:
                                    write_buffer.append((product, False))
                                    if len(write_buffer) >= 10:
                                        await flush_buffer()
                                products_found += 1
                                page_new += 1

                        logger.info(f"  → {page_new} new (total: {products_found})")

                    except PlaywrightError as e:
                        logger.error(f"Error scraping {url}: {e}")
                        stats['errors'] += 1
                        break
                    finally:
                        await page.close()

                if page_new == 0:
                    break

                await asyncio.sleep(random.uniform(2, 4))

        logger.info(f"Done with {config['name']}: {products_found} products")

    else:
        known_urls = set()
        page = None

        try:
            max_pages = config.get('max_pages', 10)

            for base_url in start_urls:
                if shutdown_event.is_set():
                    break
                page_num = 1
                url = base_url

                while url and page_num <= max_pages and not shutdown_event.is_set():
                    async with _page_sem:
                        logger.info(f"  Page {page_num}/{max_pages}: {url}")
                        page = await scrape_page_with_retry(context, url, use_stealth=config.get('use_stealth', 0))
                        if not page:
                            break

                        try:
                            await _cookies_once(page)
                            await _infinite_scroll(page, rounds=15)
                            elements = await page.query_selector_all(config['product_selector'])
                            logger.info(f"  Found {len(elements)} elements")

                            async def _extract_query_elements():
                                count = 0
                                for elem in elements:
                                    product = await extract_product(page, elem, config)
                                    if product:
                                        was_known = product['url'] in known_urls
                                        known_urls.add(product['url'])
                                        async with write_lock:
                                            write_buffer.append((product, was_known))
                                            if len(write_buffer) >= 10:
                                                await flush_buffer()
                                        count += 1
                                return count

                            try:
                                n = await asyncio.wait_for(_extract_query_elements(), timeout=60)
                                products_found += n
                            except asyncio.TimeoutError:
                                logger.warning("  Extraction timed out after 60s, moving on")
                        finally:
                            await page.close()
                            page = None

                        if config.get('pagination_type') == 'query':
                            separator = '&' if '?' in base_url else '?'
                            url = f"{base_url}{separator}page={page_num + 1}"
                        else:
                            url = None

                    page_num += 1
                    await asyncio.sleep(random.uniform(3, 7))

            logger.info(f"Done with {config['name']}: {products_found} products")
        except PlaywrightError as e:
            logger.error(f"Error in {config['name']}: {e}")
            stats['errors'] += 1
        finally:
            if page:
                await page.close()


async def flush_buffer():
    """Save buffered products to PostgreSQL"""
    if not write_buffer:
        return
    
    buffer_copy = write_buffer.copy()
    write_buffer.clear()
    
    conn = get_db()
    cur = conn.cursor()
    now = datetime.datetime.now()
    
    known_urls_list = [p[0]['url'] for p in buffer_copy if p[1]]
    current_prices = {}
    
    if known_urls_list:
        cur.execute("SELECT url, current_price FROM products WHERE url = ANY(%s::text[])", (known_urls_list,))
        for row in cur.fetchall():
            current_prices[row[0]] = row[1]
    
    for product, was_known in buffer_copy:
        try:
            current_price = current_prices.get(product['url']) if was_known else None
            
            if current_price == product['price']:
                stats['skipped'] += 1
                continue
            
            cur.execute("""
                INSERT INTO products (url, title, current_price, site_config_id, last_updated)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (url) DO UPDATE SET
                    current_price = EXCLUDED.current_price,
                    title = EXCLUDED.title,
                    last_updated = EXCLUDED.last_updated
                RETURNING id
            """, (product['url'], product['title'], product['price'], product['site_config_id'], now))
            
            product_id = cur.fetchone()[0]
            
            cur.execute("INSERT INTO price_history (product_id, price, timestamp) VALUES (%s, %s, %s)",
                       (product_id, product['price'], now))
            
            if was_known:
                stats['updated'] += 1
            else:
                stats['products'] += 1
                
        except psycopg2.Error as e:
            logger.error(f"DB error: {e}")
            stats['errors'] += 1
    
    conn.commit()
    return_db(conn)


async def periodic_flush():
    """Flush buffer every 5 seconds"""
    while not shutdown_event.is_set():
        await asyncio.sleep(5)
        if write_buffer:
            async with write_lock:
                if write_buffer:
                    await flush_buffer()


async def run_scraper():
    """Main function"""
    configs = load_configs()
    if not configs:
        logger.warning("No active configurations")
        return
    
    flush_task = asyncio.create_task(periodic_flush())
    
    concurrent_pages = get_setting('concurrent_pages')
    headless = get_setting('headless')
    proxy_url = get_setting('proxy_url')

    global_proxy_url = proxy_url

    def _make_proxy(url):
        if not url:
            return None
        display = url.split('@')[-1] if '@' in url else url
        logger.info(f"Using proxy: {display}")
        return {"server": url}

    browser = None
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=headless, args=BROWSER_ARGS)
            sem = asyncio.Semaphore(concurrent_pages)

            async def worker(cfg):
                site_proxy = _make_proxy(cfg.get('proxy_url') or global_proxy_url)
                context = await browser.new_context(
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    viewport={'width': 1920, 'height': 1080},
                    locale='sv-SE',
                    timezone_id='Europe/Stockholm',
                    extra_http_headers={
                        'Accept-Language': 'sv-SE,sv;q=0.9,en-US;q=0.8,en;q=0.7',
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                        'DNT': '1',
                    },
                    proxy=site_proxy,
                )
                try:
                    await scrape_site(context, cfg, sem)
                finally:
                    await context.close()

            tasks = [worker(cfg) for cfg in configs]
            await asyncio.gather(*tasks)
    finally:
        if browser:
            await browser.close()
    
    flush_task.cancel()
    if write_buffer:
        await flush_buffer()
    
    logger.info(f"Done. New: {stats['products']}, Updated: {stats['updated']}, Skipped: {stats['skipped']}")


async def scraper_loop():
    global scraping_active
    while not shutdown_event.is_set():
        scraping_active = True
        try:
            await run_scraper()
        except (PlaywrightError, psycopg2.Error, OSError) as e:
            logger.error(f"Scraping failed: {e}")
        finally:
            scraping_active = False
        
        interval = get_setting('scrape_interval')
        for _ in range(interval):
            if shutdown_event.is_set():
                break
            await asyncio.sleep(1)


# === Flask API ===
@app.route('/health')
def health():
    return jsonify({'status': 'healthy' if db_pool else 'degraded', 'active': scraping_active, 'stats': stats})


@app.route('/config', methods=['GET'])
def get_configs():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM scraper_config ORDER BY name")
    columns = [desc[0] for desc in cur.description]
    configs = [dict(zip(columns, row)) for row in cur.fetchall()]
    return_db(conn)
    return jsonify(configs)


@app.route('/config', methods=['POST'])
def create_config():
    data = request.json
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO scraper_config
            (name, base_url, product_selector, title_selector, price_selector, link_selector,
             pagination_type, pagination_selector, max_pages, min_price, max_price, categories,
             use_stealth, proxy_url, exclude_link_pattern, url_scope)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            data['name'], data['base_url'],
            data['product_selector'], data['title_selector'],
            data['price_selector'], data['link_selector'],
            data.get('pagination_type', 'query'),
            data.get('pagination_selector'),
            data.get('max_pages', 10),
            data.get('min_price', 0),
            data.get('max_price', 999999),
            json.dumps(data.get('categories', [])),
            1 if data.get('use_stealth') else 0,
            data.get('proxy_url', ''),
            data.get('exclude_link_pattern', ''),
            data.get('url_scope', '')
        ))
        config_id = cur.fetchone()[0]
        conn.commit()
        return jsonify({'status': 'success', 'id': config_id})
    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        return jsonify({'status': 'error', 'message': 'Name already exists'}), 400
    except psycopg2.Error as e:
        conn.rollback()
        logger.error(f"Create config error: {e}")
        return jsonify({'status': 'error', 'message': 'Internal server error'}), 500
    finally:
        return_db(conn)


@app.route('/config/<int:config_id>', methods=['PUT'])
def update_config(config_id):
    data = request.json
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE scraper_config SET
                name = %s, base_url = %s, product_selector = %s, title_selector = %s,
                price_selector = %s, link_selector = %s, pagination_type = %s,
                pagination_selector = %s, max_pages = %s, enabled = %s,
                min_price = %s, max_price = %s, categories = %s,
                use_stealth = %s, proxy_url = %s, exclude_link_pattern = %s,
                url_scope = %s, updated_at = NOW()
            WHERE id = %s
        """, (
            data['name'], data['base_url'],
            data['product_selector'], data['title_selector'],
            data['price_selector'], data['link_selector'],
            data.get('pagination_type', 'query'),
            data.get('pagination_selector'),
            data.get('max_pages', 10),
            data.get('enabled', 1),
            data.get('min_price', 0),
            data.get('max_price', 999999),
            json.dumps(data.get('categories', [])),
            1 if data.get('use_stealth') else 0,
            data.get('proxy_url', ''),
            data.get('exclude_link_pattern', ''),
            data.get('url_scope', ''),
            config_id
        ))
        conn.commit()
        return jsonify({'status': 'success'})
    except psycopg2.Error as e:
        conn.rollback()
        logger.error(f"Update config error: {e}")
        return jsonify({'status': 'error', 'message': 'Internal server error'}), 500
    finally:
        return_db(conn)


@app.route('/config/<int:config_id>', methods=['DELETE'])
def delete_config(config_id):
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM products WHERE site_config_id = %s", (config_id,))
        cur.execute("DELETE FROM scraper_config WHERE id = %s", (config_id,))
        conn.commit()
        return jsonify({'status': 'success'})
    except psycopg2.Error as e:
        conn.rollback()
        logger.error(f"Delete config error: {e}")
        return jsonify({'status': 'error', 'message': 'Internal server error'}), 500
    finally:
        return_db(conn)


@app.route('/test', methods=['POST'])
def test_scrape_sync():
    """Test scraping - sync wrapper for async"""
    config = request.json
    
    async def _test():
        browser = None
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True, args=BROWSER_ARGS)
                page = await browser.new_page()
                try:
                    await page.goto(config['base_url'], timeout=30000)
                    await page.wait_for_load_state("domcontentloaded")
                    elements = await page.query_selector_all(config['product_selector'])
                    products = []
                    for elem in elements[:5]:
                        product = await extract_product(page, elem, config)
                        if product:
                            products.append(product)
                    return {'status': 'success', 'elements_found': len(elements), 'preview': products}
                finally:
                    await page.close()
        except PlaywrightError as e:
            logger.error("Test scrape failed: %s", e)
            return {'status': 'error', 'message': 'Test scrape failed'}
        finally:
            if browser:
                await browser.close()
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        result = loop.run_until_complete(_test())
    finally:
        loop.close()
    return jsonify(result)


@app.route('/detect', methods=['POST'])
def detect_selectors():
    """Auto-detect CSS selectors for a given URL using Playwright heuristics"""
    data = request.json or {}
    url = data.get('url', '').strip()
    if not url:
        return jsonify({'status': 'error', 'message': 'Invalid URL'}), 400
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url

    detect_js = """() => {
        const PRICE_RE = /\\d[\\d\\s]*\\s*(kr|SEK|:-|,\\d{2})/i;
        const selectorCount = {};

        function getSelector(el) {
            const tag = el.tagName.toLowerCase();
            if (el.className && typeof el.className === 'string' && el.className.trim()) {
                const classes = el.className.trim().split(/\\s+/)
                    .filter(c => c && !/\\d{5,}/.test(c))
                    .slice(0, 2).join('.');
                if (classes) return tag + '.' + classes;
            }
            return null;
        }

        document.querySelectorAll('article, li, div, section, a[href]').forEach(el => {
            const sel = getSelector(el);
            if (sel) selectorCount[sel] = (selectorCount[sel] || 0) + 1;
        });

        const candidates = Object.entries(selectorCount)
            .filter(([, count]) => count >= 3)
            .sort((a, b) => b[1] - a[1]);

        let productSelector = null, titleSelector = null,
            priceSelector = null, linkSelector = null;

        for (const [sel] of candidates) {
            const elements = Array.from(document.querySelectorAll(sel));
            const withPrice = elements.slice(0, 15).filter(el => PRICE_RE.test(el.innerText));
            if (withPrice.length < 2) continue;

            productSelector = sel;
            const container = withPrice[0];

            // Title: heading first, then class-based, then first leaf with substantial non-price text
            const heading = container.querySelector('h1, h2, h3, h4');
            if (heading) {
                const tag = heading.tagName.toLowerCase();
                const cls = (heading.className && typeof heading.className === 'string')
                    ? heading.className.trim().split(/\\s+/)[0] : '';
                titleSelector = cls ? tag + '.' + cls : tag;
            } else {
                const titleEl = container.querySelector('[class*="title"], [class*="name"], [class*="label"], [class*="heading"]');
                if (titleEl) {
                    const tag = titleEl.tagName.toLowerCase();
                    const cls = (titleEl.className || '').trim().split(/\\s+/)[0];
                    titleSelector = cls ? tag + '.' + cls : tag;
                } else {
                    for (const el of container.querySelectorAll('*')) {
                        const text = el.textContent.trim();
                        if (text.length > 10 && !PRICE_RE.test(text) && el.children.length === 0) {
                            const tag = el.tagName.toLowerCase();
                            const cls = (el.className && typeof el.className === 'string')
                                ? el.className.trim().split(/\\s+/)[0] : '';
                            titleSelector = cls ? tag + '.' + cls : tag;
                            break;
                        }
                    }
                }
            }

            // Price: find the shallowest element whose full text contains a price
            const walkPrice = (el) => {
                if (priceSelector) return;
                if (PRICE_RE.test(el.textContent)) {
                    // Prefer leaf nodes; if no leaf matches, take this node
                    if (el.children.length === 0) {
                        const tag = el.tagName.toLowerCase();
                        const cls = (el.className && typeof el.className === 'string')
                            ? el.className.trim().split(/\\s+/)[0] : '';
                        priceSelector = cls ? tag + '.' + cls : tag;
                        return;
                    }
                    // Check if any child contains the price; if not, use this element
                    const childMatch = Array.from(el.children).some(c => PRICE_RE.test(c.textContent));
                    if (!childMatch) {
                        const tag = el.tagName.toLowerCase();
                        const cls = (el.className && typeof el.className === 'string')
                            ? el.className.trim().split(/\\s+/)[0] : '';
                        priceSelector = cls ? tag + '.' + cls : tag;
                        return;
                    }
                    Array.from(el.children).forEach(walkPrice);
                }
            };
            walkPrice(container);

            // Price fallback: if walkPrice found nothing, look for a price-class element
            if (!priceSelector) {
                const priceEl = container.querySelector('[class*="price"], [class*="pris"], [class*="cost"], [class*="amount"]');
                if (priceEl) {
                    const tag = priceEl.tagName.toLowerCase();
                    const cls = (priceEl.className && typeof priceEl.className === 'string')
                        ? priceEl.className.trim().split(/\\s+/)[0] : '';
                    priceSelector = cls ? tag + '.' + cls : tag;
                }
            }

            // Link: container itself, first child anchor, or closest ancestor anchor
            const anchor = container.tagName === 'A' ? container
                : (container.querySelector('a[href]') || container.closest('a[href]'));
            if (anchor) {
                const cls = (anchor.className && typeof anchor.className === 'string')
                    ? anchor.className.trim().split(/\\s+/)[0] : '';
                linkSelector = cls ? 'a.' + cls : 'a';
            }

            break;
        }

        const ogSiteName = document.querySelector('meta[property="og:site_name"]');
        let siteName = '';
        if (ogSiteName && ogSiteName.content) {
            siteName = ogSiteName.content.trim();
        } else {
            siteName = document.title.split(/[-|–—]/)[0].trim();
        }

        // Fallback: retry with looser price matching (3+ digit number)
        if (!productSelector) {
            const LOOSE_PRICE_RE = /\\b\\d{3}[\\d\\s]*\\b/;
            for (const [sel] of candidates) {
                const elements = Array.from(document.querySelectorAll(sel));
                const withPrice = elements.slice(0, 20).filter(el => LOOSE_PRICE_RE.test(el.innerText));
                if (withPrice.length < 2) continue;
                productSelector = sel;
                const container = withPrice[0];
                const heading = container.querySelector('h1,h2,h3,h4,[class*="title"],[class*="name"]');
                if (heading) {
                    const tag = heading.tagName.toLowerCase();
                    const cls = (heading.className || '').trim().split(/\\s+/)[0];
                    titleSelector = cls ? tag + '.' + cls : tag;
                }
                const priceEl = container.querySelector('[class*="price"],[class*="pris"],[class*="cost"],[class*="amount"]');
                if (priceEl) {
                    const tag = priceEl.tagName.toLowerCase();
                    const cls = (priceEl.className || '').trim().split(/\\s+/)[0];
                    priceSelector = cls ? tag + '.' + cls : tag;
                }
                const anchor = container.tagName === 'A' ? container
                    : (container.querySelector('a[href]') || container.closest('a[href]'));
                if (anchor) {
                    const cls = (anchor.className || '').trim().split(/\\s+/)[0];
                    linkSelector = cls ? 'a.' + cls : 'a';
                }
                break;
            }
        }

        return {
            product_selector: productSelector,
            title_selector: titleSelector,
            price_selector: priceSelector,
            link_selector: linkSelector,
            site_name: siteName
        };
    }"""

    bot_detect_js = """() => {
        if (window._abck !== undefined || document.cookie.includes('_abck') ||
            document.querySelector('script[src*="akam"]'))
            return 'akamai';
        if (document.querySelector('#cf-challenge-running') ||
            document.querySelector('script[src*="challenges.cloudflare.com"]') ||
            document.cookie.includes('cf_clearance'))
            return 'cloudflare';
        if (document.querySelector('script[src*="perimeterx"]') ||
            document.querySelector('div[id*="px-captcha"]'))
            return 'perimeterx';
        if (document.querySelector('script[src*="distil"]') ||
            document.querySelector('body[class*="distil"]'))
            return 'distil';
        return 'none';
    }"""

    async def _detect():
        browser = None
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True, args=BROWSER_ARGS)
                page = await browser.new_page()
                await Stealth().apply_stealth_async(page)
                try:
                    await page.goto(url, timeout=60000, wait_until="domcontentloaded")
                    try:
                        await page.wait_for_load_state("networkidle", timeout=10000)
                    except PlaywrightError as e:
                        logger.debug("networkidle timeout during detect, continuing: %s", e)
                    await accept_cookies(page)
                    await asyncio.sleep(2)
                    # Scroll to trigger lazy-loaded content, then wait for it to render
                    for _ in range(3):
                        await page.evaluate("window.scrollTo(0, document.body.scrollHeight / 2)")
                        await asyncio.sleep(1)
                    await page.evaluate("window.scrollTo(0, 0)")
                    await asyncio.sleep(2)
                    result = await page.evaluate(detect_js)
                    bot_protection = await page.evaluate(bot_detect_js)
                    return {
                        'status': 'success',
                        **result,
                        'bot_protection': bot_protection,
                        'use_stealth': bot_protection != 'none',
                    }
                finally:
                    await page.close()
        except PlaywrightError as e:
            logger.error("Detection failed: %s", e)
            return {'status': 'error', 'message': 'Detection failed'}
        finally:
            if browser:
                await browser.close()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        result = loop.run_until_complete(_detect())
    finally:
        loop.close()
    return jsonify(result)


@app.route('/settings', methods=['GET'])
def get_settings():
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT key, value FROM settings")
        stored = {row[0]: row[1] for row in cur.fetchall()}
    finally:
        return_db(conn)
    result = {}
    for key, meta in SETTINGS_META.items():
        raw = stored.get(key)
        if raw is not None:
            val = raw
        elif meta['default'] is True:
            val = 'true'
        elif meta['default'] is False:
            val = 'false'
        else:
            val = str(meta['default'])
        result[key] = {**meta, 'value': val}
    return jsonify(result)


@app.route('/settings/<key>', methods=['PUT'])
def update_setting(key):
    if key not in SETTINGS_META:
        return jsonify({'status': 'error', 'message': 'Unknown setting'}), 400
    value = (request.json or {}).get('value')
    if value is None:
        return jsonify({'status': 'error', 'message': 'Missing value'}), 400
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO settings (key, value, updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
        """, (key, str(value)))
        conn.commit()
        return jsonify({'status': 'success'})
    except psycopg2.Error as e:
        conn.rollback()
        logger.error(f"Update setting error: {e}")
        return jsonify({'status': 'error', 'message': 'Failed to update setting'}), 500
    finally:
        return_db(conn)


@app.route('/credentials/password', methods=['PUT'])
def change_db_password():
    from psycopg2 import sql as pgsql
    data = request.json or {}
    new_pw = data.get('password', '').strip()
    if len(new_pw) < 8:
        return jsonify({'status': 'error', 'message': 'Password must be at least 8 characters'}), 400
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute(pgsql.SQL("ALTER USER {} WITH PASSWORD %s").format(
            pgsql.Identifier(get_db_user())), (new_pw,))
        conn.commit()
        write_credential('db_password', new_pw)  # lgtm[py/clear-text-storage-sensitive-data]
        reinit_db_pool()
        return jsonify({'status': 'success'})
    except psycopg2.Error as e:
        conn.rollback()
        logger.error(f"Change password error: {e}")
        return jsonify({'status': 'error', 'message': 'Failed to update password'}), 500
    finally:
        try:
            return_db(conn)
        except psycopg2.Error as e:
            logger.debug("Failed to return connection: %s", e)


@app.route('/credentials/username', methods=['PUT'])
def change_db_username():
    from psycopg2 import sql as pgsql
    data = request.json or {}
    new_user = data.get('username', '').strip()
    if not new_user or not new_user.replace('_', '').isalnum():
        return jsonify({'status': 'error', 'message': 'Invalid username (letters, numbers, underscores only)'}), 400
    old_user = get_db_user()
    if new_user == old_user:
        return jsonify({'status': 'success'})
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute(pgsql.SQL("ALTER USER {} RENAME TO {}").format(
            pgsql.Identifier(old_user), pgsql.Identifier(new_user)))
        conn.commit()
        write_credential('db_user', new_user)
        reinit_db_pool()
        return jsonify({'status': 'success'})
    except psycopg2.Error as e:
        conn.rollback()
        logger.error(f"Change username error: {e}")
        return jsonify({'status': 'error', 'message': 'Failed to update username'}), 500
    finally:
        try:
            return_db(conn)
        except psycopg2.Error as e:
            logger.debug("Failed to return connection: %s", e)


@app.route('/scrape', methods=['POST'])
def trigger_scrape():
    global scraping_active
    if scraping_active:
        return jsonify({'status': 'error', 'message': 'Already running'}), 409
    
    def run():
        global scraping_active
        scraping_active = True
        try:
            asyncio.run(run_scraper())
        except (PlaywrightError, psycopg2.Error, OSError) as e:
            logger.error(f"Scrape thread failed: {e}")
        finally:
            scraping_active = False

    threading.Thread(target=run, daemon=True).start()
    return jsonify({'status': 'success'})


@app.route('/trigger-scrape', methods=['POST'])
def trigger_scrape_alias():
    """Alias for /scrape"""
    return trigger_scrape()


_EXPORT_QUERIES = {
    (True, True): ("""
        SELECT p.title, p.current_price, p.url, c.name AS site,
            ph_old.price AS old_price,
            CASE WHEN ph_old.price > 0 AND p.current_price < ph_old.price
                 THEN ROUND((1 - p.current_price::numeric / ph_old.price) * 100)::int
                 ELSE NULL END AS drop_pct
        FROM products p
        JOIN scraper_config c ON p.site_config_id = c.id
        LEFT JOIN LATERAL (
            SELECT price FROM price_history
            WHERE product_id = p.id AND timestamp <= NOW() - INTERVAL '1 day'
            ORDER BY timestamp DESC LIMIT 1
        ) ph_old ON true
        WHERE p.current_price > 0 AND c.name = %s
        ORDER BY p.current_price ASC
    """, True),
    (True, False): ("""
        SELECT p.title, p.current_price, p.url, c.name AS site,
            ph_old.price AS old_price,
            CASE WHEN ph_old.price > 0 AND p.current_price < ph_old.price
                 THEN ROUND((1 - p.current_price::numeric / ph_old.price) * 100)::int
                 ELSE NULL END AS drop_pct
        FROM products p
        JOIN scraper_config c ON p.site_config_id = c.id
        LEFT JOIN LATERAL (
            SELECT price FROM price_history
            WHERE product_id = p.id AND timestamp <= NOW() - INTERVAL '1 day'
            ORDER BY timestamp DESC LIMIT 1
        ) ph_old ON true
        WHERE p.current_price > 0
        ORDER BY c.name, p.current_price ASC
    """, False),
    (False, True): ("""
        SELECT p.title, p.current_price, p.url, c.name AS site,
            NULL AS old_price, NULL AS drop_pct
        FROM products p
        JOIN scraper_config c ON p.site_config_id = c.id
        WHERE p.current_price > 0 AND c.name = %s
        ORDER BY p.current_price ASC
    """, True),
    (False, False): ("""
        SELECT p.title, p.current_price, p.url, c.name AS site,
            NULL AS old_price, NULL AS drop_pct
        FROM products p
        JOIN scraper_config c ON p.site_config_id = c.id
        WHERE p.current_price > 0
        ORDER BY c.name, p.current_price ASC
    """, False),
}



@app.route('/export/<site_name>')
def export_site_csv(site_name):
    include_drops = request.args.get('include_drops', '0') == '1'
    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    # Always use the site-specific query variant; site_name never influences query selection
    query, _ = _EXPORT_QUERIES[(include_drops, True)]
    cur.execute(query, (site_name,))
    products = cur.fetchall()
    return_db(conn)

    output = StringIO()
    writer = csv.writer(output)
    if include_drops:
        writer.writerow(['Product', 'Price (SEK)', 'Was (SEK)', 'Drop %', 'Link'])
        for p in products:
            writer.writerow([p['title'], p['current_price'],
                             p['old_price'] or '', p['drop_pct'] or '', p['url']])
    else:
        writer.writerow(['Product', 'Price (SEK)', 'Link'])
        for p in products:
            writer.writerow([p['title'], p['current_price'], p['url']])

    output.seek(0)
    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': f'attachment; filename={site_name}_{datetime.datetime.now().strftime("%Y%m%d")}.csv'}
    )


@app.route('/export')
def export_all_csv():
    include_drops = request.args.get('include_drops', '0') == '1'
    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    query, _ = _EXPORT_QUERIES[(include_drops, False)]
    cur.execute(query, ())
    products = cur.fetchall()
    return_db(conn)

    output = StringIO()
    writer = csv.writer(output)
    if include_drops:
        writer.writerow(['Site', 'Product', 'Price (SEK)', 'Was (SEK)', 'Drop %', 'Link'])
        for p in products:
            writer.writerow([p['site'], p['title'], p['current_price'],
                             p['old_price'] or '', p['drop_pct'] or '', p['url']])
    else:
        writer.writerow(['Site', 'Product', 'Price (SEK)', 'Link'])
        for p in products:
            writer.writerow([p['site'], p['title'], p['current_price'], p['url']])

    output.seek(0)
    filename = f"products_{datetime.datetime.now().strftime('%Y%m%d')}.csv"
    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': f'attachment; filename={filename}'}
    )


def signal_handler(signum, frame):
    logger.info(f"Signal {signum}, shutting down...")
    shutdown_event.set()


if __name__ == "__main__":
    init_credentials()
    init_db_pool()
    init_db()
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    from waitress import serve
    threading.Thread(target=lambda: serve(app, host='0.0.0.0', port=5001), daemon=True).start()
    asyncio.run(scraper_loop())
