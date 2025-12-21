import asyncio
import aiohttp
import json
import io
import os
from aiohttp import web, FormData
from motor.motor_asyncio import AsyncIOMotorClient
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

# --- 1. CONFIG ---
MONGO_URI = os.getenv("MONGO_URI")
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")

DB_NAME = "render_data"
COLLECTION_NAME = "web_pages_v2"
MAX_QUEUE_SIZE = 50  # Memory safe
CRAWL_DELAY = 5      # Balanced speed
START_URL = "https://www.isro.gov.in/"

seen_urls = set()
queue = asyncio.Queue()

# --- 2. RENDER HEALTH CHECK (The Proper Way) ---
async def handle_health(request):
    return web.Response(text="Indro is Alive")

# --- 3. TELEGRAM BACKUP ---
async def backup_to_telegram(session, url, data):
    try:
        json_str = json.dumps(data, indent=2, ensure_ascii=False)
        file_obj = io.BytesIO(json_str.encode('utf-8', errors='ignore'))
        file_obj.name = "indro_data.json"
        
        form = FormData()
        form.add_field('chat_id', str(TG_CHAT_ID)) # Ensure string
        form.add_field('document', file_obj)
        
        async with session.post(f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendDocument", data=form) as resp:
            return resp.status == 200
    except Exception as e:
        print(f"❌ TG Send Error: {e}")
        return False

# --- 4. CRAWLER LOGIC ---
async def process_url(session, db, url):
    if url in seen_urls: return
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        async with session.get(url, timeout=15, headers=headers) as response:
            if response.status != 200: return
            html = await response.text(errors='ignore')
            soup = BeautifulSoup(html, 'html.parser')
            title = soup.title.string.strip() if soup.title else "No Title"
            
            # Action
            await backup_to_telegram(session, url, {"url": url, "title": title, "text": soup.get_text()[:4000]})
            await db[COLLECTION_NAME].update_one({"url": url}, {"$set": {"url": url, "title": title}}, upsert=True)
            
            seen_urls.add(url)
            print(f"✅ Indexed: {title[:30]}")

            if queue.qsize() < MAX_QUEUE_SIZE:
                for a in soup.find_all('a', href=True):
                    link = urljoin(url, a['href'])
                    if urlparse(link).netloc == urlparse(url).netloc:
                        if link not in seen_urls: await queue.put(link)
    except Exception as e:
        print(f"⚠️ Skip {url}: {e}")

# --- 5. MAIN ENGINE ---
async def run_crawler():
    client = AsyncIOMotorClient(MONGO_URI)
    db = client[DB_NAME]
    
    # Reload history
    async for doc in db[COLLECTION_NAME].find({}, {"url": 1}):
        seen_urls.add(doc['url'])
    print(f"🧠 Memory Loaded: {len(seen_urls)}")

    async with aiohttp.ClientSession() as session:
        if not seen_urls: await queue.put(START_URL)
        else: await queue.put(list(seen_urls)[-1]) # Resume from last

        while True:
            url = await queue.get()
            await process_url(session, db, url)
            await asyncio.sleep(CRAWL_DELAY)

async def start_all():
    # Health check server start
    app = web.Application()
    app.router.add_get('/', handle_health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', int(os.getenv("PORT", 10000)))
    
    print("🚀 Starting Web Server & Crawler...")
    await asyncio.gather(
        site.start(),
        run_crawler()
    )

if __name__ == "__main__":
    asyncio.run(start_all())
