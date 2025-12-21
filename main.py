import asyncio
import aiohttp
import json
import io
import os
from aiohttp import web, FormData
from motor.motor_asyncio import AsyncIOMotorClient
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

# --- CONFIGURATION ---
MONGO_URI = os.getenv("MONGO_URI")
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")
DB_NAME = "render_data"
COLLECTION_NAME = "web_pages_v2"
MAX_QUEUE_SIZE = 150 
CRAWL_DELAY = 3  # Safe & Balanced Speed

# --- MULTI-FIELD WEBSITES (No Google) ---
TARGET_SITES = [
    "https://www.isro.gov.in/",           # Space (Govt)
    "https://www.nasa.gov/",              # Space (Science)
    "https://www.bbc.com/news",           # Global News
    "https://www.nature.com/",            # Pure Science/Research
    "https://www.theverge.com/",          # Technology
    "https://www.britannica.com/",        # Education/Encyclopedia
    "https://www.nationalgeographic.com/",# Environment/Nature
    "https://www.healthline.com/"         # Health & Medicine
]

seen_urls = set()
queue = asyncio.Queue()

# --- 1. RENDER HEALTH CHECK ---
async def handle_home(request):
    return web.Response(text=f"Indro Engine is Active! Memory: {len(seen_urls)} links.")

# --- 2. TELEGRAM BACKUP ---
async def backup_to_telegram(session, url, data):
    try:
        json_str = json.dumps(data, indent=2, ensure_ascii=False)
        file_obj = io.BytesIO(json_str.encode('utf-8', errors='ignore'))
        file_obj.name = "indro_doc.json"
        form = FormData()
        form.add_field('chat_id', str(TG_CHAT_ID))
        form.add_field('document', file_obj)
        async with session.post(f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendDocument", data=form, timeout=12) as resp:
            return resp.status == 200
    except: return False

# --- 3. CRAWLER LOGIC ---
async def process_url(session, db, url):
    if url in seen_urls: return
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        async with session.get(url, timeout=15, headers=headers) as response:
            if response.status != 200: return
            raw = await response.read()
            html = raw.decode('utf-8', errors='ignore')
            soup = BeautifulSoup(html, 'html.parser')
            
            title = soup.title.string.strip() if soup.title else "No Title"
            text_content = soup.get_text(separator=' ', strip=True)[:5000]
            
            # Send to Telegram & Save to Mongo
            await backup_to_telegram(session, url, {"url": url, "title": title, "text": text_content})
            await db[COLLECTION_NAME].update_one({"url": url}, {"$set": {"url": url, "title": title}}, upsert=True)
            
            seen_urls.add(url)
            print(f"✅ Indexed [{urlparse(url).netloc}]: {title[:30]}")

            # Link Discovery (Same Domain Only)
            if queue.qsize() < MAX_QUEUE_SIZE:
                for a in soup.find_all('a', href=True):
                    link = urljoin(url, a['href'])
                    if urlparse(link).netloc == urlparse(url).netloc:
                        if link not in seen_urls: await queue.put(link)
    except Exception as e:
        print(f"⚠️ Skip {url}: {e}")

# --- 4. ENGINE RUNNER ---
async def start_crawling():
    client = AsyncIOMotorClient(MONGO_URI)
    db = client[DB_NAME]
    
    # Reload History
    async for doc in db[COLLECTION_NAME].find({}, {"url": 1}):
        seen_urls.add(doc['url'])
    print(f"🧠 {len(seen_urls)} links restored. Ready to explore!")

    async with aiohttp.ClientSession() as session:
        # Multi-Field Jumpstart
        for site in TARGET_SITES:
            await queue.put(site)
            
        while True:
            url = await queue.get()
            await process_url(session, db, url)
            await asyncio.sleep(CRAWL_DELAY)

# --- 5. MAIN (Server + Background Crawler) ---
async def main():
    app = web.Application()
    app.router.add_get('/', handle_home)
    runner = web.AppRunner(app)
    await runner.setup()
    
    port = int(os.environ.get("PORT", 10000))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    print(f"🚀 Indro Search Server Active on Port {port}")
    
    # Background Crawler Task
    asyncio.create_task(start_crawling())
    
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())
