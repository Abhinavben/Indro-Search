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
MAX_QUEUE_SIZE = 100 
CRAWL_DELAY = 2      # Fast speed
START_URL = "https://www.isro.gov.in/"

seen_urls = set()
queue = asyncio.Queue()

# --- 1. RENDER HEALTH CHECK SERVER ---
async def handle_home(request):
    return web.Response(text=f"Indro Engine is Live! Memory: {len(seen_urls)} links.")

# --- 2. TELEGRAM BACKUP ---
async def backup_to_telegram(session, url, data):
    try:
        json_str = json.dumps(data, indent=2, ensure_ascii=False)
        file_obj = io.BytesIO(json_str.encode('utf-8', errors='ignore'))
        file_obj.name = "indro_data.json"
        form = FormData()
        form.add_field('chat_id', str(TG_CHAT_ID))
        form.add_field('document', file_obj)
        # Timeout badha diya hai taaki file miss na ho
        async with session.post(f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendDocument", data=form, timeout=10) as resp:
            return resp.status == 200
    except: return False

# --- 3. CRAWLER PROCESS ---
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
            
            # Action
            await backup_to_telegram(session, url, {"url": url, "title": title, "text": soup.get_text()[:4000]})
            await db[COLLECTION_NAME].update_one({"url": url}, {"$set": {"url": url, "title": title}}, upsert=True)
            
            seen_urls.add(url)
            print(f"✅ Indexed: {title[:30]}")

            # Collect New Links
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
    
    # Mongo se history load karo
    async for doc in db[COLLECTION_NAME].find({}, {"url": 1}):
        seen_urls.add(doc['url'])
    print(f"🧠 Memory Loaded: {len(seen_urls)} links.")

    async with aiohttp.ClientSession() as session:
        # Jumpstart: ISRO ke main links se shuru karo
        await queue.put(START_URL)
        await queue.put("https://www.isro.gov.in/Missions_Archives.html")
        
        while True:
            url = await queue.get()
            await process_url(session, db, url)
            await asyncio.sleep(CRAWL_DELAY)

# --- 5. MAIN ---
async def main():
    app = web.Application()
    app.router.add_get('/', handle_home)
    runner = web.AppRunner(app)
    await runner.setup()
    
    port = int(os.environ.get("PORT", 10000))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    print(f"🚀 Port {port} Bound. Starting Crawler...")
    
    # Background Task Start
    asyncio.create_task(start_crawling())
    
    # Keep the server alive
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())
