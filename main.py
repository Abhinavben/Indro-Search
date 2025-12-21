import asyncio
import aiohttp
import json
import io
import os
from aiohttp import web, FormData
from motor.motor_asyncio import AsyncIOMotorClient
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

# --- 1. CONFIGURATION ---
MONGO_URI = os.getenv("MONGO_URI")
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")
DB_NAME = "render_data"
COLLECTION_NAME = "web_pages_v2"

# Features Settings
MAX_MEMORY_LINKS = 100  # Feature 1: RAM Clear after 100 links
CRAWL_DELAY = 2         # Feature 2: 2-3 Second Delay
TARGET_SITES = [
    "https://www.isro.gov.in/", 
    "https://www.bbc.com/news", 
    "https://www.nature.com/",
    "https://www.theverge.com/",
    "https://www.britannica.com/"
]

seen_urls = set()
queue = asyncio.Queue()

# --- 2. RENDER PORT BINDING (Anti-Crash) ---
async def handle_health(request):
    return web.Response(text=f"Indro Engine is Alive! Memory: {len(seen_urls)} links.")

# --- 3. TELEGRAM BACKUP FEATURE ---
async def backup_to_telegram(session, data):
    try:
        json_bytes = json.dumps(data, indent=2, ensure_ascii=False).encode('utf-8', errors='ignore')
        
        # Unique Filename based on title
        safe_title = "".join(x for x in data['title'][:20] if x.isalnum() or x in "._- ")
        filename = f"{safe_title or 'data'}.json"
        
        form = FormData()
        form.add_field('chat_id', str(TG_CHAT_ID))
        form.add_field('document', io.BytesIO(json_bytes), filename=filename)
        
        async with session.post(f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendDocument", data=form, timeout=15) as resp:
            return resp.status == 200
    except: return False

# --- 4. CORE CRAWLER PROCESS ---
async def start_crawling():
    client = AsyncIOMotorClient(MONGO_URI)
    db = client[DB_NAME]
    
    # Reload existing data count
    async for doc in db[COLLECTION_NAME].find({}, {"url": 1}):
        seen_urls.add(doc['url'])
    print(f"🧠 Memory Loaded: {len(seen_urls)} links.", flush=True)

    # Jumpstart Queue
    for site in TARGET_SITES:
        await queue.put(site)

    async with aiohttp.ClientSession() as session:
        while True:
            # FEATURE: RAM CLEAR
            if len(seen_urls) > MAX_MEMORY_LINKS:
                seen_urls.clear()
                print("🧹 RAM Cleared... Resetting seen URLs.", flush=True)

            url = await queue.get()
            if url in seen_urls: continue

            try:
                headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0'}
                async with session.get(url, timeout=15, headers=headers) as response:
                    if response.status != 200: continue
                    
                    html = await response.text(errors='ignore')
                    soup = BeautifulSoup(html, 'html.parser')
                    title = soup.title.string.strip() if soup.title else "No Title"
                    text = soup.get_text(separator=' ', strip=True)[:4500]

                    # FEATURE: TELEGRAM & MONGO SAVE
                    page_data = {"url": url, "title": title, "text": text}
                    await backup_to_telegram(session, page_data)
                    await db[COLLECTION_NAME].update_one({"url": url}, {"$set": page_data}, upsert=True)
                    
                    seen_urls.add(url)
                    print(f"✅ Indexed: {title[:30]}", flush=True)

                    # Discovery
                    for a in soup.find_all('a', href=True)[:15]:
                        link = urljoin(url, a['href'])
                        if urlparse(link).netloc == urlparse(url).netloc:
                            if link not in seen_urls: await queue.put(link)

                # FEATURE: DELAY
                await asyncio.sleep(CRAWL_DELAY)

            except Exception as e:
                print(f"⚠️ Skip {url}: {e}", flush=True)
                continue

# --- 5. MAIN INTEGRATION ---
async def main():
    # Setup Web Server for Render Health Check
    app = web.Application()
    app.router.add_get('/', handle_health)
    runner = web.AppRunner(app)
    await runner.setup()
    
    port = int(os.environ.get("PORT", 10000))
    site = web.TCPSite(runner, '0.0.0.0', port)
    
    print(f"🚀 Starting Server on Port {port}...", flush=True)
    await site.start()
    print(f"✅ Port {port} is OPEN. Render should be happy.", flush=True)

    # Start Crawler in Background
    asyncio.create_task(start_crawling())
    
    # Keep Alive
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())
