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
MAX_MEMORY_LINKS = 100  # Feature: RAM Clear limit
CRAWL_DELAY = 2         # Feature: 2 Second Delay
TARGET_SITES = ["https://www.isro.gov.in/", "https://www.bbc.com/news", "https://www.nature.com/"]

seen_urls = set()
queue = asyncio.Queue()

# --- 2. RENDER PORT BINDING (Anti-Crash) ---
async def handle_health(request):
    return web.Response(text=f"Indro Active. Memory: {len(seen_urls)}")

# --- 3. TELEGRAM BACKUP FEATURE ---
async def backup_to_telegram(session, data):
    try:
        json_bytes = json.dumps(data, indent=2, ensure_ascii=False).encode('utf-8', errors='ignore')
        form = FormData()
        form.add_field('chat_id', str(TG_CHAT_ID))
        form.add_field('document', io.BytesIO(json_bytes), filename="indro_data.json")
        async with session.post(f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendDocument", data=form) as resp:
            return resp.status == 200
    except: return False

# --- 4. CORE CRAWLER WITH SMART FEATURES ---
async def start_crawling():
    client = AsyncIOMotorClient(MONGO_URI)
    db = client[DB_NAME]
    
    # Starting URLs load karo
    for site in TARGET_SITES:
        await queue.put(site)

    async with aiohttp.ClientSession() as session:
        while True:
            # FEATURE: RAM CLEAR (Memory Management)
            if len(seen_urls) > MAX_MEMORY_LINKS:
                seen_urls.clear()
                print("🧹 RAM Cleared for performance...")

            url = await queue.get()
            if url in seen_urls: continue

            try:
                # User-Agent add kiya taaki real browser lage
                headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/119.0.0.0'}
                async with session.get(url, timeout=15, headers=headers) as response:
                    if response.status != 200: continue
                    
                    # Encoding fix
                    raw = await response.read()
                    html = raw.decode('utf-8', errors='ignore')
                    
                    soup = BeautifulSoup(html, 'html.parser')
                    title = soup.title.string.strip() if soup.title else "No Title"
                    text = soup.get_text(separator=' ', strip=True)[:4000]

                    # FEATURE: TELEGRAM & MONGO SAVE
                    data = {"url": url, "title": title, "text": text}
                    await backup_to_telegram(session, data)
                    await db[COLLECTION_NAME].update_one({"url": url}, {"$set": data}, upsert=True)
                    
                    seen_urls.add(url)
                    print(f"✅ Indexed: {title[:30]}")

                    # Link discovery (Same domain only to avoid getting lost)
                    for a in soup.find_all('a', href=True)[:15]:
                        link = urljoin(url, a['href'])
                        if urlparse(link).netloc == urlparse(url).netloc:
                            if link not in seen_urls: await queue.put(link)

                # FEATURE: 2-3 SEC DELAY
                await asyncio.sleep(CRAWL_DELAY)

            except Exception as e:
                print(f"⚠️ Skip {url}: {e}")
                continue

# --- 5. MAIN (Server + Crawler Integration) ---
async def main():
    # Setup Web Server for Render
    app = web.Application()
    app.router.add_get('/', handle_health)
    runner = web.AppRunner(app)
    await runner.setup()
    
    port = int(os.environ.get("PORT", 10000))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    print(f"🚀 Indro Master Server Live on Port {port}")

    # Start Crawler as a background task
    asyncio.create_task(start_crawling())
    
    # Keep it alive
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())
