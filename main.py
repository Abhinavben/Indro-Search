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

CRAWL_DELAY = 3  # Thoda sa delay badhaya hai safety ke liye
TARGET_SITES = [
    "https://www.isro.gov.in/", 
    "https://www.bbc.com/news", 
    "https://www.nature.com/",
    "https://www.theverge.com/",
    "https://www.britannica.com/"
]

queue = asyncio.Queue()

# --- 1. RENDER HEALTH CHECK ---
async def handle_health(request):
    return web.Response(text="Indro Engine is running in Super-Light Mode! 🚀")

# --- 2. TELEGRAM BACKUP ---
async def backup_to_telegram(session, data):
    try:
        json_bytes = json.dumps(data, indent=2, ensure_ascii=False).encode('utf-8', errors='ignore')
        safe_title = "".join(x for x in data['title'][:20] if x.isalnum() or x in "._- ")
        filename = f"{safe_title or 'data'}.json"
        
        form = FormData()
        form.add_field('chat_id', str(TG_CHAT_ID))
        form.add_field('document', io.BytesIO(json_bytes), filename=filename)
        
        async with session.post(f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendDocument", data=form, timeout=10) as resp:
            return resp.status == 200
    except: return False

# --- 3. CRAWLER (MEMORY OPTIMIZED) ---
async def start_crawling():
    client = AsyncIOMotorClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    
    # Queue ko shuru karo
    for site in TARGET_SITES:
        await queue.put(site)

    async with aiohttp.ClientSession() as session:
        while True:
            url = await queue.get()

            # RAM ki jagah Database se pucho: "Kya ye URL humne padh liya hai?"
            existing = await collection.find_one({"url": url}, {"_id": 1})
            if existing:
                queue.task_done()
                continue

            try:
                headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0'}
                async with session.get(url, timeout=10, headers=headers) as response:
                    if response.status != 200: 
                        queue.task_done()
                        continue
                    
                    html = await response.text(errors='ignore')
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    title = soup.title.string.strip() if soup.title else "No Title"
                    # Text limit 2000 kar di hai RAM bachane ke liye
                    text = soup.get_text(separator=' ', strip=True)[:2000]

                    page_data = {"url": url, "title": title, "text": text}
                    
                    # Parallel Save: Telegram + MongoDB
                    await asyncio.gather(
                        backup_to_telegram(session, page_data),
                        collection.update_one({"url": url}, {"$set": page_data}, upsert=True)
                    )
                    
                    print(f"✅ Indexed: {title[:25]}", flush=True)

                    # Links Discovery (Limited to 10 for safety)
                    for a in soup.find_all('a', href=True)[:10]:
                        link = urljoin(url, a['href'])
                        # Apni hi website ke andar raho
                        if urlparse(link).netloc == urlparse(url).netloc:
                            await queue.put(link)

            except Exception as e:
                print(f"⚠️ Error: {str(e)[:50]}", flush=True)
            
            queue.task_done()
            await asyncio.sleep(CRAWL_DELAY)

# --- 4. MAIN ENGINE ---
async def main():
    app = web.Application()
    app.router.add_get('/', handle_health)
    runner = web.AppRunner(app)
    await runner.setup()
    
    port = int(os.environ.get("PORT", 10000))
    site = web.TCPSite(runner, '0.0.0.0', port)
    
    print(f"🚀 Light Engine Starting on Port {port}...", flush=True)
    await site.start()

    # Crawler in Background
    asyncio.create_task(start_crawling())
    
    # Keep Server Alive
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())
