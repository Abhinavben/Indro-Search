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
COLLECTION_NAME = "web_pages_v3"

CRAWL_DELAY = 1 
TARGET_SITES = [
    "https://www.isro.gov.in/", 
    "https://www.nasa.gov/news/",
    "https://www.bbc.com/news", 
    "https://www.nature.com/",
    "https://gadgets360.com/",
    "https://www.britannica.com/"
]

queue = asyncio.Queue()
blacklist = ['about', 'contact', 'privacy', 'terms', 'help', 'signin', 'login', 'signup', 'feedback', 'legal']

async def handle_health(request):
    return web.Response(text="Indro Spider Engine is Active! 🕷️")

async def backup_to_telegram(session, data):
    try:
        json_bytes = json.dumps(data, indent=2, ensure_ascii=False).encode('utf-8', errors='ignore')
        filename = f"data_{int(asyncio.get_event_loop().time())}.json"
        form = FormData()
        form.add_field('chat_id', str(TG_CHAT_ID))
        form.add_field('document', io.BytesIO(json_bytes), filename=filename)
        async with session.post(f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendDocument", data=form) as resp:
            return resp.status == 200
    except: return False

async def start_crawling():
    client = AsyncIOMotorClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    
    for site in TARGET_SITES: await queue.put(site)

    async with aiohttp.ClientSession() as session:
        while True:
            url = await queue.get()
            
            existing = await collection.find_one({"url": url}, {"_id": 1})
            if existing:
                queue.task_done()
                continue

            try:
                headers = {'User-Agent': 'Mozilla/5.0'}
                async with session.get(url, timeout=10, headers=headers) as response:
                    if response.status != 200: 
                        queue.task_done()
                        continue
                    
                    html = await response.text(errors='ignore')
                    soup = BeautifulSoup(html, 'html.parser')
                    title = soup.title.string.strip() if soup.title else "No Title"
                    text = soup.get_text(separator=' ', strip=True)[:2500]

                    page_data = {"url": url, "title": title, "text": text}
                    
                    await asyncio.gather(
                        backup_to_telegram(session, page_data),
                        collection.update_one({"url": url}, {"$set": page_data}, upsert=True)
                    )
                    
                    # Wahi purana style wapas!
                    print(f"🕷️ Crawling: {title[:35]}...", flush=True)

                    links_found = 0
                    for a in soup.find_all('a', href=True):
                        if links_found >= 15: break 
                        link = urljoin(url, a['href'])
                        if urlparse(link).netloc == urlparse(url).netloc:
                            if not any(word in link.lower() for word in blacklist):
                                await queue.put(link)
                                links_found += 1

            except Exception: pass
            
            queue.task_done()
            await asyncio.sleep(CRAWL_DELAY)

async def main():
    app = web.Application()
    app.router.add_get('/', handle_health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', int(os.environ.get("PORT", 10000)))
    await site.start()
    print("🚀 Spider Bot Started. Web chhanna shuru! 🕷️", flush=True)
    asyncio.create_task(start_crawling())
    while True: await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())
