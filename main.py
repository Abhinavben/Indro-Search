import asyncio
import aiohttp
import json
import io
import os
import gc
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
QUEUE_COLLECTION = "link_queue"
USER_AGENT = "IndroSearchBot/5.0 (Enterprise Cloud Queue; Research Mode)"

# 40 TARGET SITES (News, Science, Tech, Business)
TARGET_SITES = [
    "https://www.isro.gov.in/", "https://www.nasa.gov/news/", "https://www.bbc.com/news", 
    "https://www.nature.com/", "https://gadgets360.com/", "https://www.indiatoday.in/science",
    "https://www.theverge.com/news", "https://www.reuters.com/", "https://www.ndtv.com/world-news",
    "https://www.cnn.com/world", "https://www.techcrunch.com/", "https://www.wired.com/",
    "https://www.cnet.com/news/", "https://www.bloomberg.com/", "https://www.forbes.com/",
    "https://www.theguardian.com/world", "https://www.nytimes.com/section/world", "https://www.wsj.com/news/world",
    "https://www.sciencedaily.com/", "https://www.space.com/news", "https://www.phys.org/",
    "https://www.britannica.com/", "https://www.nationalgeographic.com/", "https://www.cnbc.com/world-news/",
    "https://www.aljazeera.com/", "https://www.timesofindia.indiatimes.com/world", "https://www.hindustantimes.com/world-news",
    "https://www.news18.com/world/", "https://www.thehindu.com/news/international/", "https://www.engadget.com/",
    "https://www.gizmodo.com/", "https://www.arstechnica.com/", "https://www.mashable.com/",
    "https://www.scientificamerican.com/", "https://www.businessinsider.com/", "https://www.economist.com/",
    "https://www.ft.com/world", "https://www.latimes.com/world-nation", "https://www.abc.net.au/news/world/",
    "https://www.dw.com/en/world/"
]

async def handle_health(request):
    return web.Response(text="Indro Bot 5.0: Zero-RAM Engine is Pumping! 🚀")

async def backup_to_telegram(session, data):
    try:
        json_bytes = json.dumps(data, indent=2, ensure_ascii=False).encode('utf-8')
        form = FormData()
        form.add_field('chat_id', str(TG_CHAT_ID))
        form.add_field('document', io.BytesIO(json_bytes), filename=f"news_{int(asyncio.get_event_loop().time())}.json")
        async with session.post(f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendDocument", data=form, timeout=12) as resp:
            return resp.status == 200
    except Exception as e:
        print(f"   [TG ERROR] Failed to send: {e}", flush=True)
        return False

async def start_crawling():
    client = AsyncIOMotorClient(MONGO_URI)
    db = client[DB_NAME]
    pages_col = db[COLLECTION_NAME]
    queue_col = db[QUEUE_COLLECTION]
    
    print(f"🛠️ DB Init: Checking Queue Status...", flush=True)
    if await queue_col.count_documents({}) == 0:
        print(f"📥 Queue is empty. Injecting {len(TARGET_SITES)} master targets...", flush=True)
        for site in TARGET_SITES:
            await queue_col.update_one({"url": site}, {"$set": {"url": site, "depth": 0}}, upsert=True)

    async with aiohttp.ClientSession(headers={'User-Agent': USER_AGENT}) as session:
        while True:
            # 1. Fetch from DB Queue
            task = await queue_col.find_one_and_delete({})
            
            if not task:
                print("📭 DB Queue Empty. Waiting 30s to avoid loop...", flush=True)
                await asyncio.sleep(30)
                continue

            url = task['url']
            depth = task['depth']

            print(f"🔍 [PROCESS] Level {depth} | URL: {url}", flush=True)

            if depth > 2:
                print(f"   [SKIP] Max depth reached.", flush=True)
                continue

            try:
                print(f"   [FETCH] Requesting page...", flush=True)
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=12)) as response:
                    print(f"   [RESP] Status: {response.status}", flush=True)
                    if response.status != 200: continue
                    
                    html = await response.text(errors='ignore')
                    print(f"   [BS4] Parsing HTML ({len(html)} chars)...", flush=True)
                    soup = BeautifulSoup(html, 'html.parser')
                    title = soup.title.string.strip() if soup.title else "No Title"
                    
                    # Already Processed Check
                    is_old = await pages_col.find_one({"url": url}, {"_id": 1})
                    if not is_old:
                        print(f"   [DATA] New Page Found: {title[:40]}", flush=True)
                        text = soup.get_text(separator=' ', strip=True)[:1200]
                        page_data = {"url": url, "title": title, "text": text}
                        
                        # Save & Send
                        await asyncio.gather(
                            backup_to_telegram(session, page_data),
                            pages_col.update_one({"url": url}, {"$set": page_data}, upsert=True)
                        )
                        print(f"   [SAVE] Saved to Cloud & Telegram ✅", flush=True)
                    else:
                        print(f"   [SKIP] Already in DB.", flush=True)

                    # Discovery - Back to DB Queue
                    print(f"   [LINKS] Extracting new URLs...", flush=True)
                    links_count = 0
                    for a in soup.find_all('a', href=True):
                        if links_count >= 8: break
                        new_link = urljoin(url, a['href'])
                        
                        # Internal news checking
                        if urlparse(new_link).netloc == urlparse(url).netloc:
                            await queue_col.update_one(
                                {"url": new_link}, 
                                {"$setOnInsert": {"url": new_link, "depth": depth + 1}}, 
                                upsert=True
                            )
                            links_count += 1
                    print(f"   [QUEUE] Added {links_count} sub-links to DB.", flush=True)
                    
                    # Mandatory Memory Clean
                    del soup
                    del html
                    gc.collect() # Force RAM release
                    print(f"   [CLEAN] RAM Purged. Cycle complete.", flush=True)

            except Exception as e:
                print(f"   [ERROR] Cycle failed: {str(e)}", flush=True)
            
            await asyncio.sleep(2.5) # Gentle pacing

async def main():
    app = web.Application()
    app.router.add_get('/', handle_health)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 10000))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    print(f"🚀 INDRO 5.0 ACTIVE! Monitoring {len(TARGET_SITES)} domains on port {port}", flush=True)
    asyncio.create_task(start_crawling())
    while True: await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())
