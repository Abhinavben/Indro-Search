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
USER_AGENT = "IndroSearchBot/3.0 (Research Purpose)"

TARGET_SITES = [
    "https://www.isro.gov.in/", 
    "https://www.nasa.gov/news/",
    "https://www.bbc.com/news", 
    "https://www.nature.com/",
    "https://gadgets360.com/",
    "https://www.britannica.com/"
]

queue = asyncio.Queue()
blacklist = ['about', 'contact', 'privacy', 'terms', 'help', 'signin', 'login', 'signup', 'admin']

async def handle_health(request):
    return web.Response(text="Indro Bot is ALIVE! 🚀")

async def backup_to_telegram(session, data):
    try:
        json_bytes = json.dumps(data, indent=2, ensure_ascii=False).encode('utf-8')
        form = FormData()
        form.add_field('chat_id', str(TG_CHAT_ID))
        form.add_field('document', io.BytesIO(json_bytes), filename=f"news_{int(asyncio.get_event_loop().time())}.json")
        async with session.post(f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendDocument", data=form, timeout=15) as resp:
            return resp.status == 200
    except: return False

async def start_crawling():
    client = AsyncIOMotorClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    
    # Fresh start every time
    for site in TARGET_SITES: await queue.put((site, 0))

    async with aiohttp.ClientSession(headers={'User-Agent': USER_AGENT}) as session:
        while True:
            url, depth = await queue.get()
            
            # Increased depth to explore more sub-pages
            if depth > 5:
                queue.task_done()
                continue

            try:
                print(f"🔍 Attempting: {url} (Depth {depth})", flush=True)
                
                # Super tight timeout to prevent freezing
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as response:
                    if response.status != 200:
                        queue.task_done()
                        continue
                    
                    html = await response.text(errors='ignore')
                    soup = BeautifulSoup(html, 'html.parser')
                    title = soup.title.string.strip() if soup.title else "Untitled"
                    
                    # DB Check - Only skip if already present
                    existing = await collection.find_one({"url": url}, {"_id": 1})
                    
                    if not existing:
                        text = soup.get_text(separator=' ', strip=True)[:2000]
                        page_data = {"url": url, "title": title, "text": text}
                        
                        # Send and Save
                        success = await backup_to_telegram(session, page_data)
                        await collection.update_one({"url": url}, {"$set": page_data}, upsert=True)
                        
                        if success:
                            print(f"✅ Telegram Sent: {title[:30]}", flush=True)
                    else:
                        print(f"⏩ Skipping (Old): {title[:30]}", flush=True)

                    # Discovery - Get more links
                    links_found = 0
                    for a in soup.find_all('a', href=True):
                        if links_found >= 15: break
                        link = urljoin(url, a['href'])
                        if urlparse(link).netloc == urlparse(url).netloc:
                            if not any(word in link.lower() for word in blacklist):
                                await queue.put((link, depth + 1))
                                links_found += 1

            except Exception as e:
                print(f"⚠️ Error: {str(e)}", flush=True)
            
            queue.task_done()
            # Fast pacing for morning catch-up
            await asyncio.sleep(1)

async def main():
    app = web.Application()
    app.router.add_get('/', handle_health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', int(os.environ.get("PORT", 10000)))
    await site.start()
    print("🚀 INDRO REVIVAL MODE STARTED! 🕷️", flush=True)
    asyncio.create_task(start_crawling())
    while True: await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())
