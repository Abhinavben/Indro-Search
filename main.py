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

# Professional User-Agent
USER_AGENT = "IndroSearchBot/2.0 (Search Engine Research; Contact: admin@indro.in)"

TARGET_SITES = [
    "https://www.isro.gov.in/", 
    "https://www.nasa.gov/news/",
    "https://www.bbc.com/news", 
    "https://www.nature.com/",
    "https://gadgets360.com/",
    "https://www.britannica.com/"
]

queue = asyncio.Queue()
seen_urls = set()
blacklist = ['about', 'contact', 'privacy', 'terms', 'help', 'signin', 'login', 'signup', 'feedback', 'legal', 'admin']

async def handle_health(request):
    return web.Response(text="IndroSearchBot 2.0 is Active & Ethical! 🕷️")

async def backup_to_telegram(session, data):
    try:
        json_bytes = json.dumps(data, indent=2, ensure_ascii=False).encode('utf-8', errors='ignore')
        filename = f"data_{int(asyncio.get_event_loop().time())}.json"
        form = FormData()
        form.add_field('chat_id', str(TG_CHAT_ID))
        form.add_field('document', io.BytesIO(json_bytes), filename=filename)
        async with session.post(f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendDocument", data=form, timeout=10) as resp:
            return resp.status == 200
    except: return False

async def start_crawling():
    client = AsyncIOMotorClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    
    # Starting queue with depth 0
    for site in TARGET_SITES: await queue.put((site, 0))

    async with aiohttp.ClientSession(headers={'User-Agent': USER_AGENT}) as session:
        while True:
            url, depth = await queue.get()
            
            # Feature: Depth Limit (Max 3 levels deep for safety)
            if depth > 3 or url in seen_urls:
                queue.task_done()
                continue
            
            # DB Check
            existing = await collection.find_one({"url": url}, {"_id": 1})
            if existing:
                seen_urls.add(url)
                queue.task_done()
                continue

            try:
                # Feature: Extra Delay for Government Sites (.gov)
                current_delay = 2.5 if ".gov" in urlparse(url).netloc else 1.2
                
                async with session.get(url, timeout=7) as response:
                    if response.status != 200:
                        queue.task_done()
                        continue
                    
                    html = await response.text(errors='ignore')
                    soup = BeautifulSoup(html, 'html.parser')
                    title = soup.title.string.strip() if soup.title else "No Title"
                    
                    # Clean text extraction
                    for script in soup(["script", "style"]): script.extract()
                    text = soup.get_text(separator=' ', strip=True)[:2000]

                    page_data = {"url": url, "title": title, "text": text, "depth": depth}
                    
                    await asyncio.gather(
                        backup_to_telegram(session, page_data),
                        collection.update_one({"url": url}, {"$set": page_data}, upsert=True)
                    )
                    
                    seen_urls.add(url)
                    print(f"🕷️ IndroSearch: {title[:35]}... (Level {depth})", flush=True)

                    # Link Discovery
                    links_found = 0
                    for a in soup.find_all('a', href=True):
                        if links_found >= 12: break 
                        link = urljoin(url, a['href'])
                        
                        # Internal links only & Blacklist check
                        if urlparse(link).netloc == urlparse(url).netloc:
                            if not any(word in link.lower() for word in blacklist):
                                await queue.put((link, depth + 1))
                                links_found += 1

            except Exception: pass
            
            if len(seen_urls) > 8000: seen_urls.clear()
            
            queue.task_done()
            await asyncio.sleep(current_delay)

async def main():
    app = web.Application()
    app.router.add_get('/', handle_health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', int(os.environ.get("PORT", 10000)))
    await site.start()
    print("🚀 IndroSearchBot 2.0 Started. Purely Ethical & Secure. 🕷️", flush=True)
    asyncio.create_task(start_crawling())
    while True: await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())
