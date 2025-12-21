import asyncio
import aiohttp
import json
import io
import os
from aiohttp import FormData
from motor.motor_asyncio import AsyncIOMotorClient
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

# --- CONFIG ---
MONGO_URI = os.getenv("MONGO_URI")
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")

DB_NAME = "render_data"
COLLECTION_NAME = "web_pages_v2"
MAX_QUEUE_SIZE = 100 
CRAWL_DELAY = 3      
START_URL = "https://www.isro.gov.in/"

seen_urls = set()
queue = asyncio.Queue()

async def backup_to_telegram(session, url, data):
    try:
        json_bytes = json.dumps(data, indent=2, ensure_ascii=False).encode('utf-8', errors='ignore')
        file_obj = io.BytesIO(json_bytes)
        file_obj.name = f"indro_{str(asyncio.get_event_loop().time()).replace('.', '')}.json"
        form = FormData()
        form.add_field('chat_id', TG_CHAT_ID)
        form.add_field('document', file_obj)
        tg_url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendDocument"
        async with session.post(tg_url, data=form) as resp:
            return resp.status == 200
    except: return False

async def process_url(session, db, url):
    if url in seen_urls: return
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        async with session.get(url, timeout=20, headers=headers) as response:
            if response.status != 200: return
            
            # --- FIX: ERROR IGNORE LOGIC ---
            raw_content = await response.read()
            # errors='ignore' ensures it never stops at 0x9c or any other byte
            html_text = raw_content.decode('utf-8', errors='ignore') 
            
            soup = BeautifulSoup(html_text, 'html.parser')
            title = soup.title.string.strip() if soup.title else "No Title"
            
            page_data = {
                "url": url,
                "title": title,
                "text": soup.get_text(separator=' ', strip=True)[:8000]
            }
            await backup_to_telegram(session, url, page_data)
            await db[COLLECTION_NAME].update_one({"url": url}, {"$set": {"url": url, "title": title}}, upsert=True)
            seen_urls.add(url)
            print(f"✅ Safe Indexed: {title[:30]}")

            if queue.qsize() < MAX_QUEUE_SIZE:
                for a in soup.find_all('a', href=True):
                    link = urljoin(url, a['href'])
                    if urlparse(link).netloc == urlparse(url).netloc:
                        if link not in seen_urls: await queue.put(link)
    except Exception as e:
        # Error aane par ye line use skip karke aage nikal degi
        print(f"⚠️ Skipping {url} due to: {e}")

async def main():
    if not MONGO_URI: return
    client = AsyncIOMotorClient(MONGO_URI)
    db = client[DB_NAME]
    
    print("🧠 Restoring Memory...")
    async for doc in db[COLLECTION_NAME].find({}, {"url": 1}):
        seen_urls.add(doc['url'])
    print(f"✅ {len(seen_urls)} links restored.")

    async with aiohttp.ClientSession() as session:
        await queue.put(START_URL)
        while not queue.empty():
            await process_url(session, db, await queue.get())
            await asyncio.sleep(CRAWL_DELAY)

if __name__ == "__main__":
    asyncio.run(main())
