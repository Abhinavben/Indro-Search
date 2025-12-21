import asyncio
import aiohttp
import json
import io
import os
from aiohttp import FormData
from motor.motor_asyncio import AsyncIOMotorClient
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

# --- CONFIG (Render Groups se aayega) ---
MONGO_URI = os.getenv("MONGO_URI")
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")

DB_NAME = "render_data"
COLLECTION_NAME = "web_pages_v2"
MAX_QUEUE_SIZE = 100 
CRAWL_DELAY = 4 

seen_urls = set()
queue = asyncio.Queue()

async def backup_to_telegram(session, url, data):
    try:
        json_str = json.dumps(data, indent=2, ensure_ascii=False)
        file_obj = io.BytesIO(json_str.encode('utf-8', errors='ignore'))
        file_obj.name = f"indro_data.json"
        form = FormData()
        form.add_field('chat_id', TG_CHAT_ID)
        form.add_field('document', file_obj)
        async with session.post(f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendDocument", data=form) as resp:
            return resp.status == 200
    except: return False

async def process_url(session, db, url):
    if url in seen_urls: return
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        async with session.get(url, timeout=20, headers=headers) as response:
            if response.status != 200: return
            
            # Binary read to prevent encoding errors
            raw = await response.read()
            html_text = raw.decode('utf-8', errors='ignore')
            
            soup = BeautifulSoup(html_text, 'html.parser')
            title = soup.title.string.strip() if soup.title else "No Title"
            
            # Backup & Save
            await backup_to_telegram(session, url, {"url": url, "title": title, "text": soup.get_text()[:5000]})
            await db[COLLECTION_NAME].update_one({"url": url}, {"$set": {"url": url, "title": title}}, upsert=True)
            
            seen_urls.add(url)
            print(f"✅ Safe Indexed: {title[:30]}")

            if queue.qsize() < MAX_QUEUE_SIZE:
                for a in soup.find_all('a', href=True):
                    link = urljoin(url, a['href'])
                    if urlparse(link).netloc == urlparse(url).netloc:
                        if link not in seen_urls: await queue.put(link)
    except Exception as e:
        print(f"⚠️ Skipping {url} | Reason: {e}")

async def main():
    if not MONGO_URI: return
    client = AsyncIOMotorClient(MONGO_URI)
    db = client[DB_NAME]
    
    # 305 links load honge yahan
    async for doc in db[COLLECTION_NAME].find({}, {"url": 1}):
        seen_urls.add(doc['url'])
    print(f"🧠 {len(seen_urls)} links restored from DB.")

    async with aiohttp.ClientSession() as session:
        await queue.put("https://www.isro.gov.in/")
        while not queue.empty():
            await process_url(session, db, await queue.get())
            await asyncio.sleep(CRAWL_DELAY)

if __name__ == "__main__":
    asyncio.run(main())
