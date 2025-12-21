import asyncio
import aiohttp
import json
import io
import os
from aiohttp import FormData
from motor.motor_asyncio import AsyncIOMotorClient
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

# --- 1. CONFIGURATION (Render Environment Group se aayega) ---
MONGO_URI = os.getenv("MONGO_URI")
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")

# Settings
DB_NAME = "render_data"
COLLECTION_NAME = "web_pages_v2"
MAX_QUEUE_SIZE = 300 
START_URL = "https://www.isro.gov.in/"

seen_urls = set()
queue = asyncio.Queue()

# --- 2. TELEGRAM BACKUP ---
async def backup_to_telegram(session, url, data):
    try:
        json_bytes = json.dumps(data, indent=2, ensure_ascii=False).encode('utf-8')
        file_obj = io.BytesIO(json_bytes)
        file_obj.name = f"indro_file_{str(asyncio.get_event_loop().time()).replace('.', '')}.json"

        form = FormData()
        form.add_field('chat_id', TG_CHAT_ID)
        form.add_field('caption', f"🕷️ **Indro Data Captured:**\n🔗 {url}")
        form.add_field('document', file_obj)

        tg_api = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendDocument"
        async with session.post(tg_api, data=form) as resp:
            return resp.status == 200
    except:
        return False

# --- 3. CRAWLER PROCESS ---
async def process_url(session, db, url):
    if url in seen_urls: return
    try:
        async with session.get(url, timeout=15) as response:
            if response.status != 200: return
            soup = BeautifulSoup(await response.text(), 'html.parser')
            title = soup.title.string if soup.title else "No Title"
            
            # Content for Telegram
            full_info = {
                "url": url,
                "title": title,
                "text": soup.get_text(separator=' ', strip=True)[:10000]
            }
            await backup_to_telegram(session, url, full_info)

            # Mongo save (Lightweight)
            await db[COLLECTION_NAME].update_one(
                {"url": url}, 
                {"$set": {"url": url, "title": title, "indexed": True}}, 
                upsert=True
            )
            seen_urls.add(url)
            print(f"✅ Indexed: {title[:35]}")

            if queue.qsize() < MAX_QUEUE_SIZE:
                for link in soup.find_all('a', href=True):
                    next_url = urljoin(url, link['href'])
                    if urlparse(next_url).netloc == urlparse(url).netloc:
                        if next_url not in seen_urls: await queue.put(next_url)
    except Exception as e:
        print(f"❌ Error: {e}")

# --- 4. ENGINE START ---
async def main():
    if not MONGO_URI or not TG_BOT_TOKEN:
        print("❌ CRITICAL ERROR: Variables not found! Check Render Settings.")
        return

    client = AsyncIOMotorClient(MONGO_URI)
    db = client[DB_NAME]
    
    print("🧠 Memory Loading from MongoDB...")
    async for doc in db[COLLECTION_NAME].find({}, {"url": 1}):
        seen_urls.add(doc['url'])
    print(f"✅ {len(seen_urls)} links in memory.")

    async with aiohttp.ClientSession() as session:
        await queue.put(START_URL)
        while not queue.empty():
            await process_url(session, db, await queue.get())
            await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())
