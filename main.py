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

# Safety Settings
DB_NAME = "render_data"
COLLECTION_NAME = "web_pages_v2"
MAX_QUEUE_SIZE = 100 # RAM safe limit
CRAWL_DELAY = 3      # 3 seconds ka safe gap (Sarkari site block nahi karegi)
START_URL = "https://www.isro.gov.in/"

seen_urls = set()
queue = asyncio.Queue()

# --- 2. TELEGRAM BACKUP ---
async def backup_to_telegram(session, url, data):
    try:
        json_bytes = json.dumps(data, indent=2, ensure_ascii=False).encode('utf-8')
        file_obj = io.BytesIO(json_bytes)
        file_obj.name = f"indro_safe_{str(asyncio.get_event_loop().time()).replace('.', '')}.json"

        form = FormData()
        form.add_field('chat_id', TG_CHAT_ID)
        form.add_field('caption', f"🛡️ **Safe Indexing:**\n🔗 {url}")
        form.add_field('document', file_obj)

        tg_url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendDocument"
        async with session.post(tg_url, data=form) as resp:
            return resp.status == 200
    except:
        return False

# --- 3. CRAWLER LOGIC ---
async def process_url(session, db, url):
    if url in seen_urls: return
    try:
        # User-Agent add kiya hai taaki hum "Real Browser" lagein
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        
        async with session.get(url, timeout=15, headers=headers) as response:
            if response.status != 200: return
            
            soup = BeautifulSoup(await response.text(), 'html.parser')
            title = soup.title.string if soup.title else "No Title"
            
            # Heavy content for Telegram
            full_info = {
                "url": url,
                "title": title,
                "text": soup.get_text(separator=' ', strip=True)[:8000]
            }
            await backup_to_telegram(session, url, full_info)

            # Mongo save (Lightweight)
            await db[COLLECTION_NAME].update_one(
                {"url": url}, 
                {"$set": {"url": url, "title": title, "safe_indexed": True}}, 
                upsert=True
            )
            seen_urls.add(url)
            print(f"🛡️ Safe Indexed: {title[:30]}...")

            # Links collection (No Jumping - Same Domain Only)
            if queue.qsize() < MAX_QUEUE_SIZE:
                for a in soup.find_all('a', href=True):
                    next_url = urljoin(url, a['href'])
                    if urlparse(next_url).netloc == urlparse(url).netloc:
                        if next_url not in seen_urls: await queue.put(next_url)
    except Exception as e:
        print(f"⚠️ Skipping {url} due to error.")

# --- 4. ENGINE START ---
async def main():
    if not MONGO_URI or not TG_BOT_TOKEN:
        print("❌ CRITICAL ERROR: Environment variables missing!")
        return

    client = AsyncIOMotorClient(MONGO_URI)
    db = client[DB_NAME]
    
    print("🧠 Loading Memory...")
    async for doc in db[COLLECTION_NAME].find({}, {"url": 1}):
        seen_urls.add(doc['url'])
    
    async with aiohttp.ClientSession() as session:
        await queue.put(START_URL)
        while not queue.empty():
            current_target = await queue.get()
            await process_url(session, db, current_target)
            # Yahan hai asli safety gap
            await asyncio.sleep(CRAWL_DELAY)

if __name__ == "__main__":
    asyncio.run(main())
