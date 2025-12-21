import asyncio
import aiohttp
import json
import io
import os
from aiohttp import FormData
from motor.motor_asyncio import AsyncIOMotorClient
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

# --- 1. CONFIGURATION (Fetched from Render Environment Groups) ---
MONGO_URI = os.getenv("MONGO_URI")
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")

# Advanced Settings
DB_NAME = "render_data"
COLLECTION_NAME = "web_pages_v2"
MAX_QUEUE_SIZE = 100 # RAM safety
CRAWL_DELAY = 3      # Safe gap (High Level Safety)
START_URL = "https://www.isro.gov.in/"

seen_urls = set()
queue = asyncio.Queue()

# --- 2. TELEGRAM BACKUP (Cloud Storage) ---
async def backup_to_telegram(session, url, data):
    try:
        # JSON formatting
        json_bytes = json.dumps(data, indent=2, ensure_ascii=False).encode('utf-8')
        file_obj = io.BytesIO(json_bytes)
        file_obj.name = f"indro_data_{str(asyncio.get_event_loop().time()).replace('.', '')}.json"

        form = FormData()
        form.add_field('chat_id', TG_CHAT_ID)
        form.add_field('caption', f"🛡️ **Safe Indexing:**\n🔗 {url}")
        form.add_field('document', file_obj)

        tg_url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendDocument"
        async with session.post(tg_url, data=form) as resp:
            return resp.status == 200
    except Exception as e:
        print(f"⚠️ TG Error: {e}")
        return False

# --- 3. THE ENGINE (With Encoding & Error Fixes) ---
async def process_url(session, db, url):
    if url in seen_urls: return
    
    try:
        # Browser impersonation (High Level Safety)
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        async with session.get(url, timeout=20, headers=headers) as response:
            if response.status != 200: return
            
            # Binary read with forced decoding (Encoding Fix)
            raw_content = await response.read()
            html_text = raw_content.decode('utf-8', errors='ignore')
            
            soup = BeautifulSoup(html_text, 'html.parser')
            title = soup.title.string.strip() if soup.title else "No Title"
            
            # Content Extraction
            page_content = {
                "url": url,
                "title": title,
                "text": soup.get_text(separator=' ', strip=True)[:8000], # Safe length
                "domain": urlparse(url).netloc
            }

            # 1. Telegram Backup
            await backup_to_telegram(session, url, page_content)

            # 2. Mongo Lite Save
            await db[COLLECTION_NAME].update_one(
                {"url": url}, 
                {"$set": {"url": url, "title": title, "status": "safe_indexed"}}, 
                upsert=True
            )
            
            seen_urls.add(url)
            print(f"✅ Safe Indexed: {title[:40]}")

            # 3. Smart Link Discovery (Same Domain)
            if queue.qsize() < MAX_QUEUE_SIZE:
                for link in soup.find_all('a', href=True):
                    full_link = urljoin(url, link['href'])
                    if urlparse(full_link).netloc == urlparse(url).netloc:
                        if full_link not in seen_urls:
                            await queue.put(full_link)

    except Exception as e:
        print(f"⚠️ Skipping {url} | Error: {e}")

# --- 4. MAIN RUNNER (Persistence Logic) ---
async def main():
    if not all([MONGO_URI, TG_BOT_TOKEN, TG_CHAT_ID]):
        print("❌ CRITICAL: Missing Env Variables! Check Render Settings.")
        return

    client = AsyncIOMotorClient(MONGO_URI)
    db = client[DB_NAME]
    
    # Reload history
    print("🧠 Memory Loading...")
    async for doc in db[COLLECTION_NAME].find({}, {"url": 1}):
        seen_urls.add(doc['url'])
    print(f"✅ {len(seen_urls)} links restored from DB.")

    async with aiohttp.ClientSession() as session:
        # Start from where we left or START_URL
        if not seen_urls:
            await queue.put(START_URL)
        else:
            await queue.put(list(seen_urls)[-1])

        while not queue.empty():
            current_url = await queue.get()
            await process_url(session, db, current_url)
            # High-level safety delay
            await asyncio.sleep(CRAWL_DELAY)

if __name__ == "__main__":
    asyncio.run(main())
