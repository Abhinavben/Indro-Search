import asyncio
import aiohttp
import json
import io
import os
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from aiohttp import FormData
from motor.motor_asyncio import AsyncIOMotorClient
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

# --- 1. RENDER PORT FIX (Iske bina Render bot ko band kar deta hai) ---
def run_fake_server():
    class SimpleHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"Indro Engine is Running...")
        def do_HEAD(self):
            self.send_response(200)
            self.end_headers()

    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(('0.0.0.0', port), SimpleHandler)
    server.serve_forever()

# --- 2. CONFIGURATION ---
MONGO_URI = os.getenv("MONGO_URI")
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")

DB_NAME = "render_data"
COLLECTION_NAME = "web_pages_v2"
MAX_QUEUE_SIZE = 100 
CRAWL_DELAY = 10      # High Safety: Har 10 second mein 1 page (Sarkari site ke liye best)
START_URL = "https://www.isro.gov.in/"

seen_urls = set()
queue = asyncio.Queue()

# --- 3. TELEGRAM BACKUP ---
async def backup_to_telegram(session, url, data):
    try:
        json_str = json.dumps(data, indent=2, ensure_ascii=False)
        file_obj = io.BytesIO(json_str.encode('utf-8', errors='ignore'))
        file_obj.name = f"indro_safe_doc.json"
        form = FormData()
        form.add_field('chat_id', TG_CHAT_ID)
        form.add_field('document', file_obj)
        tg_url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendDocument"
        async with session.post(tg_url, data=form) as resp:
            return resp.status == 200
    except: return False

# --- 4. CRAWLER LOGIC ---
async def process_url(session, db, url):
    if url in seen_urls: return
    try:
        # Browser impersonation for high-level safety
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        async with session.get(url, timeout=25, headers=headers) as response:
            if response.status != 200: return
            
            # Binary read + Encoding fix to prevent UTF-8 errors
            raw_content = await response.read()
            html_text = raw_content.decode('utf-8', errors='ignore')
            
            soup = BeautifulSoup(html_text, 'html.parser')
            title = soup.title.string.strip() if soup.title else "No Title"
            
            # Data for Backup
            page_data = {
                "url": url,
                "title": title,
                "text": soup.get_text(separator=' ', strip=True)[:6000]
            }
            
            # Parallel saves
            await backup_to_telegram(session, url, page_data)
            await db[COLLECTION_NAME].update_one({"url": url}, {"$set": {"url": url, "title": title}}, upsert=True)
            
            seen_urls.add(url)
            print(f"🛡️ Safe Indexed: {title[:35]}...")

            # Smart link collection
            if queue.qsize() < MAX_QUEUE_SIZE:
                for a in soup.find_all('a', href=True):
                    link = urljoin(url, a['href'])
                    if urlparse(link).netloc == urlparse(url).netloc:
                        if link not in seen_urls: await queue.put(link)
    except Exception as e:
        print(f"⚠️ Skipping {url} | Reason: {e}")

# --- 5. MAIN ENGINE ---
async def main():
    if not MONGO_URI: 
        print("❌ Error: Env Variables not linked!")
        return

    # Start Fake Web Server for Render
    threading.Thread(target=run_fake_server, daemon=True).start()
    
    client = AsyncIOMotorClient(MONGO_URI)
    db = client[DB_NAME]
    
    # Reload from DB
    async for doc in db[COLLECTION_NAME].find({}, {"url": 1}):
        seen_urls.add(doc['url'])
    print(f"🧠 Memory Loaded: {len(seen_urls)} links.")

    async with aiohttp.ClientSession() as session:
        await queue.put(START_URL)
        while not queue.empty():
            await process_url(session, db, await queue.get())
            # Safety gap
            await asyncio.sleep(CRAWL_DELAY)

if __name__ == "__main__":
    asyncio.run(main())
