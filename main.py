import os
import asyncio
import aiohttp
import psycopg2
import pymongo
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import datetime
import random
from flask import Flask
from threading import Thread

# --- 1. WEB SERVER (Render ke liye zaroori) ---
app = Flask(__name__)

@app.route('/')
def home():
    return "🚀 Indro Engine is RUNNING on Render!"

def run_web_server():
    # Render PORT environment variable deta hai
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)

# --- 2. CONFIGURATION ---
MAX_CONCURRENT = 5
SEEDS = [
    "https://www.wikipedia.org/",
    "https://www.isro.gov.in/",
    "https://stackoverflow.com/",
    "https://www.bbc.com/news"
]

# --- 3. DATABASE SETUP ---
# (Render ke Environment Variables se password uthayega)
NEON_URL = os.environ.get('NEON_URL')
MONGO_URL = os.environ.get('MONGO_URL')

def save_data(url, title, text):
    if not NEON_URL or not MONGO_URL:
        print("❌ Database Secrets Missing!")
        return

    # Neon (Search)
    try:
        conn = psycopg2.connect(NEON_URL)
        cur = conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS search_index (id SERIAL PRIMARY KEY, url TEXT UNIQUE, title TEXT)")
        cur.execute("INSERT INTO search_index (url, title) VALUES (%s, %s) ON CONFLICT (url) DO NOTHING", (url, title))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Neon Error: {e}")

    # Mongo (Warehouse)
    try:
        client = pymongo.MongoClient(MONGO_URL)
        db = client["indro_warehouse"]
        coll = db["render_data"]
        doc = {
            "url": url, 
            "title": title, 
            "snippet": text[:300], 
            "date": str(datetime.datetime.now())
        }
        coll.update_one({"url": url}, {"$set": doc}, upsert=True)
    except Exception as e:
        print(f"Mongo Error: {e}")

# --- 4. CRAWLER LOGIC ---
visited = set()
queue = asyncio.Queue()

async def worker(session):
    while True:
        url = await queue.get()
        if url in visited:
            queue.task_done()
            continue
        
        visited.add(url)
        print(f"🕷️ Crawling: {url}")
        
        try:
            async with session.get(url, timeout=10) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    title = soup.title.string.strip() if soup.title else "No Title"
                    text = soup.get_text()
                    
                    # Save (Sync func in Async way)
                    save_data(url, title, text)
                    
                    # New Links
                    for link in soup.find_all('a', href=True):
                        full = urljoin(url, link['href'])
                        if full.startswith('http') and full not in visited:
                            await queue.put(full)
        except:
            pass
        
        queue.task_done()
        await asyncio.sleep(1) # Thoda aaram

async def start_crawler():
    for seed in SEEDS:
        await queue.put(seed)
    
    async with aiohttp.ClientSession() as session:
        tasks = []
        for _ in range(MAX_CONCURRENT):
            tasks.append(asyncio.create_task(worker(session)))
        await asyncio.gather(*tasks)

# --- 5. MAIN LAUNCHER ---
def run_bot_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(start_crawler())

if __name__ == "__main__":
    # Server ko alag thread me chalao (Taki Render khush rahe)
    t = Thread(target=run_web_server)
    t.start()
    
    # Bot ko main thread me chalao
    if NEON_URL and MONGO_URL:
        print("✅ Starting Indro Bot on Render...")
        run_bot_loop()
    else:
        print("⚠️ Waiting for Database Secrets...")
