import asyncio
import aiohttp
import json
import io
import os
import gc
import psutil
import time
import warnings
import urllib.robotparser
from aiohttp import web, FormData
from motor.motor_asyncio import AsyncIOMotorClient
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
from urllib.parse import urljoin, urlparse

# --- CONFIGURATION ---
MONGO_URI = os.getenv("MONGO_URI")
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")
DB_NAME = "render_data"
COLLECTION_NAME = "web_pages_v3"
QUEUE_COLLECTION = "link_queue"
USER_AGENT = "IndroSearchBot/8.0 (Safe Render Mode)"

# 40 MASTER SITES (Target List)
TARGET_SITES = [
    "https://www.isro.gov.in/", "https://www.nasa.gov/news/", "https://www.bbc.com/news", 
    "https://www.nature.com/", "https://gadgets360.com/", "https://www.indiatoday.in/science",
    "https://www.theverge.com/news", "https://www.reuters.com/", "https://www.ndtv.com/world-news",
    "https://www.cnn.com/world", "https://www.techcrunch.com/", "https://www.wired.com/",
    "https://www.cnet.com/news/", "https://www.bloomberg.com/", "https://www.forbes.com/",
    "https://www.theguardian.com/world", "https://www.nytimes.com/section/world", "https://www.wsj.com/news/world",
    "https://www.sciencedaily.com/", "https://www.space.com/news", "https://www.phys.org/",
    "https://www.britannica.com/", "https://www.nationalgeographic.com/", "https://www.cnbc.com/world-news/",
    "https://www.aljazeera.com/", "https://timesofindia.indiatimes.com/world", "https://www.hindustantimes.com/world-news",
    "https://www.scientificamerican.com/", "https://www.businessinsider.com/", "https://www.economist.com/"
]

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

# --- GLOBAL SAFETY MEMORY ---
robots_cache = {}        # Robots.txt Rules yaad rakhne ke liye
domain_last_visit = {}   # Rate Limiting ke liye

def get_memory_usage():
    process = psutil.Process(os.getpid())
    return round(process.memory_info().rss / (1024 * 1024), 2)

# --- 👮‍♂️ SAFETY CHECKER (New Feature) ---
async def is_safe_to_crawl(session, url):
    try:
        parsed = urlparse(url)
        domain = parsed.netloc

        # 1. Rate Limiting (Politeness)
        # Ek hi domain par har 2 second me sirf 1 baar jao
        last_time = domain_last_visit.get(domain, 0)
        if time.time() - last_time < 2:
            return False # Abhi mat jao, wait karo
        
        domain_last_visit[domain] = time.time()

        # 2. Check Robots.txt Cache
        if domain in robots_cache:
            return robots_cache[domain].can_fetch(USER_AGENT, url)
        
        # 3. Download robots.txt (Agar cache me nahi hai)
        robots_url = f"{parsed.scheme}://{domain}/robots.txt"
        async with session.get(robots_url, timeout=5) as resp:
            if resp.status == 200:
                content = await resp.text()
                rp = urllib.robotparser.RobotFileParser()
                rp.parse(content.splitlines())
                robots_cache[domain] = rp
                return rp.can_fetch(USER_AGENT, url)
            else:
                return True # Robots.txt nahi hai, toh allowed hai
    except:
        return True # Error aye toh safe maan lo

# --- TELEGRAM BACKUP (With Retry) ---
async def backup_to_telegram(session, data):
    json_bytes = json.dumps(data, indent=2, ensure_ascii=False).encode('utf-8')
    filename = f"indro_{data['title'][:10].replace(' ','_')}_{int(time.time())}.json"
    
    # Retry Loop (3 baar koshish karega)
    for attempt in range(3):
        try:
            form = FormData()
            form.add_field('chat_id', str(TG_CHAT_ID))
            form.add_field('document', io.BytesIO(json_bytes), filename=filename)
            
            async with session.post(f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendDocument", data=form, timeout=30) as resp:
                if resp.status == 200:
                    return True # Success
                elif resp.status == 429:
                    await asyncio.sleep(5) # Thoda ruko agar limit cross hui
        except:
            await asyncio.sleep(2)
    return False

# --- FRONTEND UI (Same as before) ---
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="hi">
<head>
    <meta charset="UTF-8">
    <title>Indro Safe Search</title>
    <style>
        body { font-family: sans-serif; text-align: center; padding: 50px; }
        h1 { color: #4285F4; }
    </style>
</head>
<body>
    <h1>🦅 Indro Safe Search Active</h1>
    <p>System Status: 🟢 Protected Mode</p>
</body>
</html>
"""

async def handle_home(request): return web.Response(text=HTML_TEMPLATE, content_type='text/html')
async def handle_search(request):
    query = request.query.get('q', '').lower()
    if not query: return web.json_response([])
    client = AsyncIOMotorClient(MONGO_URI)
    col = client[DB_NAME][COLLECTION_NAME]
    results = []
    cursor = col.find({"$or": [{"title": {"$regex": query, "$options": "i"}}, {"text": {"$regex": query, "$options": "i"}}]}).limit(15)
    async for doc in cursor: results.append({"title": doc.get("title", "No Title"), "url": doc.get("url", "#"), "text": doc.get("text", "")[:180]})
    return web.json_response(results)

# --- SMART CRAWLER ---
async def start_crawling():
    client = AsyncIOMotorClient(MONGO_URI)
    db = client[DB_NAME]
    pages_col = db[COLLECTION_NAME]
    queue_col = db[QUEUE_COLLECTION]
    
    ram_cache = set()
    MAX_RAM_CACHE = 1500 # Limit thoda kam kiya RAM bachane ke liye

    BLACKLIST = ["facebook.com", "twitter.com", "instagram.com", "linkedin.com", "youtube.com", "accounts.google.com"]
    IMPORTANT_KEYWORDS = ["news", "article", "story", "2025", "report", "update", "science", "tech", "india", "ai", "money"]

    async with aiohttp.ClientSession(headers={'User-Agent': USER_AGENT}) as session:
        while True:
            task = await queue_col.find_one_and_delete({})
            if not task:
                print("📭 Queue Empty! Refilling...", flush=True)
                for site in TARGET_SITES: await queue_col.update_one({"url": site}, {"$setOnInsert": {"url": site, "depth": 0}}, upsert=True)
                await asyncio.sleep(30)
                continue

            url, depth = task['url'], task['depth']
            if depth > 3: continue
            if url in ram_cache: continue

            # 🛑 SAFETY CHECK (Sabse Pehle)
            if not await is_safe_to_crawl(session, url):
                print(f"🛑 Skipped (Robots/Limit): {url[:40]}", flush=True)
                continue
            # ---------------------------

            if await pages_col.find_one({"url": url}, {"_id": 1}):
                ram_cache.add(url)
                continue

            try:
                print(f"🔍 [SCAN] RAM:{get_memory_usage()}MB | {url[:40]}...", flush=True)
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    if response.status == 200:
                        html = await response.text(errors='ignore')
                        soup = BeautifulSoup(html, 'html.parser')
                        title = soup.title.string.strip() if soup.title else "No Title"
                        
                        text = soup.get_text(separator=' ', strip=True)[:1500]
                        page_data = {"url": url, "title": title, "text": text, "keywords": IMPORTANT_KEYWORDS}
                        
                        await asyncio.gather(
                            pages_col.update_one({"url": url}, {"$set": page_data}, upsert=True),
                            backup_to_telegram(session, page_data)
                        )
                        print(f"   ✅ Saved: {title[:20]}", flush=True)
                        
                        ram_cache.add(url)

                        saved_links_count = 0
                        for a in soup.find_all('a', href=True):
                            if saved_links_count >= 25: break  # Limit kam kiya taaki CPU na bhare
                            new_link = urljoin(url, a['href'])
                            parsed = urlparse(new_link)
                            
                            if new_link.startswith('http') and not any(b in parsed.netloc for b in BLACKLIST):
                                is_important = any(k in new_link.lower() or k in a.get_text().lower() for k in IMPORTANT_KEYWORDS)
                                if is_important or saved_links_count < 5:
                                    nd = depth + 1 if parsed.netloc == urlparse(url).netloc else 1
                                    if new_link not in ram_cache:
                                        await queue_col.update_one({"url": new_link}, {"$setOnInsert": {"url": new_link, "depth": nd}}, upsert=True)
                                        saved_links_count += 1
                        
                        del soup, html
                        gc.collect() # Har step par safai
                        
                        if len(ram_cache) > MAX_RAM_CACHE:
                            ram_cache.clear()
                            robots_cache.clear() # Rules bhi reset karo RAM bachane ke liye
                            print("🧹 RAM Cache & Rules cleared.", flush=True)

            except Exception: pass
            await asyncio.sleep(1.0) # Thoda saans lene do CPU ko

async def main():
    app = web.Application()
    app.router.add_get('/', handle_home)
    app.router.add_get('/api/search', handle_search)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 10000))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    print(f"🚀 INDRO 8.0 SAFE MODE LIVE", flush=True)
    
    # 512MB me 2 Worker safe hain
    # Isse speed double hogi par RAM limit me rahegi
    asyncio.create_task(start_crawling()) 
    await asyncio.sleep(2)
    asyncio.create_task(start_crawling()) 
    
    while True: await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())
