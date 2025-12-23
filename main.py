import asyncio
import aiohttp
import json
import io
import os
import gc
import psutil
import warnings
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
USER_AGENT = "IndroSearchBot/7.2 (Unlimited Hybrid Mode)"

# 40 MASTER SITES
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

def get_memory_usage():
    process = psutil.Process(os.getpid())
    return round(process.memory_info().rss / (1024 * 1024), 2)

# --- TELEGRAM BACKUP (UNLIMITED STORAGE) ---
async def backup_to_telegram(session, data):
    try:
        # JSON file memory me banao
        json_bytes = json.dumps(data, indent=2, ensure_ascii=False).encode('utf-8')
        form = FormData()
        form.add_field('chat_id', str(TG_CHAT_ID))
        # Unique Name with Time
        filename = f"indro_{int(asyncio.get_event_loop().time())}.json"
        form.add_field('document', io.BytesIO(json_bytes), filename=filename)
        
        # Telegram API call
        async with session.post(f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendDocument", data=form, timeout=15) as resp:
            return resp.status == 200
    except: 
        return False

# --- FRONTEND UI ---
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="hi">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Indro Search</title>
    <style>
        body { font-family: 'Arial', sans-serif; text-align: center; margin: 0; background-color: #fff; display: flex; flex-direction: column; min-height: 100vh; }
        .main-content { flex: 1; padding-top: 80px; }
        .logo { font-size: 75px; font-weight: bold; margin-bottom: 25px; letter-spacing: -3px; }
        .logo span:nth-child(1) { color: #4285F4; } .logo span:nth-child(2) { color: #EA4335; }
        .logo span:nth-child(3) { color: #FBBC05; } .logo span:nth-child(4) { color: #34A853; }
        .logo span:nth-child(5) { color: #EA4335; }
        .search-box { width: 90%; max-width: 580px; margin: 0 auto; }
        input { width: 100%; padding: 14px 25px; border-radius: 24px; border: 1px solid #dfe1e5; font-size: 16px; outline: none; }
        input:hover { box-shadow: 0 1px 6px rgba(32,33,36,0.28); }
        .buttons { margin-top: 25px; }
        button { background-color: #f8f9fa; border: 1px solid #f8f9fa; border-radius: 4px; padding: 10px 22px; cursor: pointer; color: #3c4043; }
        button:hover { border-color: #dadce0; box-shadow: 0 1px 1px rgba(0,0,0,0.1); }
        #results { text-align: left; max-width: 650px; margin: 40px auto 20px; padding: 0 20px; }
        .result-item { margin-bottom: 28px; }
        .result-title { font-size: 20px; color: #1a0dab; text-decoration: none; display: block; }
        .result-title:hover { text-decoration: underline; }
        .result-url { font-size: 14px; color: #202124; }
        .result-snippet { font-size: 14px; color: #4d5156; margin-top: 6px; }
        footer { background: #f2f2f2; padding: 15px; color: #70757a; font-size: 14px; margin-top: auto; }
    </style>
</head>
<body>
    <div class="main-content">
        <div class="logo"><span>I</span><span>n</span><span>d</span><span>r</span><span>o</span></div>
        <div class="search-box"><input type="text" id="query" placeholder="Search..." onkeydown="if(event.key === 'Enter') doSearch()"></div>
        <div class="buttons"><button onclick="doSearch()">Indro Search</button></div>
        <div id="results"></div>
    </div>
    <footer>Indro Search AI • Unlimited Telegram Cloud Active</footer>
    <script>
        async function doSearch() {
            const query = document.getElementById('query').value;
            if(!query) return;
            const resDiv = document.getElementById('results');
            resDiv.innerHTML = "<p style='text-align:center; margin-top:20px;'>Searching...</p>";
            try {
                const response = await fetch(`/api/search?q=${encodeURIComponent(query)}`);
                const data = await response.json();
                resDiv.innerHTML = "";
                if (data.length === 0) { resDiv.innerHTML = "<p style='text-align:center;'>No results found.</p>"; return; }
                data.forEach(item => {
                    const div = document.createElement('div');
                    div.className = 'result-item';
                    div.innerHTML = `<div class="result-url">${item.url}</div><a href="${item.url}" class="result-title" target="_blank">${item.title}</a><div class="result-snippet">${item.text}...</div>`;
                    resDiv.appendChild(div);
                });
            } catch (e) { resDiv.innerHTML = "<p style='text-align:center; color:red;'>Error.</p>"; }
        }
    </script>
</body>
</html>
"""

# --- WEB HANDLERS ---
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

# --- CRAWLER LOGIC (Hybrid Mode) ---
async def start_crawling():
    client = AsyncIOMotorClient(MONGO_URI)
    db = client[DB_NAME]
    pages_col = db[COLLECTION_NAME]
    queue_col = db[QUEUE_COLLECTION]
    
    BLACKLIST = ["facebook.com", "twitter.com", "instagram.com", "linkedin.com", "youtube.com", "accounts.google.com"]
    IMPORTANT_KEYWORDS = ["news", "article", "story", "2025", "report", "update", "science", "tech", "india", "ai", "money"]

    async with aiohttp.ClientSession(headers={'User-Agent': USER_AGENT}) as session:
        while True:
            task = await queue_col.find_one_and_delete({})
            
            # Queue khali hone par refill
            if not task:
                print("📭 Queue Empty! Refilling...", flush=True)
                for site in TARGET_SITES: 
                    await queue_col.update_one({"url": site}, {"$setOnInsert": {"url": site, "depth": 0}}, upsert=True)
                await asyncio.sleep(30)
                continue

            url, depth = task['url'], task['depth']
            if depth > 3: continue

            try:
                print(f"🔍 [SCAN] RAM:{get_memory_usage()}MB | {url[:40]}...", flush=True)
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    if response.status == 200:
                        html = await response.text(errors='ignore')
                        soup = BeautifulSoup(html, 'html.parser')
                        title = soup.title.string.strip() if soup.title else "No Title"
                        
                        # --- CHANGE: Always update Mongo AND Send to Telegram ---
                        # Hum check hata rahe hain ki "if not in mongo".
                        # Taki wo purane links ko bhi dobara scan karke Telegram bheje.
                        
                        text = soup.get_text(separator=' ', strip=True)[:1500]
                        page_data = {"url": url, "title": title, "text": text, "keywords": IMPORTANT_KEYWORDS}
                        
                        # Gather ensures both happen
                        await asyncio.gather(
                            pages_col.update_one({"url": url}, {"$set": page_data}, upsert=True),
                            backup_to_telegram(session, page_data)
                        )
                        print(f"   [HYBRID] ✅ Updated Mongo + Sent to TG: {title[:15]}", flush=True)

                        # Links logic
                        saved_links_count = 0
                        for a in soup.find_all('a', href=True):
                            if saved_links_count >= 30: break 
                            new_link = urljoin(url, a['href'])
                            parsed = urlparse(new_link)
                            
                            if new_link.startswith('http') and not any(b in parsed.netloc for b in BLACKLIST):
                                is_important = any(k in new_link.lower() or k in a.get_text().lower() for k in IMPORTANT_KEYWORDS)
                                if is_important or saved_links_count < 5:
                                    new_depth = depth + 1 if parsed.netloc == urlparse(url).netloc else 1
                                    await queue_col.update_one({"url": new_link}, {"$setOnInsert": {"url": new_link, "depth": new_depth}}, upsert=True)
                                    saved_links_count += 1
                        
                        del soup, html
                        gc.collect()
            except Exception: pass
            await asyncio.sleep(1.5)

async def main():
    app = web.Application()
    app.router.add_get('/', handle_home)
    app.router.add_get('/api/search', handle_search)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 10000))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    print(f"🚀 INDRO 7.2 HYBRID LIVE (Unlimited Telegram + Search)", flush=True)
    asyncio.create_task(start_crawling()) 
    while True: await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())
