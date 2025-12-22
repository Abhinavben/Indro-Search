import asyncio
import aiohttp
import json
import io
import os
import gc
import psutil
import warnings
import re
from aiohttp import web
from motor.motor_asyncio import AsyncIOMotorClient
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
from urllib.parse import urljoin, urlparse

# --- CONFIGURATION ---
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = "render_data"
COLLECTION_NAME = "web_pages_v3"
QUEUE_COLLECTION = "link_queue"
USER_AGENT = "IndroSearchBot/7.0 (Smart AI Mode)"

# 40 MASTER SITES (Jahan se shuruwat hogi)
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
    "https://www.news18.com/world/", "https://www.thehindu.com/news/international/", "https://www.engadget.com/",
    "https://www.gizmodo.com/", "https://www.arstechnica.com/", "https://www.mashable.com/",
    "https://www.scientificamerican.com/", "https://www.businessinsider.com/", "https://www.economist.com/",
    "https://www.ft.com/world", "https://www.latimes.com/world-nation", "https://www.abc.net.au/news/world/",
    "https://www.dw.com/en/world/"
]

# Warning filter
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

def get_memory_usage():
    process = psutil.Process(os.getpid())
    return round(process.memory_info().rss / (1024 * 1024), 2)

# --- FRONTEND UI (Google Style) ---
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
        .logo { font-size: 75px; font-weight: bold; margin-bottom: 25px; letter-spacing: -3px; font-family: 'Product Sans', Arial, sans-serif; }
        .logo span:nth-child(1) { color: #4285F4; } .logo span:nth-child(2) { color: #EA4335; }
        .logo span:nth-child(3) { color: #FBBC05; } .logo span:nth-child(4) { color: #34A853; }
        .logo span:nth-child(5) { color: #EA4335; }
        
        .search-box { width: 90%; max-width: 580px; margin: 0 auto; position: relative; }
        input { width: 100%; padding: 14px 25px; border-radius: 24px; border: 1px solid #dfe1e5; font-size: 16px; outline: none; box-sizing: border-box; transition: box-shadow 0.2s; }
        input:hover, input:focus { box-shadow: 0 1px 6px rgba(32,33,36,0.28); border-color: rgba(223,225,225,0); }
        
        .buttons { margin-top: 25px; }
        button { background-color: #f8f9fa; border: 1px solid #f8f9fa; border-radius: 4px; color: #3c4043; font-size: 14px; margin: 0 6px; padding: 10px 22px; cursor: pointer; transition: all 0.2s; }
        button:hover { border-color: #dadce0; box-shadow: 0 1px 1px rgba(0,0,0,0.1); color: #202124; }
        
        #results { text-align: left; max-width: 650px; margin: 40px auto 20px; padding: 0 20px; }
        .result-item { margin-bottom: 28px; }
        .result-url { font-size: 14px; color: #202124; margin-bottom: 4px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
        .result-title { font-size: 20px; color: #1a0dab; text-decoration: none; display: block; line-height: 1.3; cursor: pointer; }
        .result-title:hover { text-decoration: underline; }
        .result-snippet { font-size: 14px; color: #4d5156; margin-top: 6px; line-height: 1.58; }
        .stats { font-size: 13px; color: #70757a; margin-bottom: 20px; }
        footer { background: #f2f2f2; padding: 15px; color: #70757a; font-size: 14px; margin-top: auto; border-top: 1px solid #dadce0; }
    </style>
</head>
<body>
    <div class="main-content">
        <div class="logo"><span>I</span><span>n</span><span>d</span><span>r</span><span>o</span></div>
        <div class="search-box">
            <input type="text" id="query" placeholder="Search anything..." onkeydown="if(event.key === 'Enter') doSearch()">
        </div>
        <div class="buttons">
            <button onclick="doSearch()">Indro Search</button>
            <button onclick="alert('Indro Bot is crawling the web for you!')">I'm Feeling Lucky</button>
        </div>
        <div id="results"></div>
    </div>
    <footer>
        India • Indro Search AI • <span id="ram-stat">System Active</span>
    </footer>

    <script>
        async function doSearch() {
            const query = document.getElementById('query').value;
            if(!query) return;
            
            const resDiv = document.getElementById('results');
            resDiv.innerHTML = "<p style='text-align:center; color:#70757a; margin-top:20px;'>Searching Indro Database...</p>";
            
            try {
                const response = await fetch(`/api/search?q=${encodeURIComponent(query)}`);
                const data = await response.json();
                
                resDiv.innerHTML = "";
                
                if (data.length === 0) {
                    resDiv.innerHTML = "<p style='text-align:center; margin-top:20px;'>No results found. Try broader keywords.</p>";
                    return;
                }
                
                const stats = document.createElement('div');
                stats.className = 'stats';
                stats.innerText = `About ${data.length} results found`;
                resDiv.appendChild(stats);
                
                data.forEach(item => {
                    const div = document.createElement('div');
                    div.className = 'result-item';
                    div.innerHTML = `
                        <div class="result-url">${item.url}</div>
                        <a href="${item.url}" class="result-title" target="_blank">${item.title}</a>
                        <div class="result-snippet">${item.text}...</div>
                    `;
                    resDiv.appendChild(div);
                });
            } catch (e) {
                resDiv.innerHTML = "<p style='text-align:center; color:red;'>Server Error. Please try again.</p>";
            }
        }
    </script>
</body>
</html>
"""

# --- WEB HANDLERS ---
async def handle_home(request):
    return web.Response(text=HTML_TEMPLATE, content_type='text/html')

async def handle_search(request):
    query = request.query.get('q', '').lower()
    if not query: return web.json_response([])
    
    client = AsyncIOMotorClient(MONGO_URI)
    db = client[DB_NAME]
    col = db[COLLECTION_NAME]
    
    # Regex search (Temporary solution, later use Atlas Search)
    results = []
    # Search in Title OR Text
    cursor = col.find({
        "$or": [
            {"title": {"$regex": query, "$options": "i"}},
            {"text": {"$regex": query, "$options": "i"}}
        ]
    }).limit(15)

    async for doc in cursor:
        results.append({
            "title": doc.get("title", "No Title"),
            "url": doc.get("url", "#"),
            "text": doc.get("text", "")[:180]
        })
    
    return web.json_response(results)

# --- SMART CRAWLER LOGIC ---
async def start_crawling():
    client = AsyncIOMotorClient(MONGO_URI)
    db = client[DB_NAME]
    pages_col = db[COLLECTION_NAME]
    queue_col = db[QUEUE_COLLECTION]
    
    # 1. Blacklist: Faltu sites par RAM waste mat karo
    BLACKLIST = [
        "facebook.com", "twitter.com", "instagram.com", "linkedin.com", "youtube.com", 
        "accounts.google.com", "tiktok.com", "pinterest.com", "reddit.com/login"
    ]
    
    # 2. Keywords: In shabdon wale link ko pehle pakdo
    IMPORTANT_KEYWORDS = [
        "news", "article", "story", "2025", "report", "update", "science", "tech", 
        "research", "india", "space", "future", "ai", "robot", "money", "crypto"
    ]

    async with aiohttp.ClientSession(headers={'User-Agent': USER_AGENT}) as session:
        while True:
            # Queue se link uthao
            task = await queue_col.find_one_and_delete({})
            
            # Agar Queue khali hai, to REFILL karo
            if not task:
                print("📭 Queue Empty! Auto-Refilling Seed Sites in 30s...", flush=True)
                for site in TARGET_SITES:
                    await queue_col.update_one({"url": site}, {"$setOnInsert": {"url": site, "depth": 0}}, upsert=True)
                await asyncio.sleep(30)
                continue

            url, depth = task['url'], task['depth']
            
            # Depth control: Bahut gehra mat jao
            if depth > 3: continue

            try:
                print(f"🔍 [SCAN] RAM:{get_memory_usage()}MB | Depth:{depth} | {url[:50]}...", flush=True)
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    if response.status == 200:
                        html = await response.text(errors='ignore')
                        soup = BeautifulSoup(html, 'html.parser')
                        title = soup.title.string.strip() if soup.title else "No Title"
                        
                        # --- DATA SAVE ---
                        # Sirf naya data save karo
                        if not await pages_col.find_one({"url": url}, {"_id": 1}):
                            text = soup.get_text(separator=' ', strip=True)[:1200]
                            # MongoDB me save
                            await pages_col.update_one(
                                {"url": url}, 
                                {"$set": {"url": url, "title": title, "text": text, "keywords": IMPORTANT_KEYWORDS}}, 
                                upsert=True
                            )
                            print(f"   [SAVE] ✅ {title[:30]}", flush=True)

                        # --- SMART LINK EXTRACTION ---
                        saved_links_count = 0
                        all_links = soup.find_all('a', href=True)
                        
                        # Sare links ko check karo
                        for a in all_links:
                            # RAM Safety: Ek page se max 30 link hi uthao
                            if saved_links_count >= 30: break 
                            
                            new_link = urljoin(url, a['href'])
                            parsed = urlparse(new_link)
                            
                            # Valid link + Not Blacklisted
                            if new_link.startswith('http') and not any(b in parsed.netloc for b in BLACKLIST):
                                
                                # KEYWORD MATCHING
                                link_text = a.get_text().lower()
                                link_url = new_link.lower()
                                is_important = any(k in link_url or k in link_text for k in IMPORTANT_KEYWORDS)
                                
                                # Agar Important hai YA fir shuru ke 5 links hain (taaki chain na toote)
                                if is_important or saved_links_count < 5:
                                    
                                    # Agar nayi website hai (Wikipedia, etc) -> Depth 1 (Reset)
                                    # Agar wahi website hai -> Depth + 1
                                    if parsed.netloc == urlparse(url).netloc:
                                        new_depth = depth + 1
                                    else:
                                        new_depth = 1 # New domain restart logic
                                    
                                    await queue_col.update_one(
                                        {"url": new_link}, 
                                        {"$setOnInsert": {"url": new_link, "depth": new_depth}}, 
                                        upsert=True
                                    )
                                    saved_links_count += 1
                        
                        del soup, html, all_links
                        gc.collect() # RAM saaf karo

            except Exception as e:
                pass # Chupchap aage badho
            
            # Thoda break lo taaki CPU 100% na jaye
            await asyncio.sleep(1.5)

# --- MAIN ENTRY ---
async def main():
    app = web.Application()
    app.router.add_get('/', handle_home)
    app.router.add_get('/api/search', handle_search)
    
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 10000))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    
    print(f"🚀 INDRO SEARCH ENGINE 7.0 LIVE ON PORT {port}", flush=True)
    
    # Background Crawler Start
    asyncio.create_task(start_crawling()) 
    
    # Keep Alive Loop
    while True: await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())
