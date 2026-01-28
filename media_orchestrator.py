"""
Project: Gemini-Powered Media Pipeline (Production Fix)
Sources: YTS (Fixed) + TPB (Safe) + BitSearch (New)
"""

import os
import sys
import argparse
import json
import time
import logging
import requests
import shutil
import urllib.parse
import re
from typing import List, Dict, Optional, Tuple

import google.generativeai as genai
from tmdbv3api import TMDb, Movie, TV
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials

# --- Configuration ---
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

class Config:
    TMDB_KEY = os.environ.get('TMDB_API_KEY')
    GEMINI_KEY = os.environ.get('GEMINI_API_KEY')
    RD_KEY = os.environ.get('REAL_DEBRID_API_KEY')
    YT_CLIENT_ID = os.environ.get('YOUTUBE_CLIENT_ID')
    YT_CLIENT_SECRET = os.environ.get('YOUTUBE_CLIENT_SECRET')
    YT_REFRESH_TOKEN = os.environ.get('YOUTUBE_REFRESH_TOKEN')

    # Real Browser Headers to bypass "Bot" blocks
    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    }

class GeminiBrain:
    def __init__(self):
        if not Config.GEMINI_KEY: raise ValueError("GEMINI_API_KEY Missing")
        genai.configure(api_key=Config.GEMINI_KEY)
        self.model = genai.GenerativeModel('gemini-2.0-flash')

    def select_best_source(self, candidates: List[Dict], media_type: str) -> Optional[Dict]:
        if not candidates: return None
        # Deduplicate by title+size
        unique = {f"{c['title']}_{c['size']}": c for c in candidates}.values()
        candidates = list(unique)
        
        # Sort by seeds
        top = sorted(candidates, key=lambda x: x.get('seeds', 0), reverse=True)[:15]
        
        prompt = (
            f"Select the best torrent for {media_type}.\n"
            f"Rules: High seeds = Speed. Avoid CAM/TS. Prefer 1080p/4k.\n"
            f"Candidates: {json.dumps(top)}\n"
            f"Return JSON: {{'index': <int>, 'reason': '...'}}"
        )
        try:
            res = self.model.generate_content(prompt)
            data = json.loads(res.text.replace('```json', '').replace('```', '').strip())
            return top[data.get('index', 0)]
        except:
            return top[0]

    def generate_metadata(self, title: str) -> Tuple[str, str]:
        try:
            res = self.model.generate_content(f"YouTube Title & Description for '{title}'. JSON: {{'title': '...', 'description': '...'}}")
            data = json.loads(res.text.replace('```json', '').replace('```', '').strip())
            return data['title'], data['description']
        except:
            return title, "Uploaded by Gemini Pipeline"

class Aggregator:
    @staticmethod
    def search_yts(query: str) -> List[Dict]:
        """Fixed YTS Search with Params"""
        try:
            url = "https://yts.mx/api/v2/list_movies.json"
            params = {'query_term': query, 'sort_by': 'seeds', 'limit': 10}
            # YTS requires no special headers usually, but we send them anyway
            r = requests.get(url, params=params, headers=Config.HEADERS, timeout=10)
            
            if r.status_code != 200:
                logger.error(f"YTS Failed: {r.status_code}")
                return []
                
            data = r.json()
            if not data.get('data', {}).get('movies'):
                logger.warning(f"YTS found 0 movies for '{query}'")
                return []

            results = []
            for m in data['data']['movies']:
                for t in m.get('torrents', []):
                    results.append({
                        "title": f"YTS: {m['title']} ({t['quality']})",
                        "seeds": t['seeds'],
                        "size": t['size'],
                        "magnet": f"magnet:?xt=urn:btih:{t['hash']}&dn={urllib.parse.quote(m['title'])}",
                        "source": "YTS"
                    })
            return results
        except Exception as e:
            logger.error(f"YTS Error: {e}")
            return []

    @staticmethod
    def search_apibay(query: str) -> List[Dict]:
        """TPB with JSON Error Handling"""
        try:
            url = "https://apibay.org/q.php"
            params = {'q': query, 'cat': ''}
            # APIBay often blocks Data Centers. 
            r = requests.get(url, params=params, headers=Config.HEADERS, timeout=10)
            
            if 'application/json' not in r.headers.get('Content-Type', ''):
                logger.warning(f"TPB returned non-JSON content (Blocked/Down).")
                return []
                
            data = r.json()
            if isinstance(data, list) and data and data[0].get('name') == 'No results returned':
                return []

            results = []
            for item in data:
                if item.get('seeders') == '0': continue
                results.append({
                    "title": item['name'],
                    "seeds": int(item['seeders']),
                    "size": f"{int(item['size'])/1048576:.1f} MB",
                    "magnet": f"magnet:?xt=urn:btih:{item['info_hash']}&dn={urllib.parse.quote(item['name'])}",
                    "source": "TPB"
                })
            return results[:10]
        except Exception as e:
            logger.error(f"TPB Failed: {e}")
            return []

    @staticmethod
    def search_bitsearch(query: str) -> List[Dict]:
        """BitSearch Scraper (No BS4 required - Regex only)"""
        try:
            url = f"https://bitsearch.to/search?q={urllib.parse.quote(query)}"
            r = requests.get(url, headers=Config.HEADERS, timeout=15)
            
            if r.status_code != 200: return []
            
            # Regex to find Magnets and Titles
            # Pattern looks for <h5 class="title">...<a href="magnet:..."
            # This is a rough scraper
            
            # Extract magnet links
            magnets = re.findall(r'href="(magnet:\?xt=urn:btih:[a-zA-Z0-9]+.*?)"', r.text)
            # Extract titles (simplified)
            titles = re.findall(r'class="title".*?>(.*?)</a>', r.text)
            # Extract stats (seeds)
            # This is hard with regex, we assume data is somewhat ordered
            
            results = []
            for i, mag in enumerate(magnets[:10]):
                title = titles[i] if i < len(titles) else "Unknown"
                # Remove HTML tags from title
                title = re.sub(r'<.*?>', '', title)
                results.append({
                    "title": f"BitSearch: {title}",
                    "seeds": 100, # Fake seeds as fallback since regex stats is hard
                    "size": "Unknown",
                    "magnet": mag.replace('&amp;', '&'),
                    "source": "BitSearch"
                })
            return results
        except Exception as e:
            logger.error(f"BitSearch Error: {e}")
            return []

class RealDebrid:
    def __init__(self):
        self.api_key = Config.RD_KEY
        self.base = "https://api.real-debrid.com/rest/1.0"
        self.headers = {"Authorization": f"Bearer {Config.RD_KEY}"}

    def download(self, magnet: str, path: str) -> Optional[str]:
        if not self.api_key: 
            logger.error("No Real-Debrid API Key")
            return None
            
        try:
            # Add Magnet
            r = requests.post(f"{self.base}/torrents/addMagnet", data={'magnet': magnet}, headers=self.headers).json()
            if 'error' in r: 
                logger.error(f"RD Add Error: {r['error']}")
                return None
            tid = r['id']

            # Select Files
            requests.post(f"{self.base}/torrents/selectFiles/{tid}", data={'files': 'all'}, headers=self.headers)

            # Wait for Cache (Max 20s)
            logger.info("Checking Real-Debrid Cache...")
            for _ in range(10):
                info = requests.get(f"{self.base}/torrents/info/{tid}", headers=self.headers).json()
                if info['status'] == 'downloaded': break
                time.sleep(2)
            
            if info['status'] != 'downloaded':
                logger.error("Torrent NOT cached on Real-Debrid. Aborting to save time.")
                return None

            # Get Link
            link = info['links'][0]
            unrestrict = requests.post(f"{self.base}/unrestrict/link", data={'link': link}, headers=self.headers).json()
            
            # Download
            dest = os.path.join(path, unrestrict['filename'])
            logger.info(f"Downloading: {unrestrict['filename']}")
            with requests.get(unrestrict['download'], stream=True) as r:
                r.raise_for_status()
                with open(dest, 'wb') as f:
                    shutil.copyfileobj(r.raw, f)
            return dest
        except Exception as e:
            logger.error(f"RD Download Failed: {e}")
            return None

class Orchestrator:
    def __init__(self):
        self.brain = GeminiBrain()
        self.rd = RealDebrid()
        if Config.YT_REFRESH_TOKEN:
            info = {"client_id": Config.YT_CLIENT_ID, "client_secret": Config.YT_CLIENT_SECRET, "refresh_token": Config.YT_REFRESH_TOKEN, "type": "authorized_user"}
            self.yt = build('youtube', 'v3', credentials=Credentials.from_authorized_user_info(info))

    def run(self, media_name: str):
        logger.info(f"=== Search: {media_name} ===")
        
        # 1. Scrape All Sources
        candidates = []
        candidates += Aggregator.search_yts(media_name)
        candidates += Aggregator.search_apibay(media_name)
        if not candidates:
             # Try BitSearch as fallback if others fail
             candidates += Aggregator.search_bitsearch(media_name)
             
        logger.info(f"Found {len(candidates)} candidates.")
        
        if not candidates:
            logger.error("No results found. APIs might be blocking IP.")
            return

        # 2. Select
        best = self.brain.select_best_source(candidates, "Movie/Series")
        logger.info(f"Selected: {best['title']}")

        # 3. Download
        os.makedirs("downloads", exist_ok=True)
        path = self.rd.download(best['magnet'], "downloads")
        
        # 4. Upload (if downloaded)
        if path and hasattr(self, 'yt'):
            title, desc = self.brain.generate_metadata(best['title'])
            self.upload(path, title, desc)

    def upload(self, path, title, desc):
        logger.info(f"Uploading to YouTube: {title}")
        body = {'snippet': {'title': title[:100], 'description': desc, 'categoryId': '24'}, 'status': {'privacyStatus': 'private'}}
        media = MediaFileUpload(path, resumable=True)
        self.yt.videos().insert(part='snippet,status', body=body, media_body=media).execute()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--media", required=True)
    args = parser.parse_args()
    Orchestrator().run(args.media)
