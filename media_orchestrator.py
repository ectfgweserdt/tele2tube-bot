"""
Project Title: Gemini-Powered Automated Media Pipeline (Re-Engineered)
Version: 4.0 (Prowlarr Integration + Series Support + Robust Error Handling)
"""

import os
import sys
import argparse
import json
import time
import logging
import requests
import shutil
from typing import List, Dict, Optional, Tuple, Any

import google.generativeai as genai
from tmdbv3api import TMDb, Movie, TV
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials

# --- Configuration & Logging ---
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

class Config:
    TMDB_KEY = os.environ.get('TMDB_API_KEY')
    GEMINI_KEY = os.environ.get('GEMINI_API_KEY')
    RD_KEY = os.environ.get('REAL_DEBRID_API_KEY')
    PROWLARR_URL = os.environ.get('PROWLARR_URL', 'http://localhost:9696')
    PROWLARR_KEY = os.environ.get('PROWLARR_API_KEY')
    YT_CLIENT_ID = os.environ.get('YOUTUBE_CLIENT_ID')
    YT_CLIENT_SECRET = os.environ.get('YOUTUBE_CLIENT_SECRET')
    YT_REFRESH_TOKEN = os.environ.get('YOUTUBE_REFRESH_TOKEN')

class GeminiBrain:
    def __init__(self):
        if not Config.GEMINI_KEY: raise ValueError("GEMINI_API_KEY missing.")
        genai.configure(api_key=Config.GEMINI_KEY)
        self.model = genai.GenerativeModel('gemini-2.0-flash') # Updated to latest stable model

    def select_best_source(self, candidates: List[Dict], media_type: str) -> Optional[Dict]:
        if not candidates: return None
        # Cap candidates to avoid token limits
        safe_candidates = candidates[:15]
        
        prompt = (
            f"You are a Quality Control bot. Select the best torrent for a {media_type}.\n"
            f"Priorities: 1. High Seeders (Speed) 2. Resolution (1080p/4k) 3. No excessive size (>20GB is too big).\n"
            f"Input Candidates: {json.dumps(safe_candidates)}\n"
            f"Return ONLY raw JSON: {{'index': <int>, 'reason': '<string>'}}"
        )
        try:
            response = self.model.generate_content(prompt)
            clean_text = response.text.replace('```json', '').replace('```', '').strip()
            data = json.loads(clean_text)
            idx = data.get('index', 0)
            logger.info(f"Gemini Selected: {safe_candidates[idx]['title']} | Reason: {data.get('reason')}")
            return safe_candidates[idx]
        except Exception as e:
            logger.error(f"Brain selection failed: {e}. Defaulting to first result.")
            return candidates[0]

    def generate_metadata(self, info: Dict) -> Tuple[str, str]:
        prompt = (
            f"Write a catchy YouTube Title and SEO Description for: {info['title']}.\n"
            f"Overview: {info['overview']}\n"
            "Return JSON: {'title': '...', 'description': '...'}"
        )
        try:
            res = self.model.generate_content(prompt)
            data = json.loads(res.text.replace('```json', '').replace('```', '').strip())
            return data['title'], data['description']
        except:
            return info['title'], info.get('overview', 'No description.')

class ProwlarrSearcher:
    """Replaces YTSScraper. Aggregates results from ALL trackers configured in Prowlarr."""
    
    @staticmethod
    def search(query: str, category: str = "2000,5000") -> List[Dict]:
        """
        Categories: 2000 (Movies), 5000 (TV).
        """
        if not Config.PROWLARR_URL or not Config.PROWLARR_KEY:
            logger.warning("Prowlarr config missing. Falling back to YTS (Movies only).")
            return ProwlarrSearcher._fallback_yts(query)

        url = f"{Config.PROWLARR_URL}/api/v1/search"
        params = {
            'apikey': Config.PROWLARR_KEY,
            'query': query,
            'categories': category, # Movies & TV
            'type': 'search',
            'limit': 20,
        }
        
        try:
            logger.info(f"Querying Prowlarr: {query} (Cats: {category})")
            res = requests.get(url, params=params, timeout=20)
            res.raise_for_status()
            results = res.json()
            
            # Normalize Data
            normalized = []
            for item in results:
                # Filter out dead torrents
                if item.get('seeders', 0) == 0: continue
                
                normalized.append({
                    "title": item.get('title'),
                    "size": item.get('size'),
                    "seeders": item.get('seeders'),
                    "magnet": item.get('magnetUrl') or item.get('downloadUrl'),
                    "indexer": item.get('indexer')
                })
            
            # Sort by seeders descending
            normalized.sort(key=lambda x: x['seeders'], reverse=True)
            return normalized
            
        except Exception as e:
            logger.error(f"Prowlarr Search Error: {e}")
            return []

    @staticmethod
    def _fallback_yts(query: str) -> List[Dict]:
        # Legacy fallback logic for Movies
        url = f"https://yts.mx/api/v2/list_movies.json?query_term={query}&sort_by=seeds"
        try:
            r = requests.get(url, timeout=10).json()
            movies = r.get('data', {}).get('movies', [])
            results = []
            for m in movies:
                for t in m.get('torrents', []):
                    results.append({
                        "title": f"{m['title']} {t['quality']}",
                        "seeders": t['seeds'],
                        "magnet": f"magnet:?xt=urn:btih:{t['hash']}&dn={m['title']}",
                        "source": "YTS-Fallback"
                    })
            return results
        except:
            return []

class RealDebridDownloader:
    def __init__(self):
        self.api_key = Config.RD_KEY
        self.base_url = "https://api.real-debrid.com/rest/1.0"
        self.headers = {"Authorization": f"Bearer {Config.RD_KEY}"}

    def download(self, magnet: str, target_dir: str) -> List[str]:
        if not self.api_key: return []
        downloaded_files = []
        
        try:
            # 1. Add Magnet
            r = requests.post(f"{self.base_url}/torrents/addMagnet", data={'magnet': magnet}, headers=self.headers).json()
            if 'error' in r: raise Exception(r['error'])
            tid = r['id']
            
            # 2. Get Info to see files
            info = requests.get(f"{self.base_url}/torrents/info/{tid}", headers=self.headers).json()
            
            # 3. Select Files (Naive: Select largest file for movies, or all for series)
            # For stability, we select 'all' to ensure the content is cached
            requests.post(f"{self.base_url}/torrents/selectFiles/{tid}", data={'files': 'all'}, headers=self.headers)
            
            # 4. Loop wait for conversion
            status = "queued"
            while status in ["queued", "downloading", "compressing"]:
                time.sleep(2)
                info = requests.get(f"{self.base_url}/torrents/info/{tid}", headers=self.headers).json()
                status = info['status']
                if status == 'magnet_error': raise Exception("Magnet Error on RD side")

            # 5. Get Unrestricted Links
            if not info.get('links'): raise Exception("No links generated")
            
            # Download Logic: Limit to 3 files to prevent flooding disk
            for link in info['links'][:3]:
                unrestrict = requests.post(f"{self.base_url}/unrestrict/link", data={'link': link}, headers=self.headers).json()
                filename = unrestrict['filename']
                
                # Filter non-video files
                if not any(filename.lower().endswith(ext) for ext in ['.mp4', '.mkv', '.avi']):
                    continue
                    
                path = os.path.join(target_dir, filename)
                logger.info(f"Downloading: {filename} ({int(unrestrict['filesize']/1024/1024)} MB)")
                
                with requests.get(unrestrict['download'], stream=True) as dl:
                    dl.raise_for_status()
                    with open(path, 'wb') as f:
                        shutil.copyfileobj(dl.raw, f)
                downloaded_files.append(path)
                
            return downloaded_files
            
        except Exception as e:
            logger.error(f"RD Error: {e}")
            return []

class Orchestrator:
    def __init__(self):
        self.brain = GeminiBrain()
        self.rd = RealDebridDownloader()
        self.tmdb = TMDb()
        self.tmdb.api_key = Config.TMDB_KEY
        
        if Config.YT_REFRESH_TOKEN:
            self.yt_service = self._get_yt_service()
        else:
            self.yt_service = None

    def _get_yt_service(self):
        info = {
            "client_id": Config.YT_CLIENT_ID,
            "client_secret": Config.YT_CLIENT_SECRET,
            "refresh_token": Config.YT_REFRESH_TOKEN,
            "type": "authorized_user"
        }
        creds = Credentials.from_authorized_user_info(info)
        return build('youtube', 'v3', credentials=creds)

    def run(self, query: str):
        logger.info(f"--- Processing: {query} ---")
        
        # 1. Determine Content Type via TMDB
        # Check Movie first
        movie_search = Movie().search(query)
        tv_search = TV().search(query)
        
        is_movie = True
        media_info = {}
        
        # Simple heuristic: Higher popularity wins
        movie_pop = movie_search[0].popularity if movie_search else 0
        tv_pop = tv_search[0].popularity if tv_search else 0
        
        if tv_pop > movie_pop:
            is_movie = False
            top = tv_search[0]
            media_info = {'title': top.name, 'overview': top.overview, 'year': top.first_air_date[:4]}
            search_query = top.name # For Series, search the name directly
            category_ids = "5000" # TV Category
        else:
            if not movie_search:
                logger.error("Content not found on TMDB")
                return
            top = movie_search[0]
            media_info = {'title': top.title, 'overview': top.overview, 'year': top.release_date[:4]}
            search_query = f"{top.title} {top.release_date[:4]}"
            category_ids = "2000" # Movie Category

        logger.info(f"Identified as: {'Movie' if is_movie else 'Series'} | Title: {media_info['title']}")

        # 2. Search (Prowlarr)
        candidates = ProwlarrSearcher.search(search_query, category=category_ids)
        if not candidates:
            logger.error("No torrents found.")
            return

        # 3. Select Best
        best = self.brain.select_best_source(candidates, 'Movie' if is_movie else 'Series')
        if not best: return

        # 4. Download
        os.makedirs("downloads", exist_ok=True)
        files = self.rd.download(best['magnet'], "downloads")
        
        if not files:
            logger.error("Download failed.")
            return

        # 5. Upload (First file only)
        if self.yt_service and files:
            self.upload_to_youtube(files[0], media_info)

    def upload_to_youtube(self, path: str, meta: Dict):
        title, desc = self.brain.generate_metadata(meta)
        logger.info(f"Uploading {title}...")
        
        body = {
            'snippet': {'title': title[:100], 'description': desc, 'categoryId': '24'},
            'status': {'privacyStatus': 'private'}
        }
        media = MediaFileUpload(path, resumable=True)
        self.yt_service.videos().insert(part='snippet,status', body=body, media_body=media).execute()
        logger.info("Upload Complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--media", required=True, help="Movie or Series name")
    args = parser.parse_args()
    
    Orchestrator().run(args.media)
