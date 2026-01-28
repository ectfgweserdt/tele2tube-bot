"""
Project Title: Gemini-Powered Automated Content Sourcing
Version: 3.3 (Production: Robust YTS Search + Gemini Selection + YouTube Upload)
"""

import os
import sys
import argparse
import json
import time
import logging
import requests
import shutil
from typing import List, Dict, Optional, Tuple

import google.generativeai as genai
from tmdbv3api import TMDb, Movie
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials
import ffmpeg

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

class GeminiBrain:
    def __init__(self, api_key: str):
        if not api_key: raise ValueError("GEMINI_API_KEY is missing.")
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel('gemini-2.5-flash-preview-09-2025')

    def select_best_source(self, candidates: List[Dict]) -> Optional[Dict]:
        if not candidates: return None
        prompt = f"From these YTS torrents, select the one with the highest quality/bitrate (usually larger size). Return ONLY JSON: {{'index': <int>, 'reason': '...'}}. Candidates: {json.dumps(candidates)}"
        try:
            response = self.model.generate_content(prompt)
            clean_text = response.text.replace('```json', '').replace('```', '').strip()
            data = json.loads(clean_text)
            idx = data.get('index', 0)
            logger.info(f"Gemini Selection Reason: {data.get('reason')}")
            return candidates[idx]
        except Exception as e:
            logger.error(f"Brain failed to select source: {e}")
            return candidates[0]

    def generate_metadata(self, info: Dict) -> Tuple[str, str]:
        prompt = f"Generate a YouTube Title and SEO Description for: {info['title']}. Plot: {info['overview']}. Return JSON: {{'title': '...', 'description': '...'}}"
        try:
            res = self.model.generate_content(prompt)
            clean_text = res.text.replace('```json', '').replace('```', '').strip()
            data = json.loads(clean_text)
            return data['title'], data['description']
        except:
            return f"{info['title']} Full Movie Overview", info['overview']

class RealDebridDownloader:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.real-debrid.com/rest/1.0"
        self.headers = {"Authorization": f"Bearer {api_key}"}

    def download(self, magnet: str, target_dir: str) -> Optional[str]:
        if not self.api_key: 
            logger.error("REAL_DEBRID_API_KEY is missing from environment.")
            return None
        try:
            # 1. Add Magnet
            r = requests.post(f"{self.base_url}/torrents/addMagnet", data={'magnet': magnet}, headers=self.headers).json()
            tid = r['id']
            # 2. Select Files
            requests.post(f"{self.base_url}/torrents/selectFiles/{tid}", data={'files': 'all'}, headers=self.headers)
            
            # 3. Wait for RD to check cache/process
            logger.info("Checking Real-Debrid cache...")
            time.sleep(5)
            
            # 4. Get Link
            info = requests.get(f"{self.base_url}/torrents/info/{tid}", headers=self.headers).json()
            if not info.get('links'): 
                logger.error("Torrent not cached or no links found.")
                return None
            
            # 5. Unrestrict
            unrestrict = requests.post(f"{self.base_url}/unrestrict/link", data={'link': info['links'][0]}, headers=self.headers).json()
            
            # 6. Download
            path = os.path.join(target_dir, unrestrict['filename'])
            logger.info(f"Downloading: {unrestrict['filename']}")
            with requests.get(unrestrict['download'], stream=True) as dl:
                dl.raise_for_status()
                with open(path, 'wb') as f:
                    shutil.copyfileobj(dl.raw, f)
            return path
        except Exception as e:
            logger.error(f"RD Download Failed: {e}")
            return None

class YTSScraper:
    @staticmethod
    def search(query: str) -> List[Dict]:
        url = f"https://yts.mx/api/v2/list_movies.json?query_term={query.replace(' ', '+')}&sort_by=seeds"
        try:
            response = requests.get(url, timeout=10)
            data = response.json()
            movies = data.get('data', {}).get('movies', [])
            if not movies:
                logger.warning(f"No movies found for query: {query}")
                return []
                
            results = []
            for m in movies:
                for t in m.get('torrents', []):
                    results.append({
                        "title": f"{m['title']} ({m['year']}) [{t['quality']}] [{t['type']}]",
                        "size": t['size'],
                        "size_bytes": t['size_bytes'],
                        "magnet": f"magnet:?xt=urn:btih:{t['hash']}&dn={m['title_long']}"
                    })
            return results
        except Exception as e:
            logger.error(f"YTS Scraper error: {e}")
            return []

class YouTubeBroadcaster:
    def __init__(self, client_info: Dict):
        info = {**client_info, "type": "authorized_user"}
        self.creds = Credentials.from_authorized_user_info(info, scopes=['https://www.googleapis.com/auth/youtube.upload'])
        self.service = build('youtube', 'v3', credentials=self.creds)

    def upload(self, path: str, meta: Dict):
        logger.info(f"Uploading to YouTube: {meta['title']}")
        body = {
            'snippet': {'title': meta['title'][:100], 'description': meta['description'], 'categoryId': '24'},
            'status': {'privacyStatus': 'private', 'selfDeclaredMadeForKids': False}
        }
        media = MediaFileUpload(path, chunksize=1024*1024, resumable=True)
        request = self.service.videos().insert(part='snippet,status', body=body, media_body=media)
        
        response = None
        while response is None:
            status, response = request.next_chunk()
            if status:
                logger.info(f"Progress: {int(status.progress() * 100)}%")
        logger.info(f"Upload Done! ID: {response.get('id')}")

class Orchestrator:
    def __init__(self):
        self.brain = GeminiBrain(os.environ.get('GEMINI_API_KEY'))
        self.rd = RealDebridDownloader(os.environ.get('REAL_DEBRID_API_KEY'))
        self.tmdb = TMDb()
        self.tmdb.api_key = os.environ.get('TMDB_API_KEY')
        
        cid = os.environ.get('YOUTUBE_CLIENT_ID')
        sec = os.environ.get('YOUTUBE_CLIENT_SECRET')
        ref = os.environ.get('YOUTUBE_REFRESH_TOKEN')
        self.yt = YouTubeBroadcaster({'client_id': cid, 'client_secret': sec, 'refresh_token': ref}) if ref else None

    def run(self, media_name: str):
        logger.info(f"--- Starting Pipeline: {media_name} ---")
        
        # 1. TMDB Metadata
        search = Movie().search(media_name)
        if not search:
            logger.error("Could not find media on TMDB.")
            return
        movie_info = {'title': search[0].title, 'overview': search[0].overview, 'year': getattr(search[0], 'release_date', '')[:4]}
        
        # 2. Scrape YTS
        logger.info(f"Searching YTS for: {movie_info['title']} ({movie_info['year']})")
        candidates = YTSScraper.search(f"{movie_info['title']} {movie_info['year']}")
        if not candidates:
            # Try searching just title if title+year fails
            candidates = YTSScraper.search(movie_info['title'])
            
        if not candidates:
            logger.error("No torrents found on YTS.")
            return

        # 3. Gemini Selects Best Quality
        best_source = self.brain.select_best_source(candidates)
        
        # 4. Real-Debrid Download
        os.makedirs("./downloads", exist_ok=True)
        video_path = self.rd.download(best_source['magnet'], "./downloads")
        
        if not video_path:
            logger.error("Failed to download media via Real-Debrid.")
            return

        # 5. YouTube Metadata & Upload
        if self.yt:
            yt_title, yt_desc = self.brain.generate_metadata(movie_info)
            self.yt.upload(video_path, {'title': yt_title, 'description': yt_desc})
        
        logger.info("--- Pipeline Completed Successfully ---")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--media", required=True)
    args = parser.parse_args()
    
    if not all([os.environ.get('TMDB_API_KEY'), os.environ.get('GEMINI_API_KEY'), os.environ.get('REAL_DEBRID_API_KEY')]):
        logger.error("Missing required API keys in environment.")
        sys.exit(1)
        
    Orchestrator().run(args.media)
