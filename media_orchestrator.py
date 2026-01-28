"""
Project Title: Gemini-Powered Automated Content Sourcing
Version: 3.2 (Production: Real YTS Scraping + Real-Debrid)
"""

import os
import sys
import argparse
import json
import time
import logging
import requests
from typing import List, Dict, Optional, Tuple

import google.generativeai as genai
from tmdbv3api import TMDb, Movie
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials
import ffmpeg

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RealDebridDownloader:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.real-debrid.com/rest/1.0"
        self.headers = {"Authorization": f"Bearer {api_key}"}

    def download(self, magnet: str, target_dir: str) -> Optional[str]:
        if not self.api_key: return None
        try:
            # 1. Add Magnet
            r = requests.post(f"{self.base_url}/torrents/addMagnet", data={'magnet': magnet}, headers=self.headers).json()
            tid = r['id']
            # 2. Select Files
            requests.post(f"{self.base_url}/torrents/selectFiles/{tid}", data={'files': 'all'}, headers=self.headers)
            time.sleep(5) # Wait for RD to process
            # 3. Get Link
            info = requests.get(f"{self.base_url}/torrents/info/{tid}", headers=self.headers).json()
            if not info['links']: return None
            # 4. Unrestrict
            unrestrict = requests.post(f"{self.base_url}/unrestrict/link", data={'link': info['links'][0]}, headers=self.headers).json()
            # 5. Download
            path = os.path.join(target_dir, unrestrict['filename'])
            logger.info(f"Downloading real media: {unrestrict['filename']}")
            with requests.get(unrestrict['download'], stream=True) as dl:
                with open(path, 'wb') as f:
                    shutil.copyfileobj(dl.raw, f)
            return path
        except Exception as e:
            logger.error(f"RD Download Failed: {e}")
            return None

class YTSScraper:
    """Searches YTS.mx for real 1080p magnets."""
    @staticmethod
    def search(query: str) -> List[Dict]:
        url = f"https://yts.mx/api/v2/list_movies.json?query_term={query}&quality=1080p"
        try:
            data = requests.get(url).json()
            movies = data.get('data', {}).get('movies', [])
            results = []
            for m in movies:
                for t in m.get('torrents', []):
                    results.append({
                        "title": f"{m['title']} ({m['year']}) [{t['quality']}]",
                        "size": t['size'],
                        "magnet": f"magnet:?xt=urn:btih:{t['hash']}&dn={m['title_long']}"
                    })
            return results
        except:
            return []

class Orchestrator:
    def __init__(self):
        self.brain = None # Init later with key
        self.rd = RealDebridDownloader(os.environ.get('REAL_DEBRID_API_KEY'))
        self.tmdb = TMDb()
        self.tmdb.api_key = os.environ.get('TMDB_API_KEY')
        
    def run(self, media_name: str):
        # 1. Search for real magnet
        logger.info(f"Searching YTS for: {media_name}")
        candidates = YTSScraper.search(media_name)
        
        if not candidates:
            logger.error("No real torrents found. Cannot proceed without media.")
            return

        # 2. Download via Real-Debrid
        os.makedirs("./downloads", exist_ok=True)
        # We take the first 1080p result
        video_file = self.rd.download(candidates[0]['magnet'], "./downloads")
        
        if not video_file:
            logger.warning("Real-Debrid failed or Key missing. Check your RD subscription.")
            # ONLY GENERATE BEEP IF ABSOLUTELY NECESSARY FOR DEBUGGING
            return

        # 3. Handle YouTube Metadata & Upload (Simplified for brevity)
        logger.info(f"Success! Ready to process: {video_file}")
        # Add your YouTube upload logic here...

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--media", required=True)
    Orchestrator().run(parser.parse_args().media)
