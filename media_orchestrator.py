"""
Project: Gemini-Powered Media Pipeline (Serverless Prowlarr Edition)
Sources: YTS + The Pirate Bay + SolidTorrents
Feature: Auto-Tracker Injection for Max Speed
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
from typing import List, Dict, Optional, Tuple

import google.generativeai as genai
from tmdbv3api import TMDb, Movie, TV
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Config:
    TMDB_KEY = os.environ.get('TMDB_API_KEY')
    GEMINI_KEY = os.environ.get('GEMINI_API_KEY')
    RD_KEY = os.environ.get('REAL_DEBRID_API_KEY')
    # YouTube Auth
    YT_CLIENT_ID = os.environ.get('YOUTUBE_CLIENT_ID')
    YT_CLIENT_SECRET = os.environ.get('YOUTUBE_CLIENT_SECRET')
    YT_REFRESH_TOKEN = os.environ.get('YOUTUBE_REFRESH_TOKEN')

    # List of open trackers to boost speed
    TRACKERS = [
        "udp://tracker.opentrackr.org:1337/announce",
        "udp://open.demonii.com:1337/announce",
        "udp://tracker.coppersurfer.tk:6969/announce",
        "udp://tracker.leechers-paradise.org:6969/announce",
        "udp://9.rarbg.to:2710/announce",
        "udp://tracker.openbittorrent.com:80/announce"
    ]

class MagnetUtils:
    @staticmethod
    def boost_speed(magnet: str) -> str:
        """Appends open trackers to the magnet link to find more seeders."""
        if "&tr=" in magnet: return magnet
        trackers = "&tr=".join([urllib.parse.quote(t) for t in Config.TRACKERS])
        return f"{magnet}&tr={trackers}"

class GeminiBrain:
    def __init__(self):
        genai.configure(api_key=Config.GEMINI_KEY)
        self.model = genai.GenerativeModel('gemini-2.0-flash')

    def select_best_source(self, candidates: List[Dict], media_type: str) -> Optional[Dict]:
        if not candidates: return None
        # Filter: Remove results with 0 seeds to save tokens
        viable = [c for c in candidates if c['seeds'] > 0]
        if not viable: return None
        
        # Sort by seeds and take top 20
        top_candidates = sorted(viable, key=lambda x: x['seeds'], reverse=True)[:20]
        
        prompt = (
            f"Role: Expert Media Curator.\n"
            f"Goal: Select the best torrent for a '{media_type}'.\n"
            f"Criteria:\n"
            f"1. QUALITY: Prioritize '1080p', '4k', 'BluRay'. Avoid 'CAM', 'TS'.\n"
            f"2. SPEED: High seed count is critical.\n"
            f"3. MATCH: Ensure it matches the request (Season packs vs Episodes).\n"
            f"Candidates: {json.dumps(top_candidates)}\n"
            f"Return JSON ONLY: {{'index': <int>, 'reason': '<string>'}}"
        )
        try:
            response = self.model.generate_content(prompt)
            text = response.text.replace('```json', '').replace('```', '').strip()
            data = json.loads(text)
            idx = data.get('index', 0)
            choice = top_candidates[idx]
            logger.info(f"Gemini Selected: {choice['title']} | Reason: {data.get('reason')}")
            return choice
        except Exception as e:
            logger.error(f"AI Selection failed: {e}. Defaulting to highest seeder.")
            return top_candidates[0]

    def generate_metadata(self, title: str, overview: str) -> Tuple[str, str]:
        prompt = f"Create a viral YouTube Title and SEO Description for '{title}'. Plot: {overview}. Return JSON {{'title': '...', 'description': '...'}}"
        try:
            res = self.model.generate_content(prompt)
            data = json.loads(res.text.replace('```json', '').replace('```', '').strip())
            return data['title'], data['description']
        except:
            return title, overview

class Aggregator:
    """The 'Serverless Prowlarr' - Queries multiple public APIs"""
    
    @staticmethod
    def search_yts(query: str) -> List[Dict]:
        """Source 1: YTS (Excellent for Movies)"""
        try:
            url = f"https://yts.mx/api/v2/list_movies.json?query_term={query}&sort_by=seeds"
            data = requests.get(url, timeout=5).json()
            movies = data.get('data', {}).get('movies', [])
            results = []
            if movies:
                for m in movies:
                    for t in m.get('torrents', []):
                        results.append({
                            "title": f"YTS: {m['title']} ({t['quality']})",
                            "seeds": t['seeds'],
                            "size": t['size'],
                            "magnet": MagnetUtils.boost_speed(f"magnet:?xt=urn:btih:{t['hash']}&dn={urllib.parse.quote(m['title'])}"),
                            "source": "YTS"
                        })
            return results
        except:
            return []

    @staticmethod
    def search_apibay(query: str) -> List[Dict]:
        """Source 2: The Pirate Bay (Huge Database for Series)"""
        try:
            url = "https://apibay.org/q.php"
            params = {"q": query, "cat": ""} 
            data = requests.get(url, params=params, timeout=10).json()
            
            results = []
            if data and isinstance(data, list) and data[0].get('name') != 'No results returned':
                for item in data:
                    if int(item['seeders']) == 0: continue
                    results.append({
                        "title": item['name'],
                        "seeds": int(item['seeders']),
                        "size": f"{int(item['size']) / (1024*1024):.1f} MB",
                        "magnet": MagnetUtils.boost_speed(f"magnet:?xt=urn:btih:{item['info_hash']}&dn={urllib.parse.quote(item['name'])}"),
                        "source": "TPB"
                    })
            return results
        except Exception as e:
            logger.error(f"TPB Error: {e}")
            return []
            
    @staticmethod
    def search_solid(query: str) -> List[Dict]:
        """Source 3: SolidTorrents (Aggregator of DHT/Other Sites)"""
        try:
            url = "https://solidtorrents.to/api/v1/search"
            params = {"q": query, "sort": "seeders"}
            headers = {'User-Agent': 'Mozilla/5.0'}
            data = requests.get(url, params=params, headers=headers, timeout=10).json()
            
            results = []
            for item in data.get('results', []):
                if item['swarm']['seeders'] == 0: continue
                results.append({
                    "title": item['title'],
                    "seeds": item['swarm']['seeders'],
                    "size": f"{item['size'] / (1024*1024):.1f} MB",
                    "magnet": MagnetUtils.boost_speed(item['magnet']),
                    "source": "SolidTorrents"
                })
            return results
        except:
            return []

class RealDebrid:
    def __init__(self):
        self.api_key = Config.RD_KEY
        self.base = "https://api.real-debrid.com/rest/1.0"
        self.headers = {"Authorization": f"Bearer {Config.RD_KEY}"}

    def download(self, magnet: str, path: str) -> Optional[str]:
        if not self.api_key: return None
        try:
            # 1. Add Magnet
            r = requests.post(f"{self.base}/torrents/addMagnet", data={'magnet': magnet}, headers=self.headers).json()
            if 'error' in r: raise Exception(r['error'])
            tid = r['id']

            # 2. Select Files (All)
            requests.post(f"{self.base}/torrents/selectFiles/{tid}", data={'files': 'all'}, headers=self.headers)

            # 3. Wait for Cache
            logger.info("Verifying Real-Debrid Cache...")
            for _ in range(15): # Wait max 30s
                info = requests.get(f"{self.base}/torrents/info/{tid}", headers=self.headers).json()
                if info['status'] == 'downloaded': break
                if info['status'] == 'magnet_error': raise Exception("Invalid Magnet")
                time.sleep(2)
            
            if info['status'] != 'downloaded':
                logger.warning("Torrent NOT cached on RD. Downloading would be too slow.")
                return None

            # 4. Download Largest Video File
            video_files = [f for f in info['files'] if f['path'].lower().endswith(('.mp4', '.mkv', '.avi'))]
            if not video_files: raise Exception("No video files found")
            
            # Sort by size to get main movie/episode
            largest = sorted(video_files, key=lambda x: x['bytes'], reverse=True)[0]
            link_to_unrestrict = info['links'][info['files'].index(largest) if len(info['links']) == len(info['files']) else 0]
            
            # Unrestrict
            unrestrict = requests.post(f"{self.base}/unrestrict/link", data={'link': link_to_unrestrict}, headers=self.headers).json()
            
            # Stream Download
            save_path = os.path.join(path, unrestrict['filename'])
            logger.info(f"Downloading: {unrestrict['filename']} ({int(unrestrict['filesize']/1024/1024)} MB)")
            
            with requests.get(unrestrict['download'], stream=True) as r:
                r.raise_for_status()
                with open(save_path, 'wb') as f:
                    shutil.copyfileobj(r.raw, f)
            return save_path

        except Exception as e:
            logger.error(f"RD Download Error: {e}")
            return None

class Orchestrator:
    def __init__(self):
        self.brain = GeminiBrain()
        self.rd = RealDebrid()
        self.tmdb = TMDb()
        self.tmdb.api_key = Config.TMDB_KEY
        
        if Config.YT_REFRESH_TOKEN:
            info = {"client_id": Config.YT_CLIENT_ID, "client_secret": Config.YT_CLIENT_SECRET, "refresh_token": Config.YT_REFRESH_TOKEN, "type": "authorized_user"}
            self.yt = build('youtube', 'v3', credentials=Credentials.from_authorized_user_info(info))
        else:
            self.yt = None

    def run(self, media_name: str):
        logger.info(f"=== Starting Pipeline for: {media_name} ===")
        
        # 1. Gather Metadata
        is_series = "S0" in media_name.upper() or "E0" in media_name.upper()
        media_type = "Series" if is_series else "Movie"
        
        # 2. AGGREGATED SEARCH
        logger.info("Scraping [YTS, PirateBay, SolidTorrents]...")
        candidates = []
        candidates.extend(Aggregator.search_yts(media_name))
        candidates.extend(Aggregator.search_apibay(media_name))
        candidates.extend(Aggregator.search_solid(media_name))
        
        logger.info(f"Found {len(candidates)} total results.")
        
        # 3. AI SELECTION
        best_source = self.brain.select_best_source(candidates, media_type)
        if not best_source: 
            logger.error("No suitable sources found.")
            return

        # 4. DOWNLOAD
        os.makedirs("downloads", exist_ok=True)
        file_path = self.rd.download(best_source['magnet'], "downloads")
        
        if file_path and self.yt:
            # 5. UPLOAD (Optional)
            t, d = self.brain.generate_metadata(best_source['title'], "Uploaded via Gemini Pipeline")
            self.upload_youtube(file_path, t, d)
            
    def upload_youtube(self, path, title, desc):
        logger.info(f"Uploading: {title}")
        body = {'snippet': {'title': title[:100], 'description': desc, 'categoryId': '24'}, 'status': {'privacyStatus': 'private'}}
        media = MediaFileUpload(path, resumable=True)
        self.yt.videos().insert(part='snippet,status', body=body, media_body=media).execute()
        logger.info("Upload Success!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--media", required=True)
    args = parser.parse_args()
    Orchestrator().run(args.media)
