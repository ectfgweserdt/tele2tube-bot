"""
Project: Gemini-Powered Media Pipeline (Resilient Edition)
Features: DNS-over-HTTPS (DoH) + SolidTorrents + YTS Mirrors
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
import socket
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
    
    # Standard Browser Headers
    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Referer': 'https://www.google.com/'
    }

class NetworkUtils:
    @staticmethod
    def resolve_doh(domain: str) -> Optional[str]:
        """Resolves Domain via Google DNS-over-HTTPS to bypass local DNS blocks."""
        try:
            url = f"https://dns.google/resolve?name={domain}&type=A"
            r = requests.get(url, timeout=5).json()
            if 'Answer' in r:
                return r['Answer'][0]['data']
        except:
            pass
        return None

    @staticmethod
    def get_with_doh(url: str, params=None) -> requests.Response:
        """Requests a URL using DoH resolved IP if standard DNS fails."""
        try:
            return requests.get(url, params=params, headers=Config.HEADERS, timeout=10)
        except requests.exceptions.ConnectionError:
            # Fallback: Resolve IP manually
            parsed = urllib.parse.urlparse(url)
            ip = NetworkUtils.resolve_doh(parsed.hostname)
            if ip:
                # Construct new URL with IP but keep Host header
                new_url = url.replace(parsed.hostname, ip)
                headers = Config.HEADERS.copy()
                headers['Host'] = parsed.hostname
                return requests.get(new_url, params=params, headers=headers, verify=False, timeout=10)
            raise

class GeminiBrain:
    def __init__(self):
        genai.configure(api_key=Config.GEMINI_KEY)
        self.model = genai.GenerativeModel('gemini-2.0-flash')

    def select_best_source(self, candidates: List[Dict], media_type: str) -> Optional[Dict]:
        if not candidates: return None
        # Basic filtering to save tokens
        clean_candidates = []
        seen = set()
        for c in candidates:
            key = c['title'] + str(c['size'])
            if key not in seen and c['seeds'] > 0:
                clean_candidates.append(c)
                seen.add(key)
        
        # Sort by seeds
        top = sorted(clean_candidates, key=lambda x: x['seeds'], reverse=True)[:10]
        
        prompt = (
            f"Select the best torrent for a {media_type}.\n"
            f"Priorities: 1. Speed (Seeds) 2. Quality (1080p/4k) 3. No CAM/TS.\n"
            f"Candidates: {json.dumps(top)}\n"
            f"Return JSON: {{'index': <int>, 'reason': '...'}}"
        )
        try:
            res = self.model.generate_content(prompt)
            data = json.loads(res.text.replace('```json', '').replace('```', '').strip())
            return top[data.get('index', 0)]
        except:
            return top[0] if top else None

class Aggregator:
    @staticmethod
    def search_solid(query: str) -> List[Dict]:
        """SolidTorrents (Very reliable API)"""
        try:
            url = "https://solidtorrents.to/api/v1/search"
            params = {'q': query, 'sort': 'seeders'}
            r = NetworkUtils.get_with_doh(url, params=params)
            data = r.json()
            
            results = []
            for item in data.get('results', []):
                results.append({
                    "title": item['title'],
                    "seeds": item['swarm']['seeders'],
                    "size": f"{int(item['size'] / 1024 / 1024)} MB",
                    "magnet": item['magnet'],
                    "source": "SolidTorrents"
                })
            return results
        except Exception as e:
            logger.warning(f"SolidTorrents Error: {e}")
            return []

    @staticmethod
    def search_yts_mirrors(query: str) -> List[Dict]:
        """Iterates YTS Mirrors to find one that works"""
        mirrors = ["https://yts.mx", "https://yts.lt", "https://yts.am", "https://yts.ag"]
        for base_url in mirrors:
            try:
                url = f"{base_url}/api/v2/list_movies.json"
                params = {'query_term': query, 'sort_by': 'seeds'}
                r = requests.get(url, params=params, headers=Config.HEADERS, timeout=5)
                if r.status_code == 200:
                    data = r.json()
                    if 'data' in data and 'movies' in data['data']:
                        results = []
                        for m in data['data']['movies']:
                            for t in m.get('torrents', []):
                                results.append({
                                    "title": f"YTS: {m['title']} {t['quality']}",
                                    "seeds": t['seeds'],
                                    "size": t['size'],
                                    "magnet": f"magnet:?xt=urn:btih:{t['hash']}&dn={urllib.parse.quote(m['title'])}",
                                    "source": "YTS"
                                })
                        return results
            except:
                continue
        return []

    @staticmethod
    def search_bitsearch(query: str) -> List[Dict]:
        """BitSearch Scraper (Fallback)"""
        try:
            url = f"https://bitsearch.to/search?q={urllib.parse.quote(query)}"
            r = NetworkUtils.get_with_doh(url)
            # Simple Regex for magnet links
            magnets = re.findall(r'href="(magnet:\?xt=urn:btih:.*?)"', r.text)
            titles = re.findall(r'class="title".*?>(.*?)</a>', r.text)
            
            results = []
            for i, mag in enumerate(magnets[:5]):
                title = titles[i] if i < len(titles) else "Unknown"
                results.append({
                    "title": re.sub(r'<.*?>', '', title),
                    "seeds": 100, # Unknown seeds, assume high
                    "size": "Unknown",
                    "magnet": mag.replace('&amp;', '&'),
                    "source": "BitSearch"
                })
            return results
        except:
            return []

class RealDebrid:
    def __init__(self):
        self.api_key = Config.RD_KEY
        self.base = "https://api.real-debrid.com/rest/1.0"

    def download(self, magnet: str, path: str) -> Optional[str]:
        if not self.api_key: return None
        try:
            headers = {"Authorization": f"Bearer {self.api_key}"}
            # Add Magnet
            r = requests.post(f"{self.base}/torrents/addMagnet", data={'magnet': magnet}, headers=headers).json()
            if 'error' in r: raise Exception(r['error'])
            tid = r['id']
            
            # Select Files
            requests.post(f"{self.base}/torrents/selectFiles/{tid}", data={'files': 'all'}, headers=headers)
            
            # Wait
            logger.info("Verifying Cache...")
            for _ in range(15):
                info = requests.get(f"{self.base}/torrents/info/{tid}", headers=headers).json()
                if info['status'] == 'downloaded': break
                time.sleep(1)
            
            if info['status'] != 'downloaded':
                logger.error("Not cached.")
                return None
                
            # Get Video Link
            vid_files = [f for f in info['files'] if f['path'].lower().endswith(('.mp4', '.mkv'))]
            if not vid_files: return None
            
            # Unrestrict
            link = info['links'][0]
            unrestrict = requests.post(f"{self.base}/unrestrict/link", data={'link': link}, headers=headers).json()
            
            # DL
            dest = os.path.join(path, unrestrict['filename'])
            logger.info(f"Downloading: {unrestrict['filename']}")
            with requests.get(unrestrict['download'], stream=True) as r:
                r.raise_for_status()
                with open(dest, 'wb') as f:
                    shutil.copyfileobj(r.raw, f)
            return dest
        except Exception as e:
            logger.error(f"RD Error: {e}")
            return None

class Orchestrator:
    def __init__(self):
        self.brain = GeminiBrain()
        self.rd = RealDebrid()
        
    def run(self, media_name: str):
        logger.info(f"--- Searching: {media_name} ---")
        
        # 1. Search (Priority Order)
        candidates = []
        candidates += Aggregator.search_solid(media_name)     # Best API
        candidates += Aggregator.search_yts_mirrors(media_name) # Best Movies
        if not candidates:
            candidates += Aggregator.search_bitsearch(media_name) # Backup
            
        logger.info(f"Found {len(candidates)} candidates.")
        
        # 2. Select
        best = self.brain.select_best_source(candidates, "Media")
        if not best:
            logger.error("No suitable sources.")
            return

        # 3. Download
        os.makedirs("downloads", exist_ok=True)
        self.rd.download(best['magnet'], "downloads")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--media", required=True)
    args = parser.parse_args()
    Orchestrator().run(args.media)
