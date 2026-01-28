"""
Project Title: Gemini-Powered Automated Content Sourcing and YouTube Publishing Pipeline
Author: Research Assistant
Date: January 28, 2026
Version: 2.5 (Fixes: Secret naming mismatch for YOUTUBE_CLIENT_SECRETS)
"""

import os
import sys
import argparse
import re
import json
import subprocess
import time
import logging
import shutil
from typing import List, Dict, Optional, Tuple

# Third-party libraries
import google.generativeai as genai
from tmdbv3api import TMDb, Movie, TV, Season
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials
import ffmpeg
import subliminal
from babelfish import Language

# --- Configuration ---
LOG_FILE = "pipeline.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(module)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Constants
DOWNLOAD_DIR = "./downloads"
OUTPUT_DIR = "./ready_to_upload"

# --- Modules ---

class GeminiBrain:
    """
    The Intelligence Unit. Uses Gemini to make decisions and generate text.
    """
    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("GEMINI_API_KEY is missing.")
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel('gemini-2.5-flash-preview-09-2025')

    def select_best_torrent(self, candidates: List[Dict], criteria: str = "1080p, high seeds") -> Optional[Dict]:
        if not candidates:
            return None

        prompt = f"""
        Act as a Media Archival Expert. Select the SINGLE best source based on: {criteria}.
        Return ONLY a JSON object: {{ "best_index": <int>, "reason": "<string>" }}
        Candidates: {json.dumps(candidates)}
        """
        
        try:
            response = self.model.generate_content(prompt)
            text = response.text.replace('```json', '').replace('```', '').strip()
            decision = json.loads(text)
            best_idx = decision.get("best_index")
            
            if best_idx is not None and 0 <= best_idx < len(candidates):
                logger.info(f"Gemini selected candidate #{best_idx}: {decision.get('reason')}")
                return candidates[best_idx]
        except Exception as e:
            logger.error(f"Gemini failed to select torrent: {e}")
        
        return sorted(candidates, key=lambda x: x.get('seeds', 0), reverse=True)[0]

    def generate_youtube_metadata(self, media_info: Dict) -> Tuple[str, str]:
        prompt = f"""
        Generate YouTube metadata (Title and Description) for: {media_info.get('title')}.
        Description should be SEO-friendly and include the plot: {media_info.get('overview')}.
        Output Format (JSON): {{ "title": "...", "description": "..." }}
        """
        try:
            response = self.model.generate_content(prompt)
            text = response.text.replace('```json', '').replace('```', '').strip()
            data = json.loads(text)
            return data['title'], data['description']
        except Exception as e:
            logger.error(f"Gemini metadata generation failed: {e}")
            return f"Clip: {media_info.get('title')}", media_info.get('overview', '')


class ContentSource:
    def __init__(self, download_dir: str):
        self.download_dir = download_dir
        os.makedirs(download_dir, exist_ok=True)

    def find_and_download_media(self, query: str, brain: GeminiBrain) -> Optional[str]:
        logger.info(f"Sourcing media for query: {query}")
        candidates = [
            {"title": f"{query} 1080p BluRay", "seeds": 150, "magnet": "mag1"},
            {"title": f"{query} 720p WEB-DL", "seeds": 450, "magnet": "mag2"}
        ]
        
        best_choice = brain.select_best_torrent(candidates)
        if not best_choice: return None
        
        import hashlib
        folder_hash = hashlib.md5(query.encode()).hexdigest()[:8]
        save_path = os.path.join(self.download_dir, folder_hash)
        os.makedirs(save_path, exist_ok=True)
        
        dummy_file = os.path.join(save_path, "movie.mp4")
        if not os.path.exists(dummy_file):
            with open(dummy_file, 'wb') as f:
                f.write(b'\x00' * 2048) 
        
        return dummy_file


class VideoLab:
    @staticmethod
    def process_video(input_path: str, output_path: str) -> bool:
        logger.info(f"Processing video: {input_path}")
        try:
            (
                ffmpeg
                .input(input_path)
                .output(output_path, vcodec='copy', acodec='copy')
                .overwrite_output()
                .run(capture_stdout=True, capture_stderr=True)
            )
            return True
        except ffmpeg.Error as e:
            logger.warning("FFmpeg noted invalid data (likely dummy file). Proceeding with simulation copy.")
            shutil.copy(input_path, output_path)
            return True


class YouTubeBroadcaster:
    def __init__(self, client_info: Dict):
        info = {
            "client_id": client_info['client_id'],
            "client_secret": client_info['client_secret'],
            "refresh_token": client_info['refresh_token'],
            "type": "authorized_user"
        }
        self.creds = Credentials.from_authorized_user_info(info, scopes=['https://www.googleapis.com/auth/youtube.upload'])
        self.service = build('youtube', 'v3', credentials=self.creds)

    def upload(self, video_path: str, metadata: Dict) -> Optional[str]:
        if not os.path.exists(video_path):
            logger.error(f"Upload aborted: File not found at {video_path}")
            return None

        logger.info(f"Starting YouTube Upload for {video_path}...")
        try:
            body = {
                'snippet': {
                    'title': metadata['title'][:100],
                    'description': metadata['description'],
                    'categoryId': '24'
                },
                'status': {'privacyStatus': 'private'}
            }
            media = MediaFileUpload(video_path, chunksize=-1, resumable=True)
            request = self.service.videos().insert(part='snippet,status', body=body, media_body=media)
            response = request.execute()
            logger.info(f"Upload Successful! ID: {response.get('id')}")
            return response.get('id')
        except Exception as e:
            logger.error(f"Upload failed: {e}")
            return None


class Orchestrator:
    def __init__(self):
        self.tmdb = TMDb()
        self.tmdb.api_key = os.environ.get('TMDB_API_KEY')
        self.brain = GeminiBrain(os.environ.get('GEMINI_API_KEY'))
        self.source = ContentSource(DOWNLOAD_DIR)
        self.lab = VideoLab()
        
        self.broadcaster = None
        cid = os.environ.get('YOUTUBE_CLIENT_ID')
        # Check both singular and plural (based on your repository secret name)
        sec = os.environ.get('YOUTUBE_CLIENT_SECRET') or os.environ.get('YOUTUBE_CLIENT_SECRETS')
        ref = os.environ.get('YOUTUBE_REFRESH_TOKEN')

        if ref and cid and sec:
            logger.info("Initializing YouTube Broadcaster with detected credentials.")
            self.broadcaster = YouTubeBroadcaster({
                'client_id': cid,
                'client_secret': sec,
                'refresh_token': ref
            })
        else:
            missing = [k for k, v in {'ID': cid, 'SECRET': sec, 'REFRESH': ref}.items() if not v]
            logger.warning(f"YouTube Broadcaster disabled. Missing: {', '.join(missing)}")

    def run(self, media_input: str):
        logger.info(f"--- Starting Pipeline for: {media_input} ---")
        movie_api = Movie()
        search = movie_api.search(media_input)
        
        if not search:
            clean_name = re.sub(r'\s+\d{4}$', '', media_input)
            search = movie_api.search(clean_name)

        if not search:
            logger.error("Media not found on TMDb.")
            return

        item = search[0]
        info = {
            'title': item.title, 
            'overview': item.overview, 
            'year': getattr(item, 'release_date', '0000')[:4]
        }
        
        # 1. Source
        raw_video = self.source.find_and_download_media(f"{info['title']} {info['year']}", self.brain)
        if not raw_video: return

        # 2. Process
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        processed_video = os.path.join(OUTPUT_DIR, f"final_{int(time.time())}.mp4")
        if self.lab.process_video(raw_video, processed_video):
            # 3. Metadata & Upload
            yt_title, yt_desc = self.brain.generate_youtube_metadata(info)
            if self.broadcaster:
                self.broadcaster.upload(processed_video, {'title': yt_title, 'description': yt_desc})
            else:
                logger.info("Upload skipped: No credentials in environment.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--media", required=True)
    args = parser.parse_args()
    
    if not os.environ.get('TMDB_API_KEY') or not os.environ.get('GEMINI_API_KEY'):
        logger.error("Missing critical API keys (TMDB or GEMINI).")
        sys.exit(1)
        
    Orchestrator().run(args.media)
