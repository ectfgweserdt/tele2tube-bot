"""
Project Title: Gemini-Powered Automated Content Sourcing and YouTube Publishing Pipeline
Author: Research Assistant
Date: January 28, 2026
Version: 2.0 (Optimized for Tools & Libraries)

Description:
This script orchestrates the entire lifecycle of media publishing. It identifies content
(Movie/TV), sources it via external CLI tools, optimizes it with FFmpeg, and uploads to YouTube.
Gemini Pro is used as the 'Brain' to make selection decisions and generate creative metadata.

Usage:
    python media_orchestrator.py --media "Inception"
    python media_orchestrator.py --media "Breaking Bad S02"
"""

import os
import sys
import argparse
import re
import json
import subprocess
import time
import logging
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
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(module)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

# Constants
DOWNLOAD_DIR = "./downloads"
OUTPUT_DIR = "./ready_to_upload"
TEMP_DIR = "./temp"

# --- Modules ---

class GeminiBrain:
    """
    The Intelligence Unit. Uses Gemini Pro to make decisions and generate text.
    """
    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("GEMINI_API_KEY is missing.")
        genai.configure(api_key=api_key)
        # Using Gemini 1.5 Flash or Pro for speed/quality balance in metadata
        self.model = genai.GenerativeModel('gemini-1.5-flash')

    def select_best_torrent(self, candidates: List[Dict], criteria: str = "1080p, high seeds") -> Optional[Dict]:
        """
        Feeds the list of torrent candidates to Gemini and asks it to pick the best one.
        This replaces manual logic with AI decision making.
        """
        if not candidates:
            return None

        prompt = f"""
        Act as a Media Archival Expert. I will provide a list of available video sources (JSON).
        Select the SINGLE best source based on these criteria: {criteria}.
        Prioritize: 1080p resolution, high seeder count (reliability), and standard codecs (x264).
        
        Return ONLY a JSON object with the index of the best candidate and a reason.
        Format: {{ "best_index": <int>, "reason": "<string>" }}

        Candidates:
        {json.dumps(candidates, indent=2)}
        """
        
        try:
            response = self.model.generate_content(prompt)
            # Clean response to ensure valid JSON
            text = response.text.replace('```json', '').replace('```', '').strip()
            decision = json.loads(text)
            best_idx = decision.get("best_index")
            
            if best_idx is not None and 0 <= best_idx < len(candidates):
                logger.info(f"Gemini selected candidate #{best_idx}: {decision.get('reason')}")
                return candidates[best_idx]
        except Exception as e:
            logger.error(f"Gemini failed to select torrent: {e}")
        
        # Fallback: Pick the one with most seeds if AI fails
        return sorted(candidates, key=lambda x: x.get('seeds', 0), reverse=True)[0]

    def generate_youtube_metadata(self, media_info: Dict) -> Tuple[str, str]:
        """
        Generates a click-worthy Title and SEO-optimized Description.
        """
        prompt = f"""
        Generate YouTube metadata for the following video content.
        
        Content Details:
        Title: {media_info.get('title')}
        Plot: {media_info.get('overview')}
        Type: {media_info.get('type')}
        Episode: {media_info.get('episode_label', 'N/A')}

        Output Format (JSON):
        {{
            "title": "A catchy, click-worthy YouTube title (max 70 chars)",
            "description": "A detailed, SEO-friendly description including the plot summary (re-written), cast, and relevant hashtags."
        }}
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
    """
    Handles finding and downloading media and subtitles using external Tools/Libraries.
    """
    def __init__(self, download_dir: str):
        self.download_dir = download_dir
        os.makedirs(download_dir, exist_ok=True)

    def find_and_download_media(self, query: str, brain: GeminiBrain) -> Optional[str]:
        """
        1. Uses a CLI tool to search for torrents.
        2. Uses Gemini to pick the best one.
        3. Uses aria2c to download.
        """
        logger.info(f"Sourcing media for query: {query}")
        
        # --- STEP 1: Search (Using external tool pattern) ---
        # NOTE: In a real deployment, replace this with a call to a specific CLI tool
        # e.g., output = subprocess.check_output(["torrent-search", query, "--json"])
        # For safety/compliance in this demo, we simulate a list of candidates.
        
        candidates = self._simulate_torrent_search(query) # <--- Replace with real tool call
        
        if not candidates:
            logger.warning("No media sources found.")
            return None

        # --- STEP 2: Selection (AI Driven) ---
        best_choice = brain.select_best_torrent(candidates)
        if not best_choice:
            return None
        
        magnet_link = best_choice['magnet']
        logger.info(f"Downloading: {best_choice['title']}")

        # --- STEP 3: Download (aria2c) ---
        # We use a unique hash for the folder to prevent collisions
        import hashlib
        folder_hash = hashlib.md5(query.encode()).hexdigest()[:8]
        save_path = os.path.join(self.download_dir, folder_hash)
        
        cmd = [
            "aria2c", 
            "--seed-time=0", 
            "-d", save_path, 
            magnet_link
        ]
        
        # In a real run, uncomment the subprocess call.
        # subprocess.run(cmd, check=True)
        
        # DEMO BYPASS: Create a dummy file for the pipeline to continue
        dummy_file = os.path.join(save_path, "movie.mp4")
        os.makedirs(save_path, exist_ok=True)
        with open(dummy_file, 'wb') as f:
            f.write(b'\x00' * 1024) # 1KB dummy file
        
        return dummy_file

    def _simulate_torrent_search(self, query: str) -> List[Dict]:
        """
        Simulates an external tool returning JSON results.
        """
        return [
            {
                "title": f"{query} 1080p BluRay x264-SPARKS",
                "size": "2.4 GB",
                "seeds": 150,
                "magnet": "magnet:?xt=urn:btih:EXAMPLE123456"
            },
            {
                "title": f"{query} 720p WEB-DL AAC2.0 H.264",
                "size": "900 MB",
                "seeds": 450,
                "magnet": "magnet:?xt=urn:btih:EXAMPLE789012"
            }
        ]

    def get_subtitles(self, video_path: str, language: str = 'en') -> Optional[str]:
        """
        Uses the 'subliminal' library to auto-detect and download best subtitles.
        """
        logger.info(f"Searching for subtitles for: {video_path}")
        try:
            # Configure subliminal
            video = subliminal.Video.fromname(video_path)
            best_subtitles = subliminal.download_best_subtitles([video], {Language(language)})
            
            if best_subtitles[video]:
                sub = best_subtitles[video][0]
                # Save subtitle content
                sub_path = os.path.splitext(video_path)[0] + f".{language}.srt"
                subliminal.save_subtitles(video, [sub])
                logger.info(f"Subtitles downloaded to: {sub_path}")
                return sub_path
            else:
                logger.warning("No subtitles found via subliminal.")
                return None
        except Exception as e:
            logger.error(f"Subtitle download failed: {e}")
            # Create dummy subtitle for pipeline continuity in demo
            dummy_sub = os.path.splitext(video_path)[0] + ".srt"
            with open(dummy_sub, 'w') as f:
                f.write("1\n00:00:01,000 --> 00:00:05,000\nSubtitle download failed, this is a placeholder.")
            return dummy_sub


class VideoLab:
    """
    Handles FFmpeg operations to ensure YouTube compliance.
    """
    @staticmethod
    def process_video(input_path: str, output_path: str) -> str:
        logger.info(f"Processing video: {input_path}")
        
        # Probe file
        try:
            probe = ffmpeg.probe(input_path)
            video_stream = next((stream for stream in probe['streams'] if stream['codec_type'] == 'video'), None)
            
            # Smart Transcode Logic:
            # If h264/mp4, just copy (fast). If mkv/hevc, re-containerize or re-encode.
            if video_stream and video_stream['codec_name'] == 'h264':
                v_codec = 'copy'
            else:
                v_codec = 'libx264'

            # Audio usually needs to be AAC
            a_codec = 'aac'

            (
                ffmpeg
                .input(input_path)
                .output(output_path, vcodec=v_codec, acodec=a_codec, strict='experimental', loglevel="error")
                .overwrite_output()
                .run()
            )
            logger.info("Video processing complete.")
            return output_path
            
        except ffmpeg.Error as e:
            logger.error(f"FFmpeg error: {e}")
            # For the demo dummy file, we just return the input
            return input_path


class YouTubeBroadcaster:
    """
    Handles authenticated uploads to YouTube.
    """
    SCOPES = ['https://www.googleapis.com/auth/youtube.upload']
    
    def __init__(self, client_info: Dict):
        # In a CI environment, we construct credentials from secrets (Refresh Token flow)
        self.creds = Credentials(
            None, # No access token initially
            refresh_token=client_info['refresh_token'],
            token_uri="https://oauth2.googleapis.com/token",
            client_id=client_info['client_id'],
            client_secret=client_info['client_secret'],
            scopes=self.SCOPES
        )
        self.service = build('youtube', 'v3', credentials=self.creds)

    def upload(self, video_path: str, metadata: Dict, thumb_url: str = None) -> str:
        logger.info("Starting YouTube Upload...")
        
        body = {
            'snippet': {
                'title': metadata['title'],
                'description': metadata['description'],
                'tags': ['AI', 'Automated', 'Movie'],
                'categoryId': '24' # Entertainment
            },
            'status': {
                'privacyStatus': 'private', # Safety default
                'selfDeclaredMadeForKids': False
            }
        }

        # MediaFileUpload handles the heavy lifting
        # resumable=True is critical for large video files
        media = MediaFileUpload(video_path, chunksize=-1, resumable=True)

        try:
            request = self.service.videos().insert(
                part=','.join(body.keys()),
                body=body,
                media_body=media
            )
            
            response = None
            while response is None:
                status, response = request.next_chunk()
                if status:
                    logger.info(f"Upload progress: {int(status.progress() * 100)}%")
            
            video_id = response.get('id')
            logger.info(f"Upload Successful! Video ID: {video_id}")
            return video_id
        except Exception as e:
            logger.error(f"Upload failed: {e}")
            return None


class Orchestrator:
    def __init__(self):
        # Initialize TMDb
        self.tmdb = TMDb()
        self.tmdb.api_key = os.environ.get('TMDB_API_KEY')
        self.tmdb.language = 'en'
        
        # Initialize Gemini
        self.brain = GeminiBrain(os.environ.get('GEMINI_API_KEY'))
        
        # Initialize Source & Lab
        self.source = ContentSource(DOWNLOAD_DIR)
        self.lab = VideoLab()

        # Initialize YouTube (if creds exist)
        if os.environ.get('YOUTUBE_REFRESH_TOKEN'):
            self.broadcaster = YouTubeBroadcaster({
                'client_id': os.environ.get('YOUTUBE_CLIENT_ID'),
                'client_secret': os.environ.get('YOUTUBE_CLIENT_SECRET'),
                'refresh_token': os.environ.get('YOUTUBE_REFRESH_TOKEN')
            })
        else:
            logger.warning("YouTube credentials missing. Upload step will be skipped.")
            self.broadcaster = None

    def parse_input(self, input_str: str) -> Dict:
        # Regex for "Name S01"
        match = re.search(r'^(.*?)\s+[sS](\d{1,2})$', input_str.strip())
        if match:
            return {'type': 'tv', 'name': match.group(1), 'season': int(match.group(2))}
        return {'type': 'movie', 'name': input_str.strip()}

    def run(self, media_input: str):
        logger.info(f"--- Starting Pipeline for: {media_input} ---")
        request = self.parse_input(media_input)

        if request['type'] == 'tv':
            self.process_tv_season(request['name'], request['season'])
        else:
            self.process_movie(request['name'])

    def process_movie(self, name: str):
        # 1. Fetch Info
        movie_api = Movie()
        search = movie_api.search(name)
        if not search:
            logger.error("Movie not found on TMDb.")
            return
        details = search[0]
        
        info = {
            'title': details.title,
            'overview': details.overview,
            'type': 'Movie',
            'year': details.release_date.split('-')[0]
        }
        
        self.execute_pipeline_for_item(info, f"{name} {info['year']}")

    def process_tv_season(self, name: str, season_num: int):
        # 1. Fetch Info
        tv_api = TV()
        season_api = Season()
        
        search = tv_api.search(name)
        if not search:
            logger.error("TV Show not found.")
            return
        show = search[0]
        
        season_details = season_api.details(show.id, season_num)
        
        if not hasattr(season_details, 'episodes'):
            logger.error("No episodes found.")
            return

        for ep in season_details.episodes:
            info = {
                'title': f"{show.name} - {ep.name}",
                'overview': ep.overview,
                'type': 'TV Episode',
                'episode_label': f"S{season_num:02d}E{ep.episode_number:02d}"
            }
            # Search query example: Breaking Bad S01E01 1080p
            query = f"{show.name} {info['episode_label']}"
            self.execute_pipeline_for_item(info, query)

    def execute_pipeline_for_item(self, info: Dict, search_query: str):
        logger.info(f">>> Processing Item: {info['title']}")
        
        # 2. Source Media (Using Tool + Gemini Selection)
        raw_video = self.source.find_and_download_media(search_query, self.brain)
        if not raw_video:
            return

        # 3. Source Subtitles (Using Subliminal Library)
        sub_file = self.source.get_subtitles(raw_video)
        
        # 4. Process Video (FFmpeg)
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        final_video = os.path.join(OUTPUT_DIR, f"final_{int(time.time())}.mp4")
        self.lab.process_video(raw_video, final_video)
        
        # 5. Generate Metadata (Gemini)
        yt_title, yt_desc = self.brain.generate_youtube_metadata(info)
        
        # 6. Upload
        if self.broadcaster:
            self.broadcaster.upload(
                final_video,
                {'title': yt_title, 'description': yt_desc}
            )
        else:
            logger.info("Upload skipped (Dry Run).")
        
        logger.info(f"<<< Finished Item: {info['title']}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gemini Media Pipeline")
    parser.add_argument("--media", required=True, help="Movie name or 'Show Name S01'")
    args = parser.parse_args()

    # Ensure API keys are set or warn
    if not os.environ.get('GEMINI_API_KEY') or not os.environ.get('TMDB_API_KEY'):
        logger.error("Please set GEMINI_API_KEY and TMDB_API_KEY environment variables.")
        sys.exit(1)

    orchestrator = Orchestrator()
    orchestrator.run(args.media)
