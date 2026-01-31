import os
import sys
import argparse
import time
import json
import logging
import subprocess
import re
import shutil
import requests
import ffmpeg
import google.generativeai as genai
from tmdbv3api import TMDb, Movie, TV
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DOWNLOAD_DIR = "./downloads"
OUTPUT_DIR = "./processed"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- Classes ---

class TorrentSearcher:
    """Aggregates search results from public APIs to find the fastest download."""
    
    # Using SolidTorrents API as a reliable, non-blocked source
    API_URL = "https://solidtorrents.to/api/v1/search"

    @staticmethod
    def search(query):
        logger.info(f"Searching torrents for: {query}")
        params = {"q": query, "category": "Video", "sort": "seeders"}
        
        try:
            response = requests.get(TorrentSearcher.API_URL, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()
            
            results = []
            for item in data.get('results', []):
                # Filter logic: Prefer 1080p, avoid huge files > 10GB for GitHub Actions limits
                size_mb = item['size'] / (1024 * 1024)
                if 500 < size_mb < 8000: # Between 500MB and 8GB
                    results.append({
                        'title': item['title'],
                        'magnet': item['magnet'],
                        'seeds': item['swarm']['seeders'],
                        'size': item['size']
                    })
            
            # Sort by seeds (descending)
            results.sort(key=lambda x: x['seeds'], reverse=True)
            return results
        except Exception as e:
            logger.error(f"Search failed: {e}")
            return []

class Downloader:
    """Uses aria2c for maximum speed torrent downloading."""
    
    @staticmethod
    def download(magnet_link):
        logger.info("Starting download via aria2c...")
        # aria2c command optimized for automated environments
        cmd = [
            'aria2c',
            '--dir', DOWNLOAD_DIR,
            '--seed-time=0',        # Stop seeding immediately after download
            '--max-connection-per-server=16',
            '--split=16',
            '--bt-stop-timeout=600', # Stop if download stalls for 10 mins
            magnet_link
        ]
        
        try:
            subprocess.run(cmd, check=True)
            
            # Find the largest video file downloaded
            video_extensions = ('.mp4', '.mkv', '.avi', '.mov')
            largest_file = None
            largest_size = 0
            
            for root, dirs, files in os.walk(DOWNLOAD_DIR):
                for file in files:
                    if file.lower().endswith(video_extensions):
                        filepath = os.path.join(root, file)
                        size = os.path.getsize(filepath)
                        if size > largest_size:
                            largest_size = size
                            largest_file = filepath
            
            if not largest_file:
                raise FileNotFoundError("No video file found in download.")
                
            logger.info(f"Download complete: {largest_file}")
            return largest_file
        except subprocess.CalledProcessError as e:
            logger.error(f"aria2c failed: {e}")
            return None

class MediaProcessor:
    """FFmpeg wrapper to filter Audio/Subs."""
    
    @staticmethod
    def process(input_file):
        filename = os.path.basename(input_file)
        name, _ = os.path.splitext(filename)
        output_video = os.path.join(OUTPUT_DIR, f"{name}_clean.mp4")
        output_sub = os.path.join(OUTPUT_DIR, f"{name}_en.srt")
        
        logger.info(f"Analyzing streams for: {input_file}")
        
        try:
            probe = ffmpeg.probe(input_file)
            streams = probe['streams']
            
            # Logic: Find English Audio and English Subtitles
            audio_map = "0:a:0" # Default to first audio
            sub_map = None
            
            for s in streams:
                if s['codec_type'] == 'audio':
                    tags = s.get('tags', {})
                    lang = tags.get('language', 'und')
                    if lang == 'eng':
                        audio_map = f"0:{s['index']}"
                        break # Found English, stop looking
                        
            for s in streams:
                if s['codec_type'] == 'subtitle':
                    tags = s.get('tags', {})
                    lang = tags.get('language', 'und')
                    if lang == 'eng':
                        sub_map = f"0:{s['index']}"
                        break
            
            # Build FFmpeg command for VIDEO processing
            # We copy video stream to preserve quality and save CPU
            # We re-encode audio to aac to ensure MP4 compatibility
            cmd_vid = [
                'ffmpeg', '-y',
                '-i', input_file,
                '-map', '0:v:0',      # Video stream
                '-map', audio_map,    # Selected Audio stream
                '-c:v', 'copy',       # Copy video (fast)
                '-c:a', 'aac',        # AAC Audio
                '-strict', 'experimental',
                output_video
            ]
            
            logger.info("Processing Video (Stripping extra audio)...")
            subprocess.run(cmd_vid, check=True)
            
            # Extract Subtitles if found
            if sub_map:
                logger.info("Extracting English Subtitles...")
                cmd_sub = [
                    'ffmpeg', '-y',
                    '-i', input_file,
                    '-map', sub_map,
                    output_sub
                ]
                subprocess.run(cmd_sub, check=True)
                return output_video, output_sub
            else:
                logger.warning("No English subtitles found internally.")
                return output_video, None
                
        except ffmpeg.Error as e:
            logger.error(f"FFmpeg Error: {e.stderr.decode() if e.stderr else str(e)}")
            return None, None
        except Exception as e:
            logger.error(f"Processing failed: {e}")
            return None, None

class GeminiMetadata:
    """Generates Title and Description using Gemini."""
    
    def __init__(self):
        api_key = os.environ.get("GEMINI_API_KEY")
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel('gemini-1.5-flash')

    def generate(self, query):
        logger.info("Generating Metadata via Gemini...")
        prompt = f"""
        I am uploading a video about "{query}" to YouTube.
        Generate a JSON object with two keys:
        1. 'title': A catchy, SEO-friendly YouTube title (max 90 chars).
        2. 'description': A detailed description (3-4 paragraphs) including plot summary, cast, and hashtags.
        Output ONLY valid JSON.
        """
        try:
            response = self.model.generate_content(prompt)
            text = response.text.strip()
            # Clean potential markdown code blocks
            if text.startswith("```"):
                text = text.split("\n", 1)[1]
                if text.endswith("```"):
                    text = text.rsplit("\n", 1)[0]
            
            data = json.loads(text)
            return data['title'], data['description']
        except Exception as e:
            logger.error(f"AI Metadata generation failed: {e}")
            return f"{query} - Full Video", f"Watch {query} in high quality."

class YouTubeUploader:
    """Uploads video and subtitles."""
    
    def __init__(self):
        client_id = os.environ.get('YOUTUBE_CLIENT_ID')
        client_secret = os.environ.get('YOUTUBE_CLIENT_SECRETS')
        refresh_token = os.environ.get('YOUTUBE_REFRESH_TOKEN')
        
        if not all([client_id, client_secret, refresh_token]):
            raise ValueError("Missing YouTube Credentials")

        info = {
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
            "token_uri": "https://oauth2.googleapis.com/token",
            "type": "authorized_user"
        }
        
        self.creds = Credentials.from_authorized_user_info(info)
        self.service = build('youtube', 'v3', credentials=self.creds)

    def upload(self, video_path, sub_path, title, description):
        logger.info(f"Uploading to YouTube: {title}")
        
        body = {
            'snippet': {
                'title': title,
                'description': description,
                'categoryId': '24' # Entertainment
            },
            'status': {
                'privacyStatus': 'private', # Always upload private first
                'selfDeclaredMadeForKids': False
            }
        }
        
        media = MediaFileUpload(video_path, chunksize=1024*1024, resumable=True)
        request = self.service.videos().insert(part="snippet,status", body=body, media_body=media)
        
        response = None
        while response is None:
            status, response = request.next_chunk()
            if status:
                logger.info(f"Upload Progress: {int(status.progress() * 100)}%")
        
        video_id = response.get('id')
        logger.info(f"Video Uploaded! ID: {video_id}")
        
        # Upload Captions if available
        if sub_path and os.path.exists(sub_path):
            self.upload_caption(video_id, sub_path)
            
    def upload_caption(self, video_id, sub_path):
        logger.info("Uploading Subtitles...")
        media = MediaFileUpload(sub_path)
        body = {
            'snippet': {
                'videoId': video_id,
                'language': 'en',
                'name': 'English'
            }
        }
        self.service.captions().insert(part="snippet", body=body, media_body=media).execute()
        logger.info("Subtitles uploaded successfully.")

# --- Main Orchestrator ---

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", required=True, help="Name of movie/series")
    args = parser.parse_args()
    
    # 1. Search
    results = TorrentSearcher.search(args.query)
    if not results:
        logger.error("No torrents found.")
        return
        
    best_magnet = results[0]['magnet']
    logger.info(f"Selected: {results[0]['title']} (Seeds: {results[0]['seeds']})")
    
    # 2. Download
    raw_video = Downloader.download(best_magnet)
    if not raw_video:
        return
        
    # 3. Process (Cut audio, extract subs)
    clean_video, clean_sub = MediaProcessor.process(raw_video)
    if not clean_video:
        return
        
    # 4. Generate Metadata
    title, desc = GeminiMetadata().generate(args.query)
    
    # 5. Upload
    try:
        uploader = YouTubeUploader()
        uploader.upload(clean_video, clean_sub, title, desc)
    except Exception as e:
        logger.error(f"Upload failed: {e}")

if __name__ == "__main__":
    main()
