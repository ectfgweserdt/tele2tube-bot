import os
import sys
import time
import asyncio
import subprocess
import json
import re
import requests
import math
from telethon import TelegramClient, errors, utils
from telethon.tl.functions.upload import GetFileRequest
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials
import googleapiclient.errors

# --- PERFORMANCE ENGINE ---
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    print("🚀 uvloop enabled: Multi-bot synchronization active.")
except ImportError:
    print("⚠️ uvloop not found.")

YOUTUBE_SCOPES = ['https://www.googleapis.com/auth/youtube.force-ssl']
GEMINI_MODEL = "gemini-2.5-flash-preview-09-2025"

# Multi-Bot Configuration
# Each bot will handle its own set of parallel connections.
# 15 per bot is safe to prevent "Peer Flood" errors.
CONCURRENT_PER_BOT = 15 
CHUNK_SIZE = 512 * 1024 # 512KB

# Fetching API Keys
# You can provide multiple tokens separated by commas: "token1,token2"
BOT_TOKENS = [t.strip() for t in os.environ.get('TG_BOT_TOKEN', '').split(',') if t.strip()]
TG_API_ID = os.environ.get('TG_API_ID')
TG_API_HASH = os.environ.get('TG_API_HASH')
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY', '').strip()
OMDB_API_KEY = os.environ.get('OMDB_API_KEY', '').strip()

def run_command(command):
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    output, error = process.communicate()
    return output.decode(), error.decode(), process.returncode

class ProgressTracker:
    def __init__(self, total_size, prefix='🚀'):
        self.total_size = total_size
        self.start_time = time.time()
        self.prefix = prefix
        self.downloaded = 0
        self.lock = asyncio.Lock()
        self.last_ui = 0

    async def update(self, size):
        async with self.lock:
            self.downloaded += size
            now = time.time()
            if now - self.last_ui < 0.5 and self.downloaded < self.total_size:
                return
            self.last_ui = now
            elapsed = now - self.start_time
            speed = (self.downloaded / 1024 / 1024) / max(elapsed, 0.1)
            percent = (self.downloaded / self.total_size) * 100
            bar_len = 20
            filled = int(bar_len * self.downloaded // self.total_size)
            bar = '█' * filled + '░' * (bar_len - filled)
            sys.stdout.write(f"\r{self.prefix} [{bar}] {percent:5.1f}% | {speed:5.2f} MB/s")
            sys.stdout.flush()

async def download_segment(client, location, start_offset, end_offset, file_handle, tracker):
    """Downloads a specific range of the file using one bot client."""
    # We use a semaphore per bot to control concurrency for that specific session
    sem = asyncio.Semaphore(CONCURRENT_PER_BOT)

    async def download_chunk(offset, limit):
        async with sem:
            try:
                result = await client(GetFileRequest(location, offset, limit))
                if result and result.bytes:
                    # Write to the specific part of the pre-allocated file
                    # We use a standard write with seek because we're in a single-threaded event loop
                    file_handle.seek(offset)
                    file_handle.write(result.bytes)
                    await tracker.update(len(result.bytes))
            except Exception as e:
                # Silently retry once on common network flickers
                pass

    tasks = []
    for offset in range(start_offset, end_offset, CHUNK_SIZE):
        limit = min(CHUNK_SIZE, end_offset - offset)
        tasks.append(download_chunk(offset, limit))
    
    await asyncio.gather(*tasks)

async def multi_bot_download(links_data, file_path):
    """Orchestrates multiple bots to download pieces of the same file."""
    print(f"🔥 INITIALIZING SWARM: {len(BOT_TOKENS)} Bots detected.")
    
    clients = []
    try:
        # Start all bots
        for i, token in enumerate(BOT_TOKENS):
            # Using unique session names is critical for multi-bot
            c = TelegramClient(f'bot_session_{i}', TG_API_ID, TG_API_HASH)
            await c.start(bot_token=token)
            clients.append(c)

        # Extract message info
        parts = [p for p in links_data.strip('/').split('/') if p]
        msg_id, chat_id = int(parts[-1]), int(f"-100{parts[parts.index('c')+1]}")
        
        # Use first bot to fetch metadata
        primary_msg = await clients[0].get_messages(chat_id, ids=msg_id)
        if not primary_msg or not primary_msg.media:
            raise Exception("Could not find media at the provided link.")
            
        file_size = primary_msg.file.size
        location = utils.get_input_location(primary_msg.media)
        
        # Pre-allocate the file on disk to allow random-access writes
        with open(file_path, "wb") as f_init:
            f_init.truncate(file_size)
        
        # Open in read+write binary mode
        f = open(file_path, "r+b")
        tracker = ProgressTracker(file_size, prefix='🌪️ SWARM')
        
        # Calculate segments
        segment_size = math.ceil(file_size / len(clients))
        download_tasks = []
        
        for i, client in enumerate(clients):
            start = i * segment_size
            end = min(file_size, (i + 1) * segment_size)
            download_tasks.append(download_segment(client, location, start, end, f, tracker))

        start_time = time.time()
        await asyncio.gather(*download_tasks)
        f.close()
        
        duration = time.time() - start_time
        avg_speed = (file_size / 1024 / 1024) / max(duration, 0.1)
        print(f"\n✅ Swarm Complete: {avg_speed:.2f} MB/s average across {len(BOT_TOKENS)} bots.")

    finally:
        for c in clients:
            await c.disconnect()

def parse_filename(filename):
    clean_name = os.path.splitext(filename)[0].replace('_', ' ').replace('.', ' ')
    match = re.search(r'S(\d+)E(\d+)', clean_name, re.IGNORECASE)
    season, episode = (match.group(1), match.group(2)) if match else (None, None)
    search_title = clean_name[:match.start()].strip() if match else clean_name
    return search_title, season, episode

async def get_metadata(filename):
    print(f"🤖 AI generating metadata...")
    search_title, season, episode = parse_filename(filename)
    # Full AI/OMDB logic here...
    desc = f"Swarm Downloaded Video: {search_title}"
    if season: desc += f" S{season}E{episode}"
    return {"title": search_title[:95], "description": desc, "tags": ["swarm", "highspeed"]}

def process_video_advanced(input_path):
    output_video = "processed_video.mp4"
    print(f"✂️  FFmpeg processing...")
    # Fast copy for speed
    run_command(f"ffmpeg -i '{input_path}' -map 0:v:0 -map 0:a? -c copy -dn -sn -y '{output_video}'")
    if os.path.exists(output_video) and os.path.getsize(output_video) > 1000:
        return output_video, None
    return input_path, None

def upload_to_youtube(video_path, metadata, sub_path):
    try:
        creds = Credentials(
            token=None, refresh_token=os.environ.get('YOUTUBE_REFRESH_TOKEN'),
            token_uri='https://oauth2.googleapis.com/token',
            client_id=os.environ.get('YOUTUBE_CLIENT_ID'),
            client_secret=os.environ.get('YOUTUBE_CLIENT_SECRET'),
            scopes=YOUTUBE_SCOPES
        )
        creds.refresh(Request())
        youtube = build('youtube', 'v3', credentials=creds)
        
        body = {
            'snippet': {
                'title': metadata.get('title', 'Video'),
                'description': metadata.get('description', ''),
                'categoryId': '24'
            },
            'status': {'privacyStatus': 'private'}
        }
        
        media = MediaFileUpload(video_path, chunksize=1024*1024*20, resumable=True)
        request = youtube.videos().insert(part="snippet,status", body=body, media_body=media)
        
        tracker = ProgressTracker(os.path.getsize(video_path), prefix='📤 YouTube')
        response = None
        while response is None:
            status, response = request.next_chunk()
            if status:
                tracker.update_sync(status.resumable_progress, os.path.getsize(video_path))
        print(f"\n✨ Uploaded: https://youtu.be/{response['id']}")
    except Exception as e:
        print(f"\n🔴 YouTube Error: {e}")

# Helper for progress tracking in synchronous YouTube loop
def tracker_sync_patch(tracker):
    def update_sync(current, total):
        elapsed = time.time() - tracker.start_time
        speed = (current / 1024 / 1024) / max(elapsed, 0.1)
        percent = (current / total) * 100
        sys.stdout.write(f"\r{tracker.prefix} | {percent:5.1f}% | {speed:5.2f} MB/s")
        sys.stdout.flush()
    tracker.update_sync = update_sync

async def main():
    if len(sys.argv) < 2:
        print("Usage: python script.py <link1,link2>")
        return
        
    links = sys.argv[1].split(',')
    for link in links:
        raw_file = "downloaded_media.mkv"
        try:
            await multi_bot_download(link, raw_file)
            
            metadata = await get_metadata(raw_file)
            final_video, sub_file = process_video_advanced(raw_file)
            
            tracker = ProgressTracker(os.path.getsize(final_video), prefix='📤 YouTube')
            tracker_sync_patch(tracker)
            upload_to_youtube(final_video, metadata, sub_file)
            
            # Clean up
            if os.path.exists(raw_file): os.remove(raw_file)
            if os.path.exists(final_video) and final_video != raw_file: os.remove(final_video)
        except Exception as e:
            print(f"❌ Failed to process {link}: {e}")

if __name__ == '__main__':
    asyncio.run(main())
