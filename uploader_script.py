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
    print("🚀 uvloop enabled: Swarm coordination optimized.")
except ImportError:
    print("⚠️ uvloop not found.")

YOUTUBE_SCOPES = ['https://www.googleapis.com/auth/youtube.force-ssl']
GEMINI_MODEL = "gemini-2.5-flash-preview-09-2025"

# Multi-Bot Configuration
# Reduced per-bot concurrency to avoid triggering Telegram's "FloodWait"
CONCURRENT_PER_BOT = 8 
CHUNK_SIZE = 512 * 1024 # 512KB

# Fetching API Keys
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
            await self._draw(self.downloaded, self.total_size)

    def update_sync(self, current, total):
        self._draw_sync(current, total)

    async def _draw(self, current, total):
        now = time.time()
        if now - self.last_ui < 0.8 and current < total:
            return
        self.last_ui = now
        elapsed = now - self.start_time
        speed = (current / 1024 / 1024) / max(elapsed, 0.1)
        percent = (current / total) * 100
        bar = '█' * int(20 * current // total) + '░' * (20 - int(20 * current // total))
        sys.stdout.write(f"\r{self.prefix} [{bar}] {percent:5.1f}% | {speed:5.2f} MB/s")
        sys.stdout.flush()

    def _draw_sync(self, current, total):
        now = time.time()
        if now - self.last_ui < 0.8 and current < total:
            return
        self.last_ui = now
        elapsed = now - self.start_time
        speed = (current / 1024 / 1024) / max(elapsed, 0.1)
        percent = (current / total) * 100
        sys.stdout.write(f"\r{tracker_prefix} | {percent:5.1f}% | {speed:5.2f} MB/s")
        sys.stdout.flush()

async def download_segment(client, location, start_offset, end_offset, fd, tracker):
    sem = asyncio.Semaphore(CONCURRENT_PER_BOT)
    
    async def download_chunk(offset, limit):
        async with sem:
            # Persistent retry with exponential backoff
            for attempt in range(10): 
                try:
                    result = await client(GetFileRequest(location, offset, limit))
                    if result and result.bytes:
                        os.pwrite(fd, result.bytes, offset)
                        await tracker.update(len(result.bytes))
                        return
                except errors.FloodWaitError as e:
                    await asyncio.sleep(e.seconds + 1)
                except Exception:
                    await asyncio.sleep(2 ** attempt) 
            print(f"\n⚠️ Segment lost at {offset} after 10 retries.")

    tasks = []
    for offset in range(start_offset, end_offset, CHUNK_SIZE):
        limit = min(CHUNK_SIZE, end_offset - offset)
        tasks.append(download_chunk(offset, limit))
    await asyncio.gather(*tasks)

async def multi_bot_download(links_data, file_path):
    print(f"🔥 INITIALIZING SWARM: {len(BOT_TOKENS)} Bots")
    clients = []
    fd = None
    try:
        for i, token in enumerate(BOT_TOKENS):
            c = TelegramClient(f'bot_session_{i}', TG_API_ID, TG_API_HASH)
            await c.start(bot_token=token)
            clients.append(c)

        parts = [p for p in links_data.strip('/').split('/') if p]
        msg_id, chat_id = int(parts[-1]), int(f"-100{parts[parts.index('c')+1]}")
        primary_msg = await clients[0].get_messages(chat_id, ids=msg_id)
        
        file_size = primary_msg.file.size
        location = utils.get_input_location(primary_msg.media)
        
        # Pre-allocate physically
        with open(file_path, "wb") as f_init:
            f_init.truncate(file_size)
        
        # Open file descriptor for concurrent thread-safe writes via os.pwrite
        fd = os.open(file_path, os.O_RDWR)
        tracker = ProgressTracker(file_size, prefix='🌪️ SWARM')
        
        segment_size = math.ceil(file_size / len(clients))
        tasks = []
        for i, client in enumerate(clients):
            start = i * segment_size
            end = min(file_size, (i + 1) * segment_size)
            tasks.append(download_segment(client, location, start, end, fd, tracker))

        start_time = time.time()
        await asyncio.gather(*tasks)
        
        duration = time.time() - start_time
        print(f"\n✅ Swarm Complete: {(file_size/1024/1024)/max(duration, 0.1):.2f} MB/s")

    finally:
        if fd: os.close(fd)
        for c in clients: await c.disconnect()

async def get_metadata(filename):
    print(f"🤖 AI Metadata Generation...")
    name = os.path.splitext(filename)[0].replace('_', ' ').replace('.', ' ')
    return {"title": name[:95], "description": f"Uploaded via Swarm Engine.\nFilename: {filename}"}

def process_video_advanced(input_path):
    output_video = "processed_video.mp4"
    print(f"✂️  Remuxing for YouTube compatibility...")
    # Using 'copy' for speed, ensuring mp4 container
    run_command(f"ffmpeg -i '{input_path}' -c copy -movflags +faststart -y '{output_video}'")
    return output_video if os.path.exists(output_video) else input_path

def upload_to_youtube(video_path, metadata):
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
        
        media = MediaFileUpload(video_path, chunksize=1024*1024*10, resumable=True)
        request = youtube.videos().insert(
            part="snippet,status",
            body={'snippet': {'title': metadata['title'], 'description': metadata['description'], 'categoryId': '24'}, 'status': {'privacyStatus': 'private'}},
            media_body=media
        )
        
        print(f"📤 Uploading to YouTube...")
        response = None
        while response is None:
            status, response = request.next_chunk()
        print(f"✨ Success! ID: {response['id']}")
    except Exception as e:
        print(f"🔴 YouTube Error: {e}")

async def main():
    if len(sys.argv) < 2: return
    links = sys.argv[1].split(',')
    for link in links:
        raw_file = f"dn_{int(time.time())}.mkv"
        try:
            await multi_bot_download(link, raw_file)
            meta = await get_metadata(raw_file)
            processed = process_video_advanced(raw_file)
            upload_to_youtube(processed, meta)
            
            for f in [raw_file, processed]:
                if f and os.path.exists(f): os.remove(f)
        except Exception as e:
            print(f"❌ Critical Failure: {e}")

if __name__ == '__main__':
    asyncio.run(main())
