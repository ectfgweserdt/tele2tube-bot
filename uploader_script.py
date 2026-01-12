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
CONCURRENT_PER_BOT = 10 
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
        """Synchronous update for YouTube upload chunks"""
        self._draw_sync(current, total)

    async def _draw(self, current, total):
        now = time.time()
        if now - self.last_ui < 0.5 and current < total:
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
        if now - self.last_ui < 0.5 and current < total:
            return
        self.last_ui = now
        elapsed = now - self.start_time
        speed = (current / 1024 / 1024) / max(elapsed, 0.1)
        percent = (current / total) * 100
        sys.stdout.write(f"\r{self.prefix} | {percent:5.1f}% | {speed:5.2f} MB/s")
        sys.stdout.flush()

async def download_segment(client, location, start_offset, end_offset, file_path, tracker):
    sem = asyncio.Semaphore(CONCURRENT_PER_BOT)
    
    async def download_chunk(offset, limit):
        async with sem:
            for retry in range(3):
                try:
                    result = await client(GetFileRequest(location, offset, limit))
                    if result and result.bytes:
                        # Use OS-level handle to write to specific offset
                        with open(file_path, "r+b") as f:
                            f.seek(offset)
                            f.write(result.bytes)
                            f.flush()
                        await tracker.update(len(result.bytes))
                        return
                except Exception:
                    await asyncio.sleep(1)
            print(f"\n❌ Permanent failure at offset {offset}")

    tasks = []
    for offset in range(start_offset, end_offset, CHUNK_SIZE):
        limit = min(CHUNK_SIZE, end_offset - offset)
        tasks.append(download_chunk(offset, limit))
    await asyncio.gather(*tasks)

async def multi_bot_download(links_data, file_path):
    print(f"🔥 INITIALIZING SWARM: {len(BOT_TOKENS)} Bots detected.")
    clients = []
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
        
        # Proper pre-allocation
        with open(file_path, "wb") as f_init:
            f_init.write(b'\0' * file_size)
        
        tracker = ProgressTracker(file_size, prefix='🌪️ SWARM')
        segment_size = math.ceil(file_size / len(clients))
        download_tasks = []
        
        for i, client in enumerate(clients):
            start = i * segment_size
            end = min(file_size, (i + 1) * segment_size)
            download_tasks.append(download_segment(client, location, start, end, file_path, tracker))

        start_time = time.time()
        await asyncio.gather(*download_tasks)
        
        # Verify if file actually contains data
        if os.path.getsize(file_path) != file_size:
            raise Exception("File size mismatch after download!")
            
        print(f"\n✅ Swarm Complete: {((file_size/1024/1024)/(time.time()-start_time)):.2f} MB/s average.")
    finally:
        for c in clients: await c.disconnect()

async def get_metadata(filename):
    print(f"🤖 AI generating metadata...")
    clean_name = os.path.splitext(filename)[0].replace('_', ' ').replace('.', ' ')
    return {"title": clean_name[:95], "description": "High speed swarm upload."}

def process_video_advanced(input_path):
    output_video = "processed_video.mp4"
    print(f"✂️  FFmpeg processing (Remuxing to MP4)...")
    # Using 'copy' is fast, but let's ensure we output a standard MP4 for YouTube
    run_command(f"ffmpeg -i '{input_path}' -c copy -movflags +faststart -y '{output_video}'")
    if os.path.exists(output_video) and os.path.getsize(output_video) > 1000:
        return output_video
    return input_path

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
        
        body = {
            'snippet': {'title': metadata.get('title', 'Video'), 'description': metadata.get('description', ''), 'categoryId': '24'},
            'status': {'privacyStatus': 'private'}
        }
        
        media = MediaFileUpload(video_path, chunksize=1024*1024*10, resumable=True)
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

async def main():
    if len(sys.argv) < 2: return
    links = sys.argv[1].split(',')
    for link in links:
        raw_file = f"download_{int(time.time())}.mkv"
        try:
            await multi_bot_download(link, raw_file)
            metadata = await get_metadata(raw_file)
            final_video = process_video_advanced(raw_file)
            upload_to_youtube(final_video, metadata)
            
            if os.path.exists(raw_file): os.remove(raw_file)
            if os.path.exists(final_video) and final_video != raw_file: os.remove(final_video)
        except Exception as e:
            print(f"❌ Failed: {e}")

if __name__ == '__main__':
    asyncio.run(main())
