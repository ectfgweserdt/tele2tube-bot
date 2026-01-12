import os
import sys
import time
import asyncio
import subprocess
import json
import re
import random
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
    print("🚀 [SYSTEM] uvloop enabled: Swarm coordination optimized.")
except ImportError:
    print("⚠️ [SYSTEM] uvloop not found.")

YOUTUBE_SCOPES = ['https://www.googleapis.com/auth/youtube.force-ssl']

# Multi-Bot Configuration
CONCURRENT_PER_BOT = 10 
CHUNK_SIZE = 512 * 1024 # 512KB

# Fetching API Keys
BOT_TOKENS = [t.strip() for t in os.environ.get('TG_BOT_TOKEN', '').split(',') if t.strip()]
TG_API_ID = os.environ.get('TG_API_ID')
TG_API_HASH = os.environ.get('TG_API_HASH')

# --- DEVICE MANIPULATION TRICKS ---
# This list mimics real user devices to avoid fingerprinting the GitHub runner
DEVICE_MODELS = [
    ("Samsung Galaxy S23", "Android 13", "v10.3.2"),
    ("iPhone 14 Pro", "iOS 16.5", "v9.6.1"),
    ("Google Pixel 7", "Android 13", "v10.0.0"),
    ("iPad Air 5", "iOS 16.1", "v9.4.0"),
    ("OnePlus 11", "Android 13", "v10.2.1")
]

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
            if now - self.last_ui < 1.0 and self.downloaded < self.total_size:
                return
            self.last_ui = now
            elapsed = now - self.start_time
            speed = (self.downloaded / 1024 / 1024) / max(elapsed, 0.1)
            percent = (self.downloaded / self.total_size) * 100
            sys.stdout.write(f"\r{self.prefix} | {percent:5.1f}% | {speed:5.2f} MB/s | {self.downloaded//1024//1024}MB")
            sys.stdout.flush()

    def update_sync(self, current, total):
        now = time.time()
        if now - self.last_ui < 1.0 and current < total:
            return
        self.last_ui = now
        elapsed = now - self.start_time
        speed = (current / 1024 / 1024) / max(elapsed, 0.1)
        percent = (current / total) * 100
        sys.stdout.write(f"\r{self.prefix} | {percent:5.1f}% | {speed:5.2f} MB/s")
        sys.stdout.flush()

async def download_segment(bot_index, client, location, start_offset, end_offset, fd, tracker):
    sem = asyncio.Semaphore(CONCURRENT_PER_BOT)
    
    async def download_chunk(offset, limit):
        async with sem:
            # Jitter: Randomize tiny delay to break simultaneous request patterns
            await asyncio.sleep(random.uniform(0.01, 0.1))
            
            for attempt in range(10): 
                try:
                    result = await client(GetFileRequest(location, offset, limit))
                    if result and result.bytes:
                        os.pwrite(fd, result.bytes, offset)
                        await tracker.update(len(result.bytes))
                        return
                except errors.FloodWaitError as e:
                    print(f"\n⏳ [BOT-{bot_index}] FloodWait: {e.seconds}s")
                    await asyncio.sleep(e.seconds + 2)
                except Exception as e:
                    if "Connection reset by peer" in str(e):
                        await asyncio.sleep(random.randint(5, 10))
                    await asyncio.sleep(1 * (attempt + 1)) 

    tasks = []
    for offset in range(start_offset, end_offset, CHUNK_SIZE):
        limit = min(CHUNK_SIZE, end_offset - offset)
        tasks.append(download_chunk(offset, limit))
    await asyncio.gather(*tasks)

async def multi_bot_download(links_data, file_path):
    print(f"🔥 [SWARM] Detected {len(BOT_TOKENS)} Bot Tokens.")
    clients = []
    fd = None
    try:
        for i, token in enumerate(BOT_TOKENS):
            device = random.choice(DEVICE_MODELS)
            print(f"🔑 [AUTH] Logging in Bot {i} as {device[0]}...")
            
            # TRICK: Spoofing system properties to look like a mobile device
            c = TelegramClient(
                f'session_{i}', TG_API_ID, TG_API_HASH,
                device_model=device[0],
                system_version=device[1],
                app_version=device[2],
                connection_retries=10,
                retry_delay=5
            )
            
            try:
                await c.start(bot_token=token)
                clients.append(c)
                # Staggered login with random jitter (3-7 seconds)
                await asyncio.sleep(random.randint(3, 7)) 
            except Exception as e:
                print(f"⚠️ [AUTH] Bot {i} skip: {e}")

        if not clients: return

        parts = [p for p in links_data.strip('/').split('/') if p]
        msg_id, chat_id = int(parts[-1]), int(f"-100{parts[parts.index('c')+1]}")
        primary_msg = await clients[0].get_messages(chat_id, ids=msg_id)
        
        file_size = primary_msg.file.size
        location = utils.get_input_location(primary_msg.media)
        
        with open(file_path, "wb") as f_init:
            f_init.truncate(file_size)
        
        fd = os.open(file_path, os.O_RDWR)
        tracker = ProgressTracker(file_size, prefix='🌪️  SWARM')
        
        segment_size = math.ceil(file_size / len(clients))
        tasks = []
        for i, client in enumerate(clients):
            start = i * segment_size
            end = min(file_size, (i + 1) * segment_size)
            tasks.append(download_segment(i, client, location, start, end, fd, tracker))

        print(f"🚀 [ACTION] Starting multi-device spoofed download...")
        start_time = time.time()
        await asyncio.gather(*tasks)
        print(f"\n✅ [DONE] Swarm Complete.")

    finally:
        if fd: os.close(fd)
        for c in clients: await c.disconnect()

def process_video_advanced(input_path):
    output_video = "processed_video.mp4"
    print(f"🎬 [FFMPEG] Finalizing container...")
    run_command(f"ffmpeg -i '{input_path}' -c copy -movflags +faststart -y '{output_video}'")
    return output_video if os.path.exists(output_video) else input_path

def upload_to_youtube(video_path, title):
    print(f"📤 [YOUTUBE] Initializing...")
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
            body={'snippet': {'title': title[:95], 'categoryId': '24'}, 'status': {'privacyStatus': 'private'}},
            media_body=media
        )
        
        tracker = ProgressTracker(os.path.getsize(video_path), prefix='📤  YOUTUBE')
        response = None
        while response is None:
            status, response = request.next_chunk()
            if status:
                tracker.update_sync(status.resumable_progress, os.path.getsize(video_path))
        print(f"\n✨ [SUCCESS] ID: {response['id']}")
    except Exception as e:
        print(f"\n🔴 [ERROR] YouTube: {e}")

async def main():
    if len(sys.argv) < 2: return
    links = sys.argv[1].split(',')
    for link in links:
        raw_file = f"dn_{int(time.time())}.mkv"
        try:
            await multi_bot_download(link, raw_file)
            processed = process_video_advanced(raw_file)
            upload_to_youtube(processed, os.path.basename(raw_file))
            for f in [raw_file, processed]:
                if f and os.path.exists(f): os.remove(f)
        except Exception as e:
            print(f"❌ [CRITICAL] {e}")

if __name__ == '__main__':
    asyncio.run(main())
