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
CONCURRENT_PER_BOT = 8 
CHUNK_SIZE = 512 * 1024 # 512KB

# Fetching API Keys
BOT_TOKENS = [t.strip() for t in os.environ.get('TG_BOT_TOKEN', '').split(',') if t.strip()]
TG_API_ID = os.environ.get('TG_API_ID')
TG_API_HASH = os.environ.get('TG_API_HASH')

# --- DEVICE MANIPULATION TRICKS ---
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

class SwarmTracker:
    def __init__(self, total_size, num_bots):
        self.total_size = total_size
        self.num_bots = num_bots
        self.bot_progress = [0] * num_bots
        self.start_time = time.time()
        self.lock = asyncio.Lock()
        self.last_ui = 0

    async def update(self, bot_index, size):
        async with self.lock:
            self.bot_progress[bot_index] += size
            now = time.time()
            if now - self.last_ui >= 1.0:
                self.last_ui = now
                self.display()

    def display(self):
        elapsed = time.time() - self.start_time
        total_downloaded = sum(self.bot_progress)
        overall_speed = (total_downloaded / 1024 / 1024) / max(elapsed, 0.1)
        percent = (total_downloaded / self.total_size) * 100
        
        sys.stdout.write(f"\033[K") # Clear line
        sys.stdout.write(f"\r🌪️  SWARM: {percent:5.1f}% | Total Speed: {overall_speed:6.2f} MB/s\n")
        for i, prog in enumerate(self.bot_progress):
            bot_speed = (prog / 1024 / 1024) / max(elapsed, 0.1)
            sys.stdout.write(f" └─ Bot {i}: {prog//1024//1024:4}MB ({bot_speed:5.2f} MB/s)\n")
        
        # Move cursor back up to overwrite next time
        sys.stdout.write(f"\033[{self.num_bots + 1}A")
        sys.stdout.flush()

    def finalize(self):
        # Move cursor to bottom after finish
        sys.stdout.write(f"\033[{self.num_bots + 1}B\n✅ Swarm Download Complete.\n")
        sys.stdout.flush()

async def download_segment(bot_index, client, location, start_offset, end_offset, fd, tracker):
    sem = asyncio.Semaphore(CONCURRENT_PER_BOT)
    
    async def download_chunk(offset, limit):
        async with sem:
            await asyncio.sleep(random.uniform(0.02, 0.1)) # Anti-detection jitter
            for attempt in range(12): 
                try:
                    result = await client(GetFileRequest(location, offset, limit))
                    if result and result.bytes:
                        os.pwrite(fd, result.bytes, offset)
                        await tracker.update(bot_index, len(result.bytes))
                        return
                except errors.FloodWaitError as e:
                    await asyncio.sleep(e.seconds + 2)
                except Exception:
                    await asyncio.sleep(1.5 ** attempt) 

    tasks = []
    for offset in range(start_offset, end_offset, CHUNK_SIZE):
        limit = min(CHUNK_SIZE, end_offset - offset)
        tasks.append(download_chunk(offset, limit))
    await asyncio.gather(*tasks)

async def multi_bot_download(links_data, file_path):
    print(f"🔥 [INIT] Starting swarm with {len(BOT_TOKENS)} sessions...")
    clients = []
    fd = None
    try:
        for i, token in enumerate(BOT_TOKENS):
            device = random.choice(DEVICE_MODELS)
            c = TelegramClient(
                f'session_{i}', TG_API_ID, TG_API_HASH,
                device_model=device[0], system_version=device[1], app_version=device[2]
            )
            await c.start(bot_token=token)
            clients.append(c)
            await asyncio.sleep(random.uniform(2, 4)) # Staggered login

        parts = [p for p in links_data.strip('/').split('/') if p]
        msg_id, chat_id = int(parts[-1]), int(f"-100{parts[parts.index('c')+1]}")
        primary_msg = await clients[0].get_messages(chat_id, ids=msg_id)
        
        file_size = primary_msg.file.size
        location = utils.get_input_location(primary_msg.media)
        
        # Create file and ensure it is not sparse
        with open(file_path, "wb") as f_init:
            f_init.truncate(file_size)
        
        fd = os.open(file_path, os.O_RDWR | os.O_DIRECT if hasattr(os, 'O_DIRECT') else os.O_RDWR)
        tracker = SwarmTracker(file_size, len(clients))
        
        segment_size = math.ceil(file_size / len(clients))
        tasks = []
        for i, client in enumerate(clients):
            start = i * segment_size
            end = min(file_size, (i + 1) * segment_size)
            tasks.append(download_segment(i, client, location, start, end, fd, tracker))

        await asyncio.gather(*tasks)
        tracker.finalize()

    finally:
        if fd: os.close(fd)
        for c in clients: await c.disconnect()

def process_video_advanced(input_path):
    output_video = "ready_for_youtube.mp4"
    print(f"🎬 [FFMPEG] Starting bitstream repair and remuxing...")
    # Using more robust flags to ensure YouTube doesn't 'abandon' the process
    # -err_detect ignore_err helps with small corruption chunks from telegram
    cmd = (f"ffmpeg -err_detect ignore_err -i '{input_path}' "
           f"-c copy -map 0:v:0 -map 0:a? -ignore_unknown "
           f"-movflags +faststart+delay_moov -y '{output_video}'")
    
    out, err, code = run_command(cmd)
    if code == 0 and os.path.exists(output_video) and os.path.getsize(output_video) > (os.path.getsize(input_path) * 0.9):
        print("✅ [FFMPEG] Success.")
        return output_video
    
    print(f"⚠️ [FFMPEG] Remux failed or file shrunken too much. Uploading raw.")
    return input_path

def upload_to_youtube(video_path, title):
    print(f"📤 [YOUTUBE] Authenticating and starting upload...")
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
        
        media = MediaFileUpload(video_path, chunksize=1024*1024*15, resumable=True)
        request = youtube.videos().insert(
            part="snippet,status",
            body={
                'snippet': {'title': title[:95], 'categoryId': '24', 'description': 'Swarm Uploaded'},
                'status': {'privacyStatus': 'private', 'selfDeclaredMadeForKids': False}
            },
            media_body=media
        )
        
        response = None
        while response is None:
            status, response = request.next_chunk()
            if status:
                print(f"\r📤 YouTube Progress: {int(status.progress() * 100)}%", end="")
        
        print(f"\n✨ [YOUTUBE] SUCCESS! ID: {response['id']}")
    except Exception as e:
        print(f"\n🔴 [ERROR] YouTube: {e}")

async def main():
    if len(sys.argv) < 2: return
    links = sys.argv[1].split(',')
    for link in links:
        raw_file = f"swarm_dn_{int(time.time())}.mkv"
        try:
            await multi_bot_download(link, raw_file)
            
            # Diagnostic check
            actual_size = os.path.getsize(raw_file)
            print(f"🔍 [DIAG] Downloaded file size: {actual_size / 1024 / 1024:.2f} MB")
            
            final_path = process_video_advanced(raw_file)
            upload_to_youtube(final_path, os.path.basename(raw_file))
            
            for f in [raw_file, final_path]:
                if f and os.path.exists(f) and "ready" in f: os.remove(f)
        except Exception as e:
            print(f"❌ [FATAL] {e}")

if __name__ == '__main__':
    asyncio.run(main())
