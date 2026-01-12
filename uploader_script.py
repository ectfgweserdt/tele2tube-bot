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
    print("🚀 [SYSTEM] uvloop enabled.")
except ImportError:
    pass

YOUTUBE_SCOPES = ['https://www.googleapis.com/auth/youtube.force-ssl']
CONCURRENT_PER_BOT = 8 
CHUNK_SIZE = 512 * 1024 # 512KB

BOT_TOKENS = [t.strip() for t in os.environ.get('TG_BOT_TOKEN', '').split(',') if t.strip()]
TG_API_ID = os.environ.get('TG_API_ID')
TG_API_HASH = os.environ.get('TG_API_HASH')

DEVICE_MODELS = [
    ("Samsung Galaxy S23", "Android 13", "v10.3.2"),
    ("iPhone 14 Pro", "iOS 16.5", "v9.6.1"),
    ("Google Pixel 7", "Android 13", "v10.0.0"),
    ("iPad Air 5", "iOS 16.1", "v9.4.0")
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
        self.last_log_time = 0

    async def update(self, bot_index, size):
        async with self.lock:
            self.bot_progress[bot_index] += size
            now = time.time()
            if now - self.last_log_time >= 5.0:
                self.last_log_time = now
                self.log_status()

    def log_status(self):
        elapsed = time.time() - self.start_time
        total_dl = sum(self.bot_progress)
        speed = (total_dl / 1024 / 1024) / max(elapsed, 0.1)
        percent = (total_dl / self.total_size) * 100
        
        print(f"📊 Progress: {percent:5.1f}% | Global Speed: {speed:5.2f} MB/s | {total_dl//1024//1024}/{self.total_size//1024//1024} MB")
        for i, prog in enumerate(self.bot_progress):
            if prog > 0:
                bot_speed = (prog / 1024 / 1024) / max(elapsed, 0.1)
                print(f"   > Bot-{i}: {prog//1024//1024}MB ({bot_speed:.2f} MB/s)")
        sys.stdout.flush()

async def download_segment(bot_index, client, location, start_offset, end_offset, fd, tracker):
    sem = asyncio.Semaphore(CONCURRENT_PER_BOT)
    for offset in range(start_offset, end_offset, CHUNK_SIZE):
        limit = min(CHUNK_SIZE, end_offset - offset)
        async with sem:
            # Jitter to avoid simultaneous requests per bot
            await asyncio.sleep(random.uniform(0.01, 0.05))
            for attempt in range(12):
                try:
                    result = await client(GetFileRequest(location, offset, limit))
                    if result and result.bytes:
                        os.pwrite(fd, result.bytes, offset)
                        await tracker.update(bot_index, len(result.bytes))
                        break
                except errors.FloodWaitError as e:
                    await asyncio.sleep(e.seconds + 2)
                except Exception:
                    await asyncio.sleep(2 ** attempt / 2)

async def multi_bot_download(link, file_path):
    print(f"🔥 [INIT] Starting swarm with {len(BOT_TOKENS)} bots...")
    clients = []
    try:
        # SEQUENTIAL START TO PREVENT AUTH_KEY ERRORS
        for i, token in enumerate(BOT_TOKENS):
            device = random.choice(DEVICE_MODELS)
            # Use distinct session names and aggressive connection retries
            c = TelegramClient(
                f'swarm_bot_{i}_{int(time.time())}', 
                TG_API_ID, TG_API_HASH, 
                device_model=device[0],
                connection_retries=15,
                retry_delay=3
            )
            
            # Initial jitter before connecting to avoid "invalid nonce" collision
            await asyncio.sleep(random.uniform(1.0, 5.0))
            
            try:
                await c.start(bot_token=token)
                clients.append(c)
                print(f"✅ Bot {i} Online ({device[0]})")
                # Wait for the session to be fully established before next one
                await asyncio.sleep(4) 
            except Exception as e:
                print(f"⚠️ Bot {i} Auth Failed: {e}")

        if not clients:
            raise Exception("No bots were able to connect.")

        parts = [p for p in link.strip('/').split('/') if p]
        msg_id, chat_id = int(parts[-1]), int(f"-100{parts[parts.index('c')+1]}")
        msg = await clients[0].get_messages(chat_id, ids=msg_id)
        
        file_size = msg.file.size
        location = utils.get_input_location(msg.media)
        print(f"📂 File: {msg.file.name} ({file_size//1024//1024} MB)")

        with open(file_path, "wb") as f: f.truncate(file_size)
        fd = os.open(file_path, os.O_RDWR)
        tracker = SwarmTracker(file_size, len(clients))
        
        seg = math.ceil(file_size / len(clients))
        tasks = []
        for i, client in enumerate(clients):
            tasks.append(download_segment(i, client, location, i*seg, min(file_size, (i+1)*seg), fd, tracker))
        
        await asyncio.gather(*tasks)
        os.close(fd)
        print("✅ Download Finished.")
    finally:
        for c in clients: 
            try: await c.disconnect()
            except: pass

def process_video_advanced(input_path):
    output = "ready.mp4"
    print(f"🎬 [FFMPEG] Remuxing...")
    # Use -map 0 to ensure all stream references are preserved for YouTube ingest
    cmd = f"ffmpeg -i '{input_path}' -c copy -map 0 -movflags +faststart -y '{output}'"
    run_command(cmd)
    return output if os.path.exists(output) else input_path

def upload_to_youtube(path):
    print(f"📤 [YOUTUBE] Starting upload...")
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
        media = MediaFileUpload(path, chunksize=1024*1024*15, resumable=True)
        request = youtube.videos().insert(
            part="snippet,status",
            body={'snippet': {'title': 'Swarm Upload', 'categoryId': '24'}, 'status': {'privacyStatus': 'private'}},
            media_body=media
        )
        response = None
        while response is None:
            status, response = request.next_chunk()
            if status: print(f" 📤 Upload Progress: {int(status.progress() * 100)}%")
        print(f"✨ SUCCESS: https://youtu.be/{response['id']}")
    except Exception as e:
        print(f"❌ YouTube Error: {e}")

async def main():
    if len(sys.argv) < 2: return
    for link in sys.argv[1].split(','):
        raw = f"dl_{int(time.time())}.mkv"
        try:
            await multi_bot_download(link, raw)
            processed = process_video_advanced(raw)
            upload_to_youtube(processed)
            for f in [raw, processed]:
                if os.path.exists(f): os.remove(f)
        except Exception as e:
            print(f"❌ Main Loop Error: {e}")

if __name__ == '__main__':
    asyncio.run(main())
