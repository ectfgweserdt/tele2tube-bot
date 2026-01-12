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
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials
import googleapiclient.errors

# Force output to show immediately
print("🚀 [SYSTEM] Script started. Initializing environment...", flush=True)

# --- PERFORMANCE ENGINE ---
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    print("🚀 [SYSTEM] uvloop enabled.", flush=True)
except ImportError:
    pass

YOUTUBE_SCOPES = ['https://www.googleapis.com/auth/youtube.force-ssl']
BOT_TOKENS = [t.strip() for t in os.environ.get('TG_BOT_TOKEN', '').split(',') if t.strip()]
TG_API_ID = os.environ.get('TG_API_ID')
TG_API_HASH = os.environ.get('TG_API_HASH')

DEVICE_MODELS = [
    ("Samsung Galaxy S23", "Android 13", "v10.3.2"),
    ("iPhone 14 Pro", "iOS 16.5", "v9.6.1"),
    ("Google Pixel 7", "Android 13", "v10.0.0"),
    ("iPad Air 5", "iOS 16.1", "v9.4.0")
]

class SwarmTracker:
    def __init__(self, total_size, num_bots):
        self.total_size = total_size
        self.num_bots = num_bots
        self.bot_progress = [0] * num_bots
        self.start_time = time.time()
        self.last_log_time = time.time()
        self.lock = asyncio.Lock()

    async def update(self, bot_index, current):
        async with self.lock:
            self.bot_progress[bot_index] = current
            now = time.time()
            if now - self.last_log_time >= 5.0:
                self.last_log_time = now
                self.log_status()

    def log_status(self):
        elapsed = time.time() - self.start_time
        total_dl = sum(self.bot_progress)
        speed = (total_dl / 1024 / 1024) / max(elapsed, 0.1)
        percent = (total_dl / self.total_size) * 100
        print(f"📊 Progress: {percent:5.1f}% | Speed: {speed:5.2f} MB/s | {total_dl//1024//1024}/{self.total_size//1024//1024} MB", flush=True)

async def bot_worker(bot_index, token, chat_id, msg_id, start, end, file_path, tracker):
    device = random.choice(DEVICE_MODELS)
    client = TelegramClient(f'bot_{bot_index}', TG_API_ID, TG_API_HASH, device_model=device[0])
    try:
        await client.start(bot_token=token)
        
        # Internal progress callback
        async def progress_callback(current, total):
            await tracker.update(bot_index, current)

        # Download specific range
        # Note: end is the offset to stop at
        message = await client.get_messages(chat_id, ids=msg_id)
        
        # We use a custom downloader to handle ranges
        # Telethon doesn't support 'offset' directly in download_media easily for bots,
        # so we fallback to a more stable chunked downloader
        from telethon.tl.functions.upload import GetFileRequest
        
        fd = os.open(file_path, os.O_RDWR)
        chunk_size = 512 * 1024
        current_offset = start
        
        while current_offset < end:
            limit = min(chunk_size, end - current_offset)
            for attempt in range(5):
                try:
                    result = await client(GetFileRequest(
                        utils.get_input_location(message.media),
                        offset=current_offset,
                        limit=limit
                    ))
                    if result and result.bytes:
                        os.pwrite(fd, result.bytes, current_offset)
                        current_offset += len(result.bytes)
                        await tracker.update(bot_index, current_offset - start)
                        break
                except errors.FloodWaitError as e:
                    await asyncio.sleep(e.seconds)
                except Exception:
                    await asyncio.sleep(2)
            
            if current_offset >= end: break

        os.close(fd)
    finally:
        await client.disconnect()

async def multi_bot_download(link, file_path):
    print(f"🔥 [INIT] Initializing swarm with {len(BOT_TOKENS)} bots...", flush=True)
    
    # Parse link
    parts = [p for p in link.strip('/').split('/') if p]
    msg_id, chat_id = int(parts[-1]), int(f"-100{parts[parts.index('c')+1]}")
    
    # Use one client to get file info
    temp_client = TelegramClient('info_session', TG_API_ID, TG_API_HASH)
    await temp_client.start(bot_token=BOT_TOKENS[0])
    msg = await temp_client.get_messages(chat_id, ids=msg_id)
    file_size = msg.file.size
    print(f"📂 File Found: {msg.file.name} ({file_size//1024//1024} MB)", flush=True)
    await temp_client.disconnect()

    # Pre-allocate
    with open(file_path, "wb") as f:
        f.truncate(file_size)
    
    tracker = SwarmTracker(file_size, len(BOT_TOKENS))
    seg = math.ceil(file_size / len(BOT_TOKENS))
    
    tasks = []
    for i, token in enumerate(BOT_TOKENS):
        start = i * seg
        end = min(file_size, (i + 1) * seg)
        tasks.append(bot_worker(i, token, chat_id, msg_id, start, end, file_path, tracker))
        # Start bots slightly staggered to avoid IP spam
        await asyncio.sleep(1.5)

    print(f"🚀 [ACTION] Swarm download active...", flush=True)
    await asyncio.gather(*tasks)
    print("✅ Swarm Download Finished.", flush=True)

def process_video_advanced(input_path):
    output = "ready.mp4"
    print(f"🎬 [FFMPEG] Remuxing...", flush=True)
    # Added -max_muxing_queue_size to prevent freezes during remux
    cmd = f"ffmpeg -i '{input_path}' -c copy -map 0 -movflags +faststart -max_muxing_queue_size 1024 -y '{output}'"
    subprocess.run(cmd, shell=True, capture_output=True)
    return output if os.path.exists(output) else input_path

def upload_to_youtube(path):
    print(f"📤 [YOUTUBE] Initializing upload...", flush=True)
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
        media = MediaFileUpload(path, chunksize=1024*1024*10, resumable=True)
        request = youtube.videos().insert(
            part="snippet,status",
            body={'snippet': {'title': 'Swarm Upload', 'categoryId': '24'}, 'status': {'privacyStatus': 'private'}},
            media_body=media
        )
        response = None
        while response is None:
            status, response = request.next_chunk()
            if status: print(f" 📤 Upload: {int(status.progress() * 100)}%", flush=True)
        print(f"✨ SUCCESS: https://youtu.be/{response['id']}", flush=True)
    except Exception as e:
        print(f"❌ YouTube Error: {e}", flush=True)

async def main():
    if len(sys.argv) < 2: return
    link_arg = sys.argv[1]
    for link in link_arg.split(','):
        raw = f"dl_{int(time.time())}.mkv"
        try:
            await multi_bot_download(link, raw)
            processed = process_video_advanced(raw)
            upload_to_youtube(processed)
            for f in [raw, processed]:
                if os.path.exists(f): os.remove(f)
        except Exception as e:
            print(f"❌ Critical Error: {e}", flush=True)

if __name__ == '__main__':
    asyncio.run(main())
