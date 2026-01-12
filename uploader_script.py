import os
import sys
import time
import asyncio
import subprocess
from telethon import TelegramClient, errors, utils
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials

# --- CONFIGURATION ---
BOT_TOKENS = [t.strip() for t in os.environ.get('TG_BOT_TOKEN', '').split(',') if t.strip()]
TG_API_ID = os.environ.get('TG_API_ID')
TG_API_HASH = os.environ.get('TG_API_HASH')
YOUTUBE_SCOPES = ['https://www.googleapis.com/auth/youtube.force-ssl']

class SwarmTracker:
    def __init__(self, total_size):
        self.total_size = total_size
        self.downloaded = 0
        self.start_time = time.time()
        self.lock = asyncio.Lock()

    async def update(self, chunk_size):
        async with self.lock:
            self.downloaded += chunk_size
            elapsed = time.time() - self.start_time
            speed = (self.downloaded / 1024 / 1024) / max(elapsed, 0.1)
            percent = (self.downloaded / self.total_size) * 100
            if int(percent) % 5 == 0: # Log every 5%
                print(f"📊 {percent:.1f}% | Speed: {speed:.2f} MB/s", flush=True)

async def worker(bot_token, chat_id, msg_id, start, end, file_path, tracker):
    client = TelegramClient(f'bot_{bot_token[:8]}', TG_API_ID, TG_API_HASH)
    try:
        await client.start(bot_token=bot_token)
        message = await client.get_messages(chat_id, ids=msg_id)
        
        # Open file in 'r+b' to write at specific offsets
        with open(file_path, "rb+") as f:
            # 512KB is the standard Telegram chunk size
            chunk_size = 512 * 1024
            offset = start
            while offset < end:
                limit = min(chunk_size, end - offset)
                result = await client.download_media(message, bytes, offset=offset, limit=limit)
                if result:
                    f.seek(offset)
                    f.write(result)
                    offset += len(result)
                    await tracker.update(len(result))
    except Exception as e:
        print(f"❌ Worker Error: {e}", flush=True)
    finally:
        await client.disconnect()

async def download_swarm(link, output_path):
    # Parse TG Link
    parts = link.strip('/').split('/')
    msg_id = int(parts[-1])
    chat_id = int(f"-100{parts[-2]}") if 'c/' in link else parts[-2]

    # Get file metadata using first bot
    init_bot = TelegramClient('init_sess', TG_API_ID, TG_API_HASH)
    await init_bot.start(bot_token=BOT_TOKENS[0])
    msg = await init_bot.get_messages(chat_id, ids=msg_id)
    total_size = msg.file.size
    print(f"📂 Downloading: {msg.file.name} ({total_size / 1e6:.2f} MB)", flush=True)
    await init_bot.disconnect()

    # Pre-allocate file
    with open(output_path, "wb") as f:
        f.truncate(total_size)

    tracker = SwarmTracker(total_size)
    chunk_per_bot = total_size // len(BOT_TOKENS)
    
    tasks = []
    for i, token in enumerate(BOT_TOKENS):
        start = i * chunk_per_bot
        end = (i + 1) * chunk_per_bot if i != len(BOT_TOKENS) - 1 else total_size
        tasks.append(worker(token, chat_id, msg_id, start, end, output_path, tracker))
    
    await asyncio.gather(*tasks)

def fix_and_prepare_video(input_path):
    """
    Prevents 'Processing Abandoned' by ensuring the container 
    is valid and MOOV atom is at the front.
    """
    output = "final_upload.mp4"
    print("🎬 Finalizing video container for YouTube...", flush=True)
    # Re-muxing into MP4 with faststart (YouTube preferred)
    cmd = [
        "ffmpeg", "-y", "-i", input_path, 
        "-c", "copy", "-movflags", "+faststart", 
        "-f", "mp4", output
    ]
    subprocess.run(cmd, capture_output=True)
    return output if os.path.exists(output) else input_path

def upload_to_youtube(file_path):
    print("📤 Starting YouTube Upload...", flush=True)
    creds = Credentials(
        token=None,
        refresh_token=os.environ.get('YOUTUBE_REFRESH_TOKEN'),
        token_uri='https://oauth2.googleapis.com/token',
        client_id=os.environ.get('YOUTUBE_CLIENT_ID'),
        client_secret=os.environ.get('YOUTUBE_CLIENT_SECRET')
    )
    youtube = build('youtube', 'v3', credentials=creds)
    
    body = {
        'snippet': {'title': 'Swarm Uploaded Video', 'description': 'Automated Upload'},
        'status': {'privacyStatus': 'private', 'selfDeclaredMadeForKids': False}
    }
    
    media = MediaFileUpload(file_path, chunksize=1024*1024*5, resumable=True)
    request = youtube.videos().insert(part="snippet,status", body=body, media_body=media)
    
    response = None
    while response is None:
        status, response = request.next_chunk()
        if status:
            print(f" 📤 {int(status.progress() * 100)}% uploaded", flush=True)
    print(f"✅ Upload Complete: https://youtu.be/{response['id']}")

async def main():
    if len(sys.argv) < 2: return
    video_link = sys.argv[1]
    raw_file = "temp_raw.mkv"
    
    await download_swarm(video_link, raw_file)
    ready_file = fix_and_prepare_video(raw_file)
    upload_to_youtube(ready_file)
    
    # Cleanup
    for f in [raw_file, ready_file]:
        if os.path.exists(f): os.remove(f)

if __name__ == "__main__":
    asyncio.run(main())
