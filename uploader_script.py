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
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials
import googleapiclient.errors

# --- STABLE PERFORMANCE CONFIG ---
# 8-12 workers is the limit for Telegram before they trigger 'FloodWait'
# Using 1MB chunks for maximum throughput per request
MAX_CONCURRENT_CHUNKS = 20
CHUNK_SIZE = 1024 * 1024 

YOUTUBE_SCOPES = ['https://www.googleapis.com/auth/youtube.force-ssl']
GEMINI_MODEL = "gemini-2.5-flash-preview-09-2025"

def run_command(command):
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    output, error = process.communicate()
    return output.decode(), error.decode(), process.returncode

class ProgressTracker:
    def __init__(self, total_size, prefix='🚀'):
        self.total_size = total_size
        self.start_time = time.time()
        self.prefix = prefix
        self.last_ui_update = 0
        self.current_bytes = 0

    def update(self, inc_bytes):
        self.current_bytes += inc_bytes
        now = time.time()
        if now - self.last_ui_update < 0.5:
            return
        
        self.last_ui_update = now
        elapsed = max(now - self.start_time, 0.01)
        speed = (self.current_bytes / 1024 / 1024) / elapsed
        
        # Prevent percentage > 100 display errors
        percentage = min((self.current_bytes / self.total_size) * 100, 100.0)
        
        bar = '█' * int(percentage // 4) + '-' * (25 - int(percentage // 4))
        status = (
            f"\r{self.prefix} [{bar}] {percentage:5.1f}% | "
            f"{self.current_bytes/1024/1024:7.2f}/{self.total_size/1024/1024:7.2f} MB | "
            f"⚡ {speed:5.2f} MB/s"
        )
        sys.stdout.write(status)
        sys.stdout.flush()

async def fast_download(client, message, file_path):
    print(f"📡 Stability Mode: {MAX_CONCURRENT_CHUNKS} Workers | 1MB Chunks")
    
    file_size = message.file.size
    tracker = ProgressTracker(file_size, prefix='📥 Downloading')
    
    # Pre-allocate file on disk
    with open(file_path, 'wb') as f:
        f.truncate(file_size)

    # Use a Semaphore to prevent Telegram FloodWait errors
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_CHUNKS)
    
    async def download_chunk(offset, size):
        async with semaphore:
            for attempt in range(5):
                try:
                    # Direct chunk fetch
                    chunk = await client.download_dtm(
                        message.media.document,
                        offset=offset,
                        limit=size
                    )
                    if chunk:
                        # Thread-safe write isn't needed with standard sync file in this specific loop
                        with open(file_path, 'r+b') as f:
                            f.seek(offset)
                            f.write(chunk)
                        tracker.update(len(chunk))
                        return
                except errors.FloodWaitError as e:
                    await asyncio.sleep(e.seconds + 1)
                except Exception:
                    await asyncio.sleep(2 ** attempt)
            print(f"\n⚠️ Failed to download chunk at {offset}")

    total_chunks = math.ceil(file_size / CHUNK_SIZE)
    tasks = []
    for i in range(total_chunks):
        offset = i * CHUNK_SIZE
        limit = min(CHUNK_SIZE, file_size - offset)
        tasks.append(download_chunk(offset, limit))

    await asyncio.gather(*tasks)
    print("\n✅ Download Complete.")

async def get_metadata(filename):
    print("🤖 AI: Cleaning Metadata...")
    api_key = os.environ.get('GEMINI_API_KEY', '').strip()
    clean_name = os.path.splitext(filename)[0].replace('_', ' ').replace('.', ' ')
    
    if api_key:
        try:
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={api_key}"
            prompt = (
                f"For the file '{filename}', provide YouTube metadata in JSON format (title, description, tags). "
                "The title must be clean, human-readable, and exclude words like 'Trailer', 'Teaser', 'Official', or tech specs like 'x265'."
            )
            payload = {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"responseMimeType": "application/json"}
            }
            res = requests.post(url, json=payload, timeout=15)
            if res.status_code == 200:
                return json.loads(res.json()['candidates'][0]['content']['parts'][0]['text'])
        except: pass
    
    return {"title": clean_name, "description": "Auto-uploaded content.", "tags": ["video"]}

def process_video_advanced(input_path):
    output_video = f"ready_{os.path.basename(input_path)}.mp4"
    
    # Analyze codec
    check_cmd = f"ffprobe -v error -select_streams v:0 -show_entries stream=codec_name -of csv=p=0 '{input_path}'"
    codec, _, _ = run_command(check_cmd)
    codec = codec.strip().lower()

    if "hevc" in codec or "h265" in codec:
        print("🛠️ Converting x265 to x264 (Lightning Ultrafast)...")
        # Fixing frame sync while maintaining high speed
        cmd = (
            f"ffmpeg -i '{input_path}' -map 0:v:0 -map 0:a:0? "
            f"-c:v libx264 -preset ultrafast -crf 23 -c:a aac -b:a 128k "
            f"-movflags +faststart -y '{output_video}'"
        )
    else:
        print("⚡ Codec OK. Remuxing only...")
        cmd = f"ffmpeg -i '{input_path}' -map 0:v:0 -map 0:a:0? -c copy -movflags +faststart -y '{output_video}'"
    
    run_command(cmd)
    
    # Subtitle extraction
    sub_file = "subs.srt"
    run_command(f"ffmpeg -i '{input_path}' -map 0:s:0? -c:s srt '{sub_file}' -y")
    has_subs = os.path.exists(sub_file) and os.path.getsize(sub_file) > 100
    
    return output_video, (sub_file if has_subs else None)

def upload_to_youtube(video_path, metadata, sub_path):
    try:
        creds = Credentials(
            token=None,
            refresh_token=os.environ.get('YOUTUBE_REFRESH_TOKEN'),
            token_uri='https://oauth2.googleapis.com/token',
            client_id=os.environ.get('YOUTUBE_CLIENT_ID'),
            client_secret=os.environ.get('YOUTUBE_CLIENT_SECRET'),
            scopes=YOUTUBE_SCOPES
        )
        creds.refresh(Request())
        youtube = build('youtube', 'v3', credentials=creds)

        body = {
            'snippet': {
                'title': metadata.get('title', 'Video')[:95],
                'description': metadata.get('description', ''),
                'tags': metadata.get('tags', ['video']),
                'categoryId': '24'
            },
            'status': {'privacyStatus': 'private'}
        }

        media = MediaFileUpload(video_path, chunksize=1024*1024*10, resumable=True)
        request = youtube.videos().insert(part="snippet,status", body=body, media_body=media)

        print(f"📤 Uploading to YouTube: {body['snippet']['title']}")
        response = None
        tracker = ProgressTracker(os.path.getsize(video_path), prefix='📤 Uploading  ')
        while response is None:
            status, response = request.next_chunk()
            if status: tracker.update(status.resumable_progress - tracker.current_bytes)

        print(f"\n🎉 SUCCESS: https://youtu.be/{response['id']}")

        if sub_path:
            try:
                youtube.captions().insert(
                    part="snippet",
                    body={'snippet': {'videoId': response['id'], 'language': 'en', 'name': 'English'}},
                    media_body=MediaFileUpload(sub_path)
                ).execute()
            except: pass
            
    except Exception as e:
        print(f"\n🔴 YouTube Error: {e}")

async def process_link(client, link):
    try:
        parts = [p for p in link.strip('/').split('/') if p]
        msg_id = int(parts[-1])
        chat_val = parts[parts.index('c')+1] if 'c' in parts else parts[-2]
        chat_id = int(f"-100{chat_val}") if chat_val.isdigit() else chat_val
        
        message = await client.get_messages(chat_id, ids=msg_id)
        raw_file = f"download_{msg_id}.mkv"
        
        await fast_download(client, message, raw_file)
        metadata = await get_metadata(message.file.name or raw_file)
        final_video, sub_file = process_video_advanced(raw_file)
        
        upload_to_youtube(final_video, metadata, sub_file)

        # Cleanup
        for f in [raw_file, final_video, sub_file]:
            if f and os.path.exists(f): os.remove(f)
    except Exception as e:
        print(f"\n❌ Failure: {e}")

async def main():
    if len(sys.argv) < 2: return
    links = sys.argv[1].split(',')
    client = TelegramClient(
        'bot_session', 
        os.environ['TG_API_ID'], 
        os.environ['TG_API_HASH'],
        connection_retries=None
    )
    await client.start(bot_token=os.environ['TG_BOT_TOKEN'])
    for link in links: await process_link(client, link)
    await client.disconnect()

if __name__ == '__main__':
    asyncio.run(main())
