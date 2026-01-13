import os
import sys
import time
import asyncio
import subprocess
import json
import re
import requests
import math
import io
from telethon import TelegramClient, errors, utils
from telethon.tl.types import InputDocumentFileLocation
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials
import googleapiclient.errors

# --- EXTREME SPEED CONFIGURATION ---
YOUTUBE_SCOPES = ['https://www.googleapis.com/auth/youtube.force-ssl']
GEMINI_MODEL = "gemini-2.5-flash-preview-09-2025"

# Maximum Parallelism for GitHub Runners (Network Optimized)
PARALLEL_WORKERS = 32  
CHUNK_SIZE_KB = 1024   

# Fetching API Keys
TG_BOT_TOKEN = os.environ.get('TG_BOT_TOKEN', '').strip()
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
        self.last_ui_update = 0
        self.downloaded_bytes = 0

    def update(self, current_inc=0, abs_current=None):
        if abs_current is not None:
            self.downloaded_bytes = abs_current
        else:
            self.downloaded_bytes += current_inc

        now = time.time()
        if now - self.last_ui_update < 0.4:
            return
        
        self.last_ui_update = now
        elapsed = now - self.start_time
        safe_elapsed = max(elapsed, 0.01)
        
        speed = (self.downloaded_bytes / 1024 / 1024) / safe_elapsed
        percentage = (self.downloaded_bytes / self.total_size) * 100
        display_percentage = min(percentage, 100.0)
        
        bar_length = 25
        filled = int(bar_length * display_percentage // 100)
        bar = '█' * filled + '-' * (bar_length - filled)
        
        status = (
            f"\r{self.prefix} [{bar}] {display_percentage:5.1f}% | "
            f"{self.downloaded_bytes/1024/1024:7.2f}/{self.total_size/1024/1024:7.2f} MB | "
            f"⚡ {speed:5.2f} MB/s"
        )
        sys.stdout.write(status)
        sys.stdout.flush()

async def fast_download(client, message, file_path):
    print(f"📡 Initializing Hyper-Speed Engine (Workers: {PARALLEL_WORKERS})...")
    
    try:
        import cryptg
        print("⚡ Cryptg detected: Decryption acceleration active.")
    except ImportError:
        print("⚠️ Cryptg NOT detected: CPU limited.")

    media = message.media
    if not media or not hasattr(media, 'document'):
        print("⚠️ Standard download fallback.")
        await client.download_media(message, file_path)
        return

    file_size = media.document.size
    tracker = ProgressTracker(file_size, prefix='📥 Downloading')
    
    chunk_size = CHUNK_SIZE_KB * 1024
    total_chunks = math.ceil(file_size / chunk_size)
    queue = asyncio.Queue()
    for i in range(total_chunks): queue.put_nowait(i)

    with open(file_path, 'wb') as f:
        f.truncate(file_size)
        file_lock = asyncio.Lock()
        
        async def worker():
            while not queue.empty():
                try:
                    chunk_index = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

                offset = chunk_index * chunk_size
                current_limit = min(chunk_size, file_size - offset)
                
                buffer = io.BytesIO()
                bytes_received = 0
                try:
                    async for chunk in client.iter_download(media, offset=offset, limit=current_limit, request_size=chunk_size):
                        remaining = current_limit - bytes_received
                        if remaining <= 0: break
                        to_write = chunk[:remaining]
                        buffer.write(to_write)
                        bytes_received += len(to_write)
                        tracker.update(current_inc=len(to_write))
                    
                    data = buffer.getvalue()
                    async with file_lock:
                        f.seek(offset)
                        f.write(data)
                except Exception:
                    await queue.put(chunk_index)
                finally:
                    queue.task_done()

        start_time = time.time()
        worker_tasks = [asyncio.create_task(worker()) for _ in range(min(PARALLEL_WORKERS, total_chunks))]
        await asyncio.gather(*worker_tasks)
        
    duration = time.time() - start_time
    print(f"\n✅ Finished! Average Speed: {(file_size/1024/1024)/duration:.2f} MB/s")
    time.sleep(0.5)

def get_video_codec(file_path):
    cmd = f"ffprobe -v error -select_streams v:0 -show_entries stream=codec_name -of csv=p=0 '{file_path}'"
    output, _, _ = run_command(cmd)
    return output.strip().lower()

async def get_metadata(filename):
    print(f"🤖 AI is crafting cinematic metadata (CLEAN TITLE MODE)...")
    clean_name = os.path.splitext(filename)[0].replace('_', ' ').replace('.', ' ')
    
    if GEMINI_API_KEY:
        gemini_url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
        
        system_instruction = (
            "You are a professional YouTube metadata expert. "
            "IMPORTANT RULES for TITLE:\n"
            "1. NO 'Trailer', 'Teaser', 'Clip', 'Official', or 'Promo' words.\n"
            "2. If it's a Movie: Use 'Movie Name (Year)'.\n"
            "3. If it's a TV Show: Use 'Show Name - S00E00 - Episode Title'.\n"
            "4. NEVER include file extensions or group tags (like 10bit, x265, PSA).\n"
            "5. The title must be clean and look like a real movie title."
        )
        
        prompt = f"Extract the clean movie/show title from this filename: '{filename}'. Return JSON with 'title', 'description', 'tags' (list)."
        
        try:
            payload = {
                "contents": [{"parts": [{"text": prompt}]}],
                "systemInstruction": {"parts": [{"text": system_instruction}]},
                "generationConfig": {"responseMimeType": "application/json"}
            }
            res = requests.post(gemini_url, json=payload, timeout=20)
            if res.status_code == 200:
                data = json.loads(res.json()['candidates'][0]['content']['parts'][0]['text'])
                data['title'] = re.sub(r'(?i)\b(trailer|teaser|official|clip|promo|10bit|x265|hevc|web-dl)\b', '', data['title']).strip()
                return data
        except: pass
    return {"title": clean_name, "description": "Auto-uploaded content.", "tags": ["video"]}

def process_video_advanced(input_path):
    output_video = f"final_{os.path.basename(input_path)}.mp4"
    codec = get_video_codec(input_path)
    
    print(f"🛠️  Detected Codec: {codec.upper()}")
    
    # If it's HEVC (x265), we transcode to x264 to prevent "missing frames" on YouTube
    # If it's x264 already, we just copy to save time
    if "hevc" in codec or "h265" in codec:
        print("⚠️ x265 detected: Converting to x264 for YouTube compatibility (Fast Mode)...")
        # -preset ultrafast is used to minimize conversion time in GitHub Actions
        # -crf 23 maintains decent quality
        cmd = (
            f"ffmpeg -i '{input_path}' -map 0:v:0 -map 0:a:0? "
            f"-c:v libx264 -preset ultrafast -crf 23 -vsync passthrough "
            f"-c:a aac -b:a 128k -movflags +faststart -y '{output_video}'"
        )
    else:
        print("✅ Standard codec: Using fast stream copy...")
        cmd = (
            f"ffmpeg -i '{input_path}' -map 0:v:0 -map 0:a:0? "
            f"-c copy -movflags +faststart -y '{output_video}'"
        )
    
    run_command(cmd)
    
    if not os.path.exists(output_video) or os.path.getsize(output_video) < 5000:
        return input_path, None

    sub_file = "subs.srt"
    run_command(f"ffmpeg -i '{input_path}' -map 0:s:0? -c:s srt '{sub_file}' -y")
    has_subs = os.path.exists(sub_file) and os.path.getsize(sub_file) > 100
    
    return output_video, (sub_file if has_subs else None)

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
        
        print(f"🚀 Pushing to YouTube: {metadata.get('title')}")
        body = {
            'snippet': {
                'title': metadata.get('title', 'Video')[:95],
                'description': metadata.get('description', ''),
                'tags': metadata.get('tags', ['video']),
                'categoryId': '24'
            },
            'status': {'privacyStatus': 'private'}
        }
        
        media = MediaFileUpload(video_path, chunksize=1024*1024*15, resumable=True)
        request = youtube.videos().insert(part="snippet,status", body=body, media_body=media)
        
        tracker = ProgressTracker(os.path.getsize(video_path), prefix='📤 Uploading  ')
        response = None
        while response is None:
            status, response = request.next_chunk()
            if status: tracker.update(abs_current=status.resumable_progress)

        video_id = response['id']
        print(f"\n✨ SUCCESS: https://youtu.be/{video_id}")

        if sub_path:
            try:
                youtube.captions().insert(
                    part="snippet",
                    body={'snippet': {'videoId': video_id, 'language': 'en', 'name': 'English'}},
                    media_body=MediaFileUpload(sub_path)
                ).execute()
                print("✅ Subtitles uploaded.")
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
        filename = message.file.name or f"media_{msg_id}.mkv"
        raw_file = f"raw_{msg_id}_{filename}"
        
        await fast_download(client, message, raw_file)
        metadata = await get_metadata(filename)
        final_video, sub_file = process_video_advanced(raw_file)
        
        upload_to_youtube(final_video, metadata, sub_file)

        for f in [raw_file, final_video, sub_file]:
            if f and os.path.exists(f): 
                try: os.remove(f)
                except: pass
    except Exception as e:
        print(f"\n❌ Link Failure: {e}")

async def main():
    if len(sys.argv) < 2: return
    links = sys.argv[1].split(',')
    client = TelegramClient('bot_session', os.environ['TG_API_ID'], os.environ['TG_API_HASH'])
    await client.start(bot_token=TG_BOT_TOKEN)
    for link in links: await process_link(client, link)
    await client.disconnect()

if __name__ == '__main__':
    asyncio.run(main())
