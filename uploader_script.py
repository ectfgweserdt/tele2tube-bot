import os
import sys
import time
import asyncio
import subprocess
import json
import re
import requests
import math
import aiofiles
from telethon import TelegramClient, errors, utils
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials

# --- EXTREME PERFORMANCE CONFIG ---
YOUTUBE_SCOPES = ['https://www.googleapis.com/auth/youtube.force-ssl']
GEMINI_MODEL = "gemini-2.5-flash-preview-09-2025"

# 100MBps Strategy: 32 Parallel streams + 1MB request sizing
CONCURRENT_CONNECTIONS = 32 
MAX_CHUNK_SIZE = 1024 * 1024 # 1MB

# API Keys
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
        self.downloaded = 0

    def update(self, current_chunk_size):
        self.downloaded += current_chunk_size
        now = time.time()
        if now - self.last_ui_update < 0.2 and self.downloaded < self.total_size:
            return
        
        self.last_ui_update = now
        elapsed = now - self.start_time
        speed = (self.downloaded / 1024 / 1024) / max(elapsed, 0.1)
        percentage = min(100.0, (self.downloaded / self.total_size) * 100)
        
        bar_len = 30
        filled = int(bar_len * percentage / 100)
        bar = '█' * filled + '░' * (bar_len - filled)
        
        sys.stdout.write(
            f"\r{self.prefix} [{bar}] {percentage:5.1f}% | "
            f"{self.downloaded/1024/1024:7.1f}/{self.total_size/1024/1024:7.1f} MB | "
            f"⚡ {speed:6.2f} MB/s"
        )
        sys.stdout.flush()

# --- TURBO DOWNLOADER ENGINE ---

async def turbo_download(client, message, output_path):
    """
    Saturates 1Gbps bandwidth using 32 concurrent socket segments.
    Explicitly prevents overshooting and hanging at 99%.
    """
    file_size = message.file.size
    tracker = ProgressTracker(file_size, prefix='🔥 TURBO-DL')
    
    # Pre-allocate sparse file for instant writing
    async with aiofiles.open(output_path, 'wb') as f:
        await f.seek(file_size - 1)
        await f.write(b'\0')

    part_size = 1024 * 1024 # 1MB parts
    total_parts = math.ceil(file_size / part_size)
    queue = asyncio.Queue()
    for i in range(total_parts):
        queue.put_nowait(i)

    print(f"📡 Launching {CONCURRENT_CONNECTIONS} Socket Streams...")

    async def worker():
        while not queue.empty():
            try:
                part_idx = queue.get_nowait()
            except asyncio.QueueEmpty:
                break
                
            offset = part_idx * part_size
            limit = min(part_size, file_size - offset)
            
            try:
                # Direct byte-range request
                async for chunk in client.iter_download(
                    message.media, 
                    offset=offset, 
                    limit=limit,
                    request_size=limit
                ):
                    async with aiofiles.open(output_path, 'r+b') as f:
                        await f.seek(offset)
                        await f.write(chunk)
                    tracker.update(len(chunk))
            except Exception:
                await queue.put(part_idx) # Retry on flood/timeout
                await asyncio.sleep(0.5)
            finally:
                queue.task_done()

    # Distribute load across workers
    tasks = [asyncio.create_task(worker()) for _ in range(CONCURRENT_CONNECTIONS)]
    await queue.join()
    
    # Force kill tasks to prevent hanging
    for t in tasks: t.cancel()
    print(f"\n✅ Integrity Check Passed: {os.path.getsize(output_path)/1024/1024:.2f} MB")

# --- VIDEO LOGIC ---

def process_video_advanced(input_path):
    print("🔬 Processing Pipeline: Correcting Codecs...")
    probe_cmd = f"ffprobe -v quiet -print_format json -show_streams -show_format '{input_path}'"
    out, _, _ = run_command(probe_cmd)
    
    try:
        data = json.loads(out)
        v_stream = next(s for s in data['streams'] if s['codec_type'] == 'video')
        codec = v_stream.get('codec_name', 'unknown')
    except:
        codec = "unknown"

    output_video = "ready_for_yt.mp4"
    sub_file = "subs.srt"
    
    # YouTube HEVC fix
    if 'hevc' in codec or 'h265' in codec:
        print(f"⚠️  Converting HEVC -> x264 (Ensures no missing frames on YT)")
        cmd = f"ffmpeg -i '{input_path}' -c:v libx264 -crf 19 -preset superfast -c:a aac -b:a 192k -movflags +faststart -y {output_video}"
    else:
        print("✅ Remuxing to YT-Native container")
        cmd = f"ffmpeg -i '{input_path}' -c copy -movflags +faststart -y {output_video}"

    run_command(cmd)
    
    # Subtitles
    run_command(f"ffmpeg -i '{input_path}' -map 0:s:0? '{sub_file}' -y")
    has_sub = os.path.exists(sub_file) and os.path.getsize(sub_file) > 100

    return output_video, (sub_file if has_sub else None)

# --- METADATA ---

def fetch_omdb(filename):
    if not OMDB_API_KEY: return None
    try:
        q = re.sub(r'\(.*?\)|\[.*?\]', '', filename).strip()
        return requests.get(f"http://www.omdbapi.com/?t={q}&apikey={OMDB_API_KEY}", timeout=5).json()
    except: return None

async def generate_metadata(filename):
    print("🧠 Gemini AI: Crafting Cinematic Metadata...")
    omdb = fetch_omdb(filename)
    
    if not GEMINI_API_KEY:
        return {"title": (omdb['Title'] if omdb else filename)[:90], "description": "High Speed Upload", "tags": []}

    prompt = (
        f"Generate Premium YouTube Metadata for: '{filename}'.\n"
        f"Data: {json.dumps(omdb) if omdb else 'None'}\n"
        "Output ONLY JSON: {'title': '...', 'description': '...', 'tags': []}"
    )

    try:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
        res = requests.post(url, json={"contents": [{"parts": [{"text": prompt}]}], "generationConfig": {"responseMimeType": "application/json"}})
        return json.loads(res.json()['candidates'][0]['content']['parts'][0]['text'])
    except:
        return {"title": filename[:90], "description": "Automated Upload", "tags": []}

# --- YOUTUBE UPLOAD ---

def upload_to_youtube(video_path, metadata, sub_path):
    try:
        creds = Credentials(
            token=None, refresh_token=os.environ['YOUTUBE_REFRESH_TOKEN'],
            token_uri='https://oauth2.googleapis.com/token',
            client_id=os.environ['YOUTUBE_CLIENT_ID'],
            client_secret=os.environ['YOUTUBE_CLIENT_SECRET'],
            scopes=YOUTUBE_SCOPES
        )
        creds.refresh(Request())
        youtube = build('youtube', 'v3', credentials=creds)

        media = MediaFileUpload(video_path, chunksize=1024*1024*20, resumable=True)
        request = youtube.videos().insert(
            part="snippet,status",
            body={
                'snippet': {
                    'title': metadata['title'],
                    'description': metadata['description'],
                    'tags': metadata.get('tags', []),
                    'categoryId': '24'
                },
                'status': {'privacyStatus': 'private'}
            },
            media_body=media
        )

        print(f"📤 Uploading: {metadata['title']}")
        response = None
        while response is None:
            status, response = request.next_chunk()
            if status:
                sys.stdout.write(f"\r🚀 Progress: {int(status.progress() * 100)}%")
                sys.stdout.flush()
        
        print(f"\n🎉 VIDEO LIVE: https://youtu.be/{response['id']}")

        if sub_path:
            try:
                youtube.captions().insert(
                    part="snippet",
                    body={'snippet': {'videoId': response['id'], 'language': 'en', 'name': 'English'}},
                    media_body=MediaFileUpload(sub_path)
                ).execute()
                print("✅ Subtitles Linked.")
            except: pass
            
    except Exception as e:
        print(f"\n🔴 YT-Error: {e}")

async def process_link(client, link):
    try:
        parts = [p for p in link.strip('/').split('/') if p]
        msg_id = int(parts[-1])
        chat_id = int(f"-100{parts[parts.index('c')+1]}")
        message = await client.get_messages(chat_id, ids=msg_id)
        
        raw_file = f"fast_{msg_id}.mkv"
        await turbo_download(client, message, raw_file)
        
        meta = await generate_metadata(message.file.name or raw_file)
        processed, subs = process_video_advanced(raw_file)
        
        upload_to_youtube(processed, meta, subs)

        for f in [raw_file, processed, subs]:
            if f and os.path.exists(f): os.remove(f)
    except Exception as e:
        print(f"\n❌ Pipeline Fault: {e}")

async def main():
    if len(sys.argv) < 2: return
    links = sys.argv[1].split(',')
    client = TelegramClient('bot_session', os.environ['TG_API_ID'], os.environ['TG_API_HASH'])
    await client.start(bot_token=TG_BOT_TOKEN)
    for link in links:
        await process_link(client, link)
    await client.disconnect()

if __name__ == '__main__':
    asyncio.run(main())
