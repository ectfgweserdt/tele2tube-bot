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

# --- OPTIMIZED CONFIGURATION ---
YOUTUBE_SCOPES = ['https://www.googleapis.com/auth/youtube.force-ssl']
GEMINI_MODEL = "gemini-2.5-flash-preview-09-2025"

# Tuning for 20Mbps+ (Maximized for GitHub Actions runners)
DOWNLOAD_WORKERS = 8       # Increased workers for parallel throughput
CHUNK_SIZE = 1024 * 1024   # 1MB chunk requests

# Fetching API Keys
TG_BOT_TOKEN = os.environ.get('TG_BOT_TOKEN', '').strip()
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY', '').strip()

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
        self.processed_bytes = 0

    def update(self, current_chunk_size):
        self.processed_bytes += current_chunk_size
        now = time.time()
        if now - self.last_ui_update < 0.5 and self.processed_bytes < self.total_size:
            return
        
        self.last_ui_update = now
        elapsed = now - self.start_time
        speed = (self.processed_bytes / 1024 / 1024) / max(elapsed, 0.1)
        percentage = (self.processed_bytes / self.total_size) * 100
        percentage = min(100.0, percentage)
        
        bar_len = 20
        filled = int(bar_len * percentage / 100)
        bar = '█' * filled + '░' * (bar_len - filled)
        
        sys.stdout.write(
            f"\r{self.prefix} [{bar}] {percentage:5.1f}% | "
            f"{self.processed_bytes/1024/1024:7.1f}/{self.total_size/1024/1024:7.1f} MB | "
            f"⚡ {speed:5.2f} MB/s"
        )
        sys.stdout.flush()

# --- HIGH-SPEED DOWNLOADER ---

async def fast_download(client, message, output_path):
    """
    Parallel chunk downloader with strict EOF boundaries to prevent infinite loops.
    """
    file_size = message.file.size
    tracker = ProgressTracker(file_size, prefix='📥 FastDL')
    
    # Pre-allocate the file on disk
    async with aiofiles.open(output_path, 'wb') as f:
        await f.seek(file_size - 1)
        await f.write(b'\0')

    # Calculate parts strictly
    part_size = 1024 * 1024 # 1MB parts
    total_parts = math.ceil(file_size / part_size)
    queue = asyncio.Queue()
    
    for i in range(total_parts):
        queue.put_nowait(i)

    async def worker():
        while not queue.empty():
            part_idx = await queue.get()
            offset = part_idx * part_size
            # Crucial: Calculate exactly how much to download for this part
            limit = min(part_size, file_size - offset)
            
            try:
                # iter_download with strict offset and limit prevents over-downloading
                async for chunk in client.iter_download(
                    message.media, 
                    offset=offset, 
                    request_size=limit, 
                    limit=limit
                ):
                    async with aiofiles.open(output_path, 'r+b') as f:
                        await f.seek(offset)
                        await f.write(chunk)
                    tracker.update(len(chunk))
            except Exception as e:
                await queue.put(part_idx) # Retry on failure
            finally:
                queue.task_done()

    workers = [asyncio.create_task(worker()) for _ in range(DOWNLOAD_WORKERS)]
    await queue.join()
    for w in workers: w.cancel()
    print(f"\n✅ Download Verified: {os.path.getsize(output_path)/1024/1024:.2f} MB")

# --- VIDEO PIPELINE ---

def process_video_advanced(input_path):
    """
    Pipeline to detect x265/HEVC and correct it for YouTube.
    """
    print("🔍 Analyzing file for YouTube compatibility...")
    probe_cmd = f"ffprobe -v quiet -print_format json -show_streams -show_format '{input_path}'"
    out, _, _ = run_command(probe_cmd)
    
    try:
        data = json.loads(out)
        v_stream = next(s for s in data['streams'] if s['codec_type'] == 'video')
        codec = v_stream.get('codec_name', 'unknown')
        width = v_stream.get('width', 'Unknown')
        height = v_stream.get('height', 'Unknown')
    except:
        print("⚠️ Warning: Could not analyze codec. Proceeding with remux.")
        codec = "unknown"
        width, height = "?", "?"

    # Pre-processing info display
    size = os.path.getsize(input_path) / (1024 * 1024)
    print(f"📊 Media Info: {codec.upper()} | {width}x{height} | {size:.2f} MB")

    output_video = "ready_to_upload.mp4"
    sub_file = "subs.srt"
    
    # Extract Subs first
    run_command(f"ffmpeg -i '{input_path}' -map 0:s:0? '{sub_file}' -y")
    has_sub = os.path.exists(sub_file) and os.path.getsize(sub_file) > 100

    # Decision: Transcode if HEVC to prevent missing frames
    if 'hevc' in codec or 'h265' in codec:
        print("🛠️  Detected HEVC: Transcoding to x264 (CRF 20) for YouTube stability...")
        cmd = f"ffmpeg -i '{input_path}' -c:v libx264 -crf 20 -preset fast -c:a aac -movflags +faststart -y {output_video}"
    else:
        print("✅ Codec safe: Remuxing to clean MP4 container...")
        cmd = f"ffmpeg -i '{input_path}' -c copy -movflags +faststart -y {output_video}"

    run_command(cmd)
    return output_video, (sub_file if has_sub else None)

# --- METADATA & UPLOADER ---

async def get_metadata(filename):
    print("🎬 Generating Cinematic Metadata...")
    clean_name = os.path.splitext(filename)[0].replace('.', ' ').replace('_', ' ')
    
    if not GEMINI_API_KEY:
        return {"title": clean_name[:95], "description": "High Quality Upload", "tags": []}

    prompt = (
        f"Create metadata for file: '{filename}'.\n"
        "Rules:\n"
        "1. Title: Clean 'Name (Year) | 4K' or 'Show S01E01'. NO 'Trailer' or 'Teaser'.\n"
        "2. Description: Catchy synopsis + Cast list. Streaming style (like Netflix).\n"
        "3. Output JSON: {'title': '...', 'description': '...', 'tags': []}"
    )

    try:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
        res = requests.post(url, json={"contents": [{"parts": [{"text": prompt}]}], "generationConfig": {"responseMimeType": "application/json"}})
        return json.loads(res.json()['candidates'][0]['content']['parts'][0]['text'])
    except:
        return {"title": clean_name[:95], "description": "High Quality Video", "tags": []}

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

        media = MediaFileUpload(video_path, chunksize=1024*1024*5, resumable=True)
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

        print(f"☁️ Uploading: {metadata['title']}")
        response = None
        while response is None:
            status, response = request.next_chunk()
        
        video_id = response['id']
        print(f"🎉 SUCCESS: https://youtu.be/{video_id}")

        if sub_path:
            try:
                youtube.captions().insert(
                    part="snippet",
                    body={'snippet': {'videoId': video_id, 'language': 'en', 'name': 'English'}},
                    media_body=MediaFileUpload(sub_path)
                ).execute()
                print("✅ Subtitles Linked.")
            except: pass
            
    except Exception as e:
        print(f"🔴 YouTube Error: {e}")

async def process_link(client, link):
    try:
        parts = [p for p in link.strip('/').split('/') if p]
        msg_id = int(parts[-1])
        chat_id = int(f"-100{parts[parts.index('c')+1]}")
        message = await client.get_messages(chat_id, ids=msg_id)
        
        raw_file = f"temp_{msg_id}.mkv"
        await fast_download(client, message, raw_file)
        
        metadata = await get_metadata(message.file.name or raw_file)
        final_video, sub_file = process_video_advanced(raw_file)
        
        upload_to_youtube(final_video, metadata, sub_file)

        for f in [raw_file, final_video, sub_file]:
            if f and os.path.exists(f): os.remove(f)
    except Exception as e:
        print(f"❌ Error: {e}")

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
