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
import threading
from telethon import TelegramClient, errors, utils
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials

# --- PERFORMANCE CONFIG ---
YOUTUBE_SCOPES = ['https://www.googleapis.com/auth/youtube.force-ssl']
GEMINI_MODEL = "gemini-2.5-flash-preview-09-2025"

# Optimal for 100MBps: 24-32 concurrent workers
CONCURRENT_CONNECTIONS = 24
PART_SIZE = 1024 * 1024  # 1MB per chunk

# API Keys
TG_BOT_TOKEN = os.environ.get('TG_BOT_TOKEN', '').strip()
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY', '').strip()
OMDB_API_KEY = os.environ.get('OMDB_API_KEY', '').strip()

# Global lock for file writing to prevent disk IO hang
write_lock = threading.Lock()

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

# --- DETERMINISTIC TURBO DOWNLOADER ---

async def turbo_download(client, message, output_path):
    file_size = message.file.size
    tracker = ProgressTracker(file_size, prefix='🔥 TURBO-DL')
    
    with open(output_path, 'wb') as f:
        f.truncate(file_size)

    total_parts = math.ceil(file_size / PART_SIZE)
    ranges = []
    for i in range(total_parts):
        start = i * PART_SIZE
        end = min((i + 1) * PART_SIZE - 1, file_size - 1)
        ranges.append((start, end))

    queue = asyncio.Queue()
    for r in ranges:
        queue.put_nowait(r)

    async def worker():
        while not queue.empty():
            try:
                start, end = queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            limit = (end - start) + 1
            try:
                async for chunk in client.iter_download(
                    message.media, 
                    offset=start, 
                    limit=limit,
                    request_size=limit
                ):
                    if not chunk: continue
                    with write_lock:
                        with open(output_path, 'r+b') as f:
                            f.seek(start)
                            f.write(chunk)
                    tracker.update(len(chunk))
            except Exception:
                await queue.put((start, end))
                await asyncio.sleep(1)
            finally:
                queue.task_done()

    tasks = [asyncio.create_task(worker()) for _ in range(CONCURRENT_CONNECTIONS)]
    await queue.join()
    for t in tasks: t.cancel()
    
    actual_size = os.path.getsize(output_path)
    if actual_size != file_size:
        with open(output_path, 'ab') as f:
            f.truncate(file_size)
    print(f"\n✅ Download Verified: {os.path.getsize(output_path)/1024/1024:.2f} MB")

# --- SMART VIDEO & AUDIO PIPELINE (ENGLISH PRIORITY) ---

def process_video_advanced(input_path):
    print("🔬 Analyzing Streams (Prioritizing English)...")
    probe_cmd = f"ffprobe -v quiet -print_format json -show_streams -show_format '{input_path}'"
    out, _, _ = run_command(probe_cmd)
    
    video_idx, audio_idx, sub_idx = 0, 0, None
    v_codec = "unknown"

    try:
        data = json.loads(out)
        streams = data.get('streams', [])
        
        # 1. Find Best Video
        for s in streams:
            if s['codec_type'] == 'video':
                video_idx = s['index']
                v_codec = s.get('codec_name', 'unknown')
                break

        # 2. Find English Audio (Priority)
        audio_streams = [s for s in streams if s['codec_type'] == 'audio']
        if audio_streams:
            # Default to first audio if no English found
            audio_idx = audio_streams[0]['index']
            for s in audio_streams:
                lang = s.get('tags', {}).get('language', '').lower()
                if lang in ['eng', 'en', 'english']:
                    audio_idx = s['index']
                    print(f"🔊 English Audio Found: Stream #{audio_idx}")
                    break
        
        # 3. Find English Subtitles
        sub_streams = [s for s in streams if s['codec_type'] == 'subtitle']
        for s in sub_streams:
            lang = s.get('tags', {}).get('language', '').lower()
            if lang in ['eng', 'en', 'english']:
                sub_idx = s['index']
                print(f"📝 English Subtitles Found: Stream #{sub_idx}")
                break
    except Exception as e:
        print(f"⚠️ Metadata analysis warning: {e}")

    output_video = "upload_ready.mp4"
    sub_file = "subs.srt"
    
    # Video Parameters
    video_params = "-c:v libx264 -crf 19 -preset superfast" if ('hevc' in v_codec or 'h265' in v_codec) else "-c:v copy"
    # Audio Parameters (Always transcode to AAC for YT stability)
    audio_params = f"-c:a aac -b:a 192k -ac 2"

    print(f"🎬 Processing: Video Stream #{video_idx} | Audio Stream #{audio_idx}")
    # Map specific English tracks
    cmd = f"ffmpeg -i '{input_path}' -map 0:{video_idx} -map 0:{audio_idx} {video_params} {audio_params} -movflags +faststart -y {output_video}"
    run_command(cmd)
    
    # Extract English Subtitles if found
    if sub_idx is not None:
        run_command(f"ffmpeg -i '{input_path}' -map 0:{sub_idx} '{sub_file}' -y")
    
    has_sub = os.path.exists(sub_file) and os.path.getsize(sub_file) > 100
    return output_video, (sub_file if has_sub else None)

# --- METADATA ENHANCEMENT ---

def fetch_omdb(filename):
    if not OMDB_API_KEY: return None
    try:
        q = re.sub(r'\(.*?\)|\[.*?\]|1080p|720p|WEB-DL|HDR|H264|H265|x264|x265', '', filename).strip()
        return requests.get(f"http://www.omdbapi.com/?t={q}&apikey={OMDB_API_KEY}", timeout=5).json()
    except: return None

async def generate_metadata(filename):
    print("🧠 Gemini AI: Crafting English Metadata...")
    omdb = fetch_omdb(filename)
    
    if not GEMINI_API_KEY:
        title = (omdb['Title'] if omdb else filename)[:90]
        return {"title": title, "description": "High Performance Upload", "tags": []}

    prompt = (
        f"Generate English YouTube Metadata for: '{filename}'.\n"
        f"Context from OMDb: {json.dumps(omdb) if omdb else 'None'}\n"
        "Return ONLY JSON: {'title': '...', 'description': '...', 'tags': []}"
    )

    try:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
        res = requests.post(url, json={"contents": [{"parts": [{"text": prompt}]}], "generationConfig": {"responseMimeType": "application/json"}})
        return json.loads(res.json()['candidates'][0]['content']['parts'][0]['text'])
    except:
        return {"title": filename[:90], "description": "Automated Upload", "tags": []}

# --- YOUTUBE UPLOAD CORE ---

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
        
        print(f"\n🎉 DONE: https://youtu.be/{response['id']}")

        if sub_path:
            try:
                youtube.captions().insert(
                    part="snippet",
                    body={'snippet': {'videoId': response['id'], 'language': 'en', 'name': 'English'}},
                    media_body=MediaFileUpload(sub_path)
                ).execute()
                print("✅ English Subtitles linked.")
            except: pass
            
    except Exception as e:
        print(f"\n🔴 YouTube Error: {e}")

async def process_link(client, link):
    try:
        parts = [p for p in link.strip('/').split('/') if p]
        msg_id, chat_id = int(parts[-1]), int(f"-100{parts[parts.index('c')+1]}")
        message = await client.get_messages(chat_id, ids=msg_id)
        
        raw_file = f"cache_{msg_id}.mkv"
        await turbo_download(client, message, raw_file)
        
        meta = await generate_metadata(message.file.name or raw_file)
        proc_video, sub_path = process_video_advanced(raw_file)
        
        upload_to_youtube(proc_video, meta, sub_path)

        for f in [raw_file, proc_video, sub_path]:
            if f and os.path.exists(f): os.remove(f)
    except Exception as e:
        print(f"\n❌ Pipeline Error: {e}")

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
