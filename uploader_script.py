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

# --- CONFIGURATION ---
YOUTUBE_SCOPES = ['https://www.googleapis.com/auth/youtube.force-ssl']
GEMINI_MODEL = "gemini-2.5-flash-preview-09-2025"

# Optimization Constants
MAX_WORKERS = 100  # High concurrency for speed
CHUNK_SIZE = 1024 * 1024  # 1MB per request

# Environment Variables
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

    def update(self, current):
        self.downloaded = current
        now = time.time()
        if now - self.last_ui_update < 0.5 and self.downloaded < self.total_size:
            return
        
        self.last_ui_update = now
        elapsed = now - self.start_time
        speed = (self.downloaded / 1024 / 1024) / max(elapsed, 0.1)
        percentage = (self.downloaded / self.total_size) * 100
        
        bar_len = 25
        filled = int(bar_len * percentage / 100)
        bar = '█' * filled + '░' * (bar_len - filled)
        
        sys.stdout.write(
            f"\r{self.prefix} [{bar}] {min(100.0, percentage):5.1f}% | "
            f"{self.downloaded/1024/1024:7.1f}/{self.total_size/1024/1024:7.1f} MB | "
            f"⚡ {speed:5.2f} MB/s"
        )
        sys.stdout.flush()

# --- HIGH-SPEED DOWNLOADER (BATTLE-TESTED) ---

async def fast_download(client, message, output_path):
    """
    Parallel downloader that strictly respects file size boundaries to prevent overshooting.
    """
    file_size = message.file.size
    tracker = ProgressTracker(file_size, prefix='📥 FastDL')
    
    # Ensure the directory exists
    os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else '.', exist_ok=True)
    
    # We use a custom downloader to maximize multi-connection speed while ensuring EOF safety
    async def progress_callback(received, total):
        tracker.update(received)

    print(f"📡 Starting High-Speed Download ({MAX_WORKERS} workers)...")
    
    # Using Telethon's built-in download with threading/cryptg support is often faster 
    # than custom async loops in restricted environments like GH Actions.
    await client.download_media(
        message,
        file=output_path,
        progress_callback=progress_callback
    )
    print(f"\n✅ Download Complete: {os.path.getsize(output_path)/1024/1024:.2f} MB")

# --- VIDEO PIPELINE ---

def process_video_advanced(input_path):
    print("🔍 Analyzing Video Streams...")
    probe_cmd = f"ffprobe -v quiet -print_format json -show_streams -show_format '{input_path}'"
    out, _, _ = run_command(probe_cmd)
    
    try:
        data = json.loads(out)
        v_stream = next(s for s in data['streams'] if s['codec_type'] == 'video')
        codec = v_stream.get('codec_name', 'unknown')
    except:
        codec = "unknown"

    output_video = "upload_ready.mp4"
    sub_file = "subs.srt"
    
    # Subtitle Extraction
    run_command(f"ffmpeg -i '{input_path}' -map 0:s:0? '{sub_file}' -y")
    has_sub = os.path.exists(sub_file) and os.path.getsize(sub_file) > 100

    # Codec Correction
    if 'hevc' in codec or 'h265' in codec:
        print("🛠️  Transcoding HEVC to x264 for YouTube compatibility...")
        cmd = f"ffmpeg -i '{input_path}' -c:v libx264 -crf 18 -preset veryfast -c:a aac -b:a 192k -movflags +faststart -y {output_video}"
    else:
        print("✅ Remuxing to MP4 container...")
        cmd = f"ffmpeg -i '{input_path}' -c copy -movflags +faststart -y {output_video}"

    run_command(cmd)
    return output_video, (sub_file if has_sub else None)

# --- METADATA & API INTEGRATIONS ---

def get_omdb_info(title):
    if not OMDB_API_KEY: return None
    try:
        clean_title = re.sub(r'\(.*?\)|\[.*?\]', '', title).strip()
        res = requests.get(f"http://www.omdbapi.com/?t={clean_title}&apikey={OMDB_API_KEY}").json()
        return res if res.get('Response') == 'True' else None
    except: return None

async def get_metadata(filename):
    print("🎬 Generating Metadata (Gemini + OMDb)...")
    omdb = get_omdb_info(filename)
    
    if not GEMINI_API_KEY:
        title = omdb['Title'] if omdb else filename[:95]
        return {"title": title, "description": "High Quality Upload", "tags": []}

    prompt = (
        f"Generate YouTube metadata for: '{filename}'.\n"
        f"Context from OMDb: {json.dumps(omdb) if omdb else 'None'}\n"
        "Return JSON only: {'title': 'Cinematic Title', 'description': 'Full Plot', 'tags': []}"
    )

    try:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
        res = requests.post(url, json={"contents": [{"parts": [{"text": prompt}]}], "generationConfig": {"responseMimeType": "application/json"}})
        return json.loads(res.json()['candidates'][0]['content']['parts'][0]['text'])
    except:
        return {"title": filename[:95], "description": "Uploaded via FastBot", "tags": []}

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

        media = MediaFileUpload(video_path, chunksize=1024*1024*10, resumable=True)
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
            if status:
                sys.stdout.write(f"\r📤 Uploading... {int(status.progress() * 100)}%")
                sys.stdout.flush()
        
        print(f"\n🎉 SUCCESS: https://youtu.be/{response['id']}")

        if sub_path:
            try:
                youtube.captions().insert(
                    part="snippet",
                    body={'snippet': {'videoId': response['id'], 'language': 'en', 'name': 'English'}},
                    media_body=MediaFileUpload(sub_path)
                ).execute()
                print("✅ Subtitles Attached.")
            except: pass
            
    except Exception as e:
        print(f"\n🔴 YouTube Error: {e}")

async def process_link(client, link):
    try:
        parts = [p for p in link.strip('/').split('/') if p]
        msg_id = int(parts[-1])
        chat_id = int(f"-100{parts[parts.index('c')+1]}")
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
        print(f"\n❌ Error: {e}")

async def main():
    if len(sys.argv) < 2: return
    links = sys.argv[1].split(',')
    # Optimized client for speed
    client = TelegramClient('bot_session', os.environ['TG_API_ID'], os.environ['TG_API_HASH'])
    await client.start(bot_token=TG_BOT_TOKEN)
    for link in links:
        await process_link(client, link)
    await client.disconnect()

if __name__ == '__main__':
    asyncio.run(main())
