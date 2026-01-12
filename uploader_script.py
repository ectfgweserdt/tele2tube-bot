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
from telethon.tl.functions.upload import GetFileRequest
from telethon.tl.types import InputFileLocation
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials
import googleapiclient.errors

# --- AGGRESSIVE PERFORMANCE OPTIMIZATIONS ---
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    print("🚀 uvloop enabled for maximum speed.")
except ImportError:
    print("⚠️ uvloop not found. Using default asyncio loop.")

YOUTUBE_SCOPES = ['https://www.googleapis.com/auth/youtube.force-ssl']
GEMINI_MODEL = "gemini-2.5-flash-preview-09-2025"

# "Dangerous" Configuration
# 512KB is the absolute maximum chunk size Telegram allows.
CHUNK_SIZE = 512 * 1024 
# Spawning 40 concurrent workers to saturate bandwidth. 
CONCURRENT_WORKERS = 40 

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
        self.downloaded_size = 0

    def update(self, current_chunk_size):
        self.downloaded_size += current_chunk_size
        now = time.time()
        
        # Throttling UI updates to prevent I/O blocking
        if now - self.last_ui_update < 0.5 and self.downloaded_size < self.total_size:
            return
        
        self.last_ui_update = now
        elapsed = now - self.start_time
        speed = (self.downloaded_size / 1024 / 1024) / max(elapsed, 0.1)
        percentage = (self.downloaded_size / self.total_size) * 100
        bar_length = 25
        filled = int(bar_length * self.downloaded_size // self.total_size)
        bar = '█' * filled + '░' * (bar_length - filled)
        
        status = (
            f"\r{self.prefix} [{bar}] {percentage:5.1f}% | "
            f"{self.downloaded_size/1024/1024:7.2f}/{self.total_size/1024/1024:7.2f} MB | "
            f"⚡ {speed:5.2f} MB/s"
        )
        sys.stdout.write(status)
        sys.stdout.flush()

async def download_chunk(client, location, offset, file_handle, semaphore, tracker, retries=5):
    """Downloads a specific chunk using raw GetFileRequest."""
    async with semaphore:
        for attempt in range(retries):
            try:
                # Use raw GetFileRequest to bypass high-level logic and force specific chunks
                # This corresponds to the "dangerous" manual method
                request = GetFileRequest(
                    location=location,
                    offset=offset,
                    limit=CHUNK_SIZE
                )
                result = await client(request)
                chunk = result.bytes
                
                if not chunk:
                    break # End of file
                
                # Seek and write. In single-threaded asyncio (even with uvloop), 
                # this is safe as long as we don't await *during* the seek/write pair.
                file_handle.seek(offset)
                file_handle.write(chunk)
                tracker.update(len(chunk))
                return
            except Exception as e:
                if attempt == retries - 1:
                    print(f"\n❌ Chunk failed at {offset}: {e}")
                    raise e
                await asyncio.sleep(0.5 * attempt)

async def fast_download(client, message, file_path):
    """
    Aggressive Manual Parallel Downloader.
    """
    print(f"🔥 INITIALIZING AGGRESSIVE DOWNLOADER: {CONCURRENT_WORKERS} threads")
    
    file_size = message.file.size
    # Convert message media to InputFileLocation for GetFileRequest
    input_location = utils.get_input_location(message.media)
    
    # Pre-allocate file on disk
    with open(file_path, "wb") as f:
        f.seek(file_size - 1)
        f.write(b"\0")
    
    f = open(file_path, "r+b")
    tracker = ProgressTracker(file_size, prefix='📥 FLOODING')
    semaphore = asyncio.Semaphore(CONCURRENT_WORKERS)
    
    tasks = []
    # Create tasks for every 512KB chunk
    for offset in range(0, file_size, CHUNK_SIZE):
        task = asyncio.create_task(
            download_chunk(client, input_location, offset, f, semaphore, tracker)
        )
        tasks.append(task)
    
    start_time = time.time()
    
    try:
        # Run all chunks concurrently. If one fails, we want to know.
        await asyncio.gather(*tasks)
    except Exception as e:
        print(f"\n⚠️ Critical Download Error: {e}")
        f.close()
        os.remove(file_path) # Delete corrupt file
        raise e # Re-raise to stop processing
    finally:
        f.close()
        
    duration = time.time() - start_time
    avg_speed = (file_size / 1024 / 1024) / max(duration, 0.1)
    print(f"\n✅ Download Complete in {duration:.2f}s! (Avg: {avg_speed:.2f} MB/s)")

def parse_filename(filename):
    clean_name = os.path.splitext(filename)[0].replace('_', ' ').replace('.', ' ')
    match = re.search(r'S(\d+)E(\d+)', clean_name, re.IGNORECASE)
    season, episode = None, None
    if match:
        season, episode = match.group(1), match.group(2)
        search_title = clean_name[:match.start()].strip()
    else:
        tags = [r'\d{3,4}p', 'HD', 'NF', 'WEB-DL', 'Dual Audio', 'x264', 'x265', 'HEVC']
        search_title = clean_name
        for tag in tags: search_title = re.sub(tag, '', search_title, flags=re.IGNORECASE)
        search_title = ' '.join(search_title.split()).strip()
    return search_title, season, episode

async def get_metadata(filename):
    print(f"🤖 AI is crafting cinematic metadata...")
    search_title, season, episode = parse_filename(filename)
    omdb_data = None
    if OMDB_API_KEY:
        try:
            url = f"http://www.omdbapi.com/?t={search_title}&apikey={OMDB_API_KEY}"
            if season: url += f"&Season={season}&Episode={episode}"
            res = requests.get(url, timeout=10)
            data = res.json()
            if data.get("Response") == "True": omdb_data = data
        except: pass

    if GEMINI_API_KEY:
        gemini_url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
        prompt = (
            f"Context: Filename '{filename}'. Data: {json.dumps(omdb_data) if omdb_data else 'N/A'}.\n"
            "Task: Create cinematic YouTube metadata. Use this EXACT structure:\n\n"
            "TITLE: [Movie Name (Year)] OR [Show Name - S00E00 - Episode Title]\n"
            "DESCRIPTION:\n"
            "🃏 Synopsis:\n[Engaging 3-4 sentence summary]\n\n"
            "👥 Cast:\n[Actor 1, Actor 2, Actor 3...]\n\n"
            "🔍 Details:\nGenre: ... | Network/Studio: ... | Origin: ...\n\n"
            "Return JSON with 'title', 'description', 'tags' (string or list)."
        )
        try:
            payload = {"contents": [{"parts": [{"text": prompt}]}], "generationConfig": {"responseMimeType": "application/json"}}
            res = requests.post(gemini_url, json=payload, timeout=30)
            if res.status_code == 200:
                return json.loads(res.json()['candidates'][0]['content']['parts'][0]['text'])
        except: pass
    return {"title": search_title, "description": "High-speed upload.", "tags": "movie,tv"}

def process_video_advanced(input_path):
    print(f"🛠️  Step 1: Analyzing media streams...")
    # Check if file is valid before ffprobe
    if not os.path.exists(input_path) or os.path.getsize(input_path) < 1024:
        raise Exception("Downloaded file is empty or too small.")

    probe_cmd = f"ffprobe -v quiet -print_format json -show_streams '{input_path}'"
    probe_out, _, _ = run_command(probe_cmd)
    
    try:
        probe_data = json.loads(probe_out)
    except json.JSONDecodeError:
        print("⚠️ FFprobe failed, uploading original file.")
        return input_path, None

    streams = probe_data.get('streams', [])
    if not streams:
        print("⚠️ No streams found, uploading original file.")
        return input_path, None

    audio_streams = [s for s in streams if s['codec_type'] == 'audio']
    eng_track = next((i for i, s in enumerate(audio_streams) if s.get('tags', {}).get('language') in ['eng', 'en']), None)
    
    audio_map = f"0:a:{eng_track}" if eng_track is not None else "0:a:0"
    output_video = "processed_video.mp4"
    
    print(f"✂️  Step 2: Stripping extra audio & keeping English (Fast Stream Copy)...")
    run_command(f"ffmpeg -i '{input_path}' -map 0:v:0 -map {audio_map} -c:v copy -c:a copy -dn -y '{output_video}'")
    
    print(f"📜 Step 3: Extracting and validating subtitles...")
    sub_file = "subs.srt"
    run_command(f"ffmpeg -i '{input_path}' -map 0:s:0 -c:s srt '{sub_file}' -y")
    
    has_subs = os.path.exists(sub_file) and os.path.getsize(sub_file) > 100
    
    if os.path.exists(output_video) and os.path.getsize(output_video) > 1024:
        return output_video, (sub_file if has_subs else None)
    
    return input_path, (sub_file if has_subs else None)

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
        
        raw_tags = metadata.get('tags', 'video')
        if isinstance(raw_tags, str):
            tags_list = [t.strip() for t in raw_tags.split(',')]
        elif isinstance(raw_tags, list):
            tags_list = raw_tags
        else:
            tags_list = ["video"]

        print(f"🚀 Initializing YouTube Upload...")
        body = {
            'snippet': {
                'title': metadata.get('title', 'Video')[:95],
                'description': metadata.get('description', ''),
                'tags': tags_list,
                'categoryId': '24'
            },
            'status': {'privacyStatus': 'private'}
        }
        
        media = MediaFileUpload(video_path, chunksize=1024*1024*10, resumable=True)
        request = youtube.videos().insert(part="snippet,status", body=body, media_body=media)
        
        total_v_size = os.path.getsize(video_path)
        tracker = ProgressTracker(total_v_size, prefix='📤 Uploading  ')
        
        response = None
        while response is None:
            status, response = request.next_chunk()
            if status:
                tracker.update(status.resumable_progress - tracker.downloaded_size)

        video_id = response['id']
        print(f"\n✨ Video Uploaded! ID: {video_id}")

        if sub_path:
            print("📜 Attaching Subtitles...")
            try:
                youtube.captions().insert(
                    part="snippet",
                    body={'snippet': {'videoId': video_id, 'language': 'en', 'name': 'English', 'isDraft': False}},
                    media_body=MediaFileUpload(sub_path),
                    sync=True 
                ).execute()
                print("✅ Subtitles Published.")
            except: print("⚠️ Subtitle attachment failed.")
            
        print(f"🎉 SUCCESS: https://youtu.be/{video_id}")
    except Exception as e:
        print(f"\n🔴 YouTube Error: {e}")

async def process_link(client, link):
    try:
        parts = [p for p in link.strip('/').split('/') if p]
        msg_id, chat_id = int(parts[-1]), int(f"-100{parts[parts.index('c')+1]}")
        message = await client.get_messages(chat_id, ids=msg_id)
        
        raw_file = f"temp_{msg_id}.mkv"
        
        # Will raise exception if download fails
        await fast_download(client, message, raw_file)
        
        metadata = await get_metadata(message.file.name or raw_file)
        final_video, sub_file = process_video_advanced(raw_file)
        
        upload_to_youtube(final_video, metadata, sub_file)

        for f in [raw_file, final_video, sub_file]:
            if f and os.path.exists(f): os.remove(f)
    except Exception as e:
        print(f"\n❌ Error processing link: {e}")
        # Ensure cleanup on failure
        if 'raw_file' in locals() and os.path.exists(raw_file):
            os.remove(raw_file)

async def main():
    if len(sys.argv) < 2: return
    links = sys.argv[1].split(',')
    # Optimized client params
    client = TelegramClient(
        'bot_session', 
        os.environ['TG_API_ID'], 
        os.environ['TG_API_HASH'],
        request_retries=15,
        connection_retries=15,
        retry_delay=1, 
        auto_reconnect=True,
        flood_sleep_threshold=5 
    )
    await client.start(bot_token=TG_BOT_TOKEN)
    for link in links:
        await process_link(client, link)
    await client.disconnect()

if __name__ == '__main__':
    asyncio.run(main())
