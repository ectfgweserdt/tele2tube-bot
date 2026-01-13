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
from telethon.tl.types import InputDocumentFileLocation
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials
import googleapiclient.errors

# --- OPTIMIZED CONFIGURATION ---
YOUTUBE_SCOPES = ['https://www.googleapis.com/auth/youtube.force-ssl']
GEMINI_MODEL = "gemini-2.5-flash-preview-09-2025"

# AGGRESSIVE CONFIGURATION FOR SPEED
# 16-32 workers is safe on high-bandwidth servers (like GitHub Actions)
PARALLEL_WORKERS = 20 
CHUNK_SIZE_KB = 1024  # 1MB chunks (better for high throughput than 512KB)

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
        # Allow incremental updates (for parallel downloader) or absolute (for single thread)
        if abs_current is not None:
            self.downloaded_bytes = abs_current
        else:
            self.downloaded_bytes += current_inc

        now = time.time()
        # Throttling UI to 1 update per 0.5 second
        if now - self.last_ui_update < 0.5 and self.downloaded_bytes < self.total_size:
            return
        
        self.last_ui_update = now
        elapsed = now - self.start_time
        # Prevent division by zero
        safe_elapsed = max(elapsed, 0.01)
        
        speed = (self.downloaded_bytes / 1024 / 1024) / safe_elapsed
        percentage = (self.downloaded_bytes / self.total_size) * 100
        
        # Cap percentage at 100 for UI cleanliness
        percentage = min(percentage, 100.0)
        
        bar_length = 20
        filled = int(bar_length * percentage // 100)
        bar = '█' * filled + '-' * (bar_length - filled)
        
        status = (
            f"\r{self.prefix} [{bar}] {percentage:5.1f}% | "
            f"{self.downloaded_bytes/1024/1024:7.2f}/{self.total_size/1024/1024:7.2f} MB | "
            f"⚡ {speed:5.2f} MB/s"
        )
        sys.stdout.write(status)
        sys.stdout.flush()

async def fast_download(client, message, file_path):
    """
    High-Speed Parallel Downloader.
    Downloads chunks of the file concurrently to maximize bandwidth.
    """
    print(f"📡 Initializing Hyper-Speed Download...")
    
    # Get media details
    media = message.media
    if not media:
        print("❌ No media found.")
        return

    # Determine file size and input location
    if hasattr(media, 'document'):
        file_size = media.document.size
        input_location = InputDocumentFileLocation(
            id=media.document.id,
            access_hash=media.document.access_hash,
            file_reference=media.document.file_reference,
            thumb_size=""
        )
    else:
        # Fallback for other media types (photos etc), though less likely for videos
        print("⚠️ Not a document, falling back to standard download.")
        await client.download_media(message, file_path)
        return

    tracker = ProgressTracker(file_size, prefix='📥 Downloading')
    
    # Calculate chunks
    chunk_size = CHUNK_SIZE_KB * 1024
    total_chunks = math.ceil(file_size / chunk_size)
    queue = asyncio.Queue()
    
    # Populate queue with chunk indices
    for i in range(total_chunks):
        queue.put_nowait(i)

    # Open file for random access writing
    with open(file_path, 'wb') as f:
        # Pre-allocate file size on disk to prevent fragmentation and pointer jumping
        f.truncate(file_size)
        
        # Lock for thread-safe file writing
        file_lock = asyncio.Lock()
        
        async def worker():
            while not queue.empty():
                chunk_index = await queue.get()
                offset = chunk_index * chunk_size
                
                # Calculate size for this specific chunk (last one might be smaller)
                current_limit = min(chunk_size, file_size - offset)
                
                try:
                    # Download chunk
                    # part_size_kb ensures internal Telethon fetch size matches our logic
                    async for chunk in client.iter_download(
                        media, 
                        offset=offset, 
                        limit=current_limit, 
                        chunk_size=None, # Let Telethon decide internal buffer
                        request_size=CHUNK_SIZE_KB * 1024
                    ):
                        async with file_lock:
                            f.seek(offset)
                            f.write(chunk)
                        
                        # Update tracker
                        tracker.update(current_inc=len(chunk))
                        
                except Exception as e:
                    print(f"\n❌ Chunk {chunk_index} failed: {e}")
                    # In a production app, you might want to re-queue the chunk here
                    # queue.put_nowait(chunk_index)
                finally:
                    queue.task_done()

        # Launch workers
        start_time = time.time()
        workers = [asyncio.create_task(worker()) for _ in range(min(PARALLEL_WORKERS, total_chunks))]
        await asyncio.gather(*workers)
        
    duration = time.time() - start_time
    avg_speed = (file_size / 1024 / 1024) / max(duration, 0.1)
    print(f"\n✅ Download Complete in {duration:.2f}s! (Avg: {avg_speed:.2f} MB/s)")

def get_file_info(file_path):
    """Analyzes file and prints detailed stats before upload."""
    print("\n📋 --- FILE ANALYSIS REPORT ---")
    if not os.path.exists(file_path):
        print("❌ File not found.")
        return

    size_bytes = os.path.getsize(file_path)
    size_mb = size_bytes / (1024 * 1024)
    size_gb = size_mb / 1024
    
    print(f"📦 Size: {size_mb:.2f} MB ({size_gb:.2f} GB)")
    
    # Use ffprobe for media details
    try:
        cmd = f"ffprobe -v error -select_streams v:0 -show_entries stream=width,height,duration,codec_name -of default=noprint_wrappers=1:nokey=1 '{file_path}'"
        output, _, _ = run_command(cmd)
        details = output.strip().split('\n')
        
        if len(details) >= 3:
            # Output order varies, so we use a json format for reliability usually, but simple text is fine if parsed loosely
            # Let's try JSON for safety
            cmd_json = f"ffprobe -v error -select_streams v:0 -show_entries stream=width,height,duration,codec_name,bit_rate -of json '{file_path}'"
            out_json, _, _ = run_command(cmd_json)
            data = json.loads(out_json)
            stream = data['streams'][0]
            
            width = stream.get('width', 'N/A')
            height = stream.get('height', 'N/A')
            duration = float(stream.get('duration', 0))
            codec = stream.get('codec_name', 'unknown')
            bitrate = int(stream.get('bit_rate', 0)) / 1024 if stream.get('bit_rate') else 0
            
            mins, secs = divmod(duration, 60)
            hours, mins = divmod(mins, 60)
            
            print(f"🎬 Resolution: {width}x{height}")
            print(f"⏱️  Duration: {int(hours)}h {int(mins)}m {int(secs)}s")
            print(f"🎞️  Codec: {codec.upper()}")
            print(f"📶 Bitrate: {bitrate:.0f} kbps")
    except Exception as e:
        print(f"⚠️ Could not read media metadata: {e}")
    print("------------------------------\n")

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
    probe_cmd = f"ffprobe -v quiet -print_format json -show_streams '{input_path}'"
    probe_out, _, _ = run_command(probe_cmd)
    try:
        probe_data = json.loads(probe_out)
    except:
        probe_data = {"streams": []}
    
    streams = probe_data.get('streams', [])
    audio_streams = [s for s in streams if s['codec_type'] == 'audio']
    eng_track = next((i for i, s in enumerate(audio_streams) if s.get('tags', {}).get('language') in ['eng', 'en']), None)
    
    audio_map = f"0:a:{eng_track}" if eng_track is not None else "0:a:0"
    output_video = "processed_video.mp4"
    
    # Check if processing is even needed (if it's already mp4 and has 1 audio track, maybe skip?)
    # But user asked to keep functions, so we proceed.
    
    print(f"✂️  Step 2: Stripping extra audio & keeping English (Fast Stream Copy)...")
    # -strict -2 ensures compatibility with some older aac standards if needed
    run_command(f"ffmpeg -i '{input_path}' -map 0:v:0 -map {audio_map} -c:v copy -c:a copy -y '{output_video}'")
    
    # Fallback if ffmpeg failed (empty output)
    if not os.path.exists(output_video) or os.path.getsize(output_video) < 1000:
        print("⚠️ Processing failed or file too small, using original file.")
        if os.path.exists(output_video): os.remove(output_video)
        import shutil
        shutil.copy(input_path, output_video)

    print(f"📜 Step 3: Extracting and validating subtitles...")
    sub_file = "subs.srt"
    run_command(f"ffmpeg -i '{input_path}' -map 0:s:0 -c:s srt '{sub_file}' -y")
    
    has_subs = os.path.exists(sub_file) and os.path.getsize(sub_file) > 100
    return output_video, (sub_file if has_subs else None)

def upload_to_youtube(video_path, metadata, sub_path):
    # Show file details before uploading
    get_file_info(video_path)

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
                tracker.update(abs_current=status.resumable_progress)

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
        
        # Ensure we have a valid filename
        filename = message.file.name
        if not filename:
            # Try to guess extension from mime type or attributes
            ext = ".mkv" # Default
            if message.file.mime_type:
                 if 'mp4' in message.file.mime_type: ext = ".mp4"
                 elif 'matroska' in message.file.mime_type: ext = ".mkv"
            filename = f"downloaded_media_{msg_id}{ext}"

        raw_file = f"temp_{msg_id}_{filename}"
        
        # Use the new parallel downloader
        await fast_download(client, message, raw_file)
        
        # Display stats of the raw downloaded file
        print("\n--- Raw Download Info ---")
        get_file_info(raw_file)

        metadata = await get_metadata(filename)
        final_video, sub_file = process_video_advanced(raw_file)
        
        upload_to_youtube(final_video, metadata, sub_file)

        for f in [raw_file, final_video, sub_file]:
            if f and os.path.exists(f): os.remove(f)
    except Exception as e:
        print(f"\n❌ Error processing link: {e}")
        import traceback
        traceback.print_exc()

async def main():
    if len(sys.argv) < 2: return
    links = sys.argv[1].split(',')
    # Optimized client params
    client = TelegramClient(
        'bot_session', 
        os.environ['TG_API_ID'], 
        os.environ['TG_API_HASH'],
        request_retries=20,
        connection_retries=20,
        retry_delay=1,
        auto_reconnect=True
    )
    await client.start(bot_token=TG_BOT_TOKEN)
    for link in links:
        await process_link(client, link)
    await client.disconnect()

if __name__ == '__main__':
    asyncio.run(main())
