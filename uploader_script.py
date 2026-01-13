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

# --- CONFIGURATION & TUNING ---
YOUTUBE_SCOPES = ['https://www.googleapis.com/auth/youtube.force-ssl']
GEMINI_MODEL = "gemini-2.5-flash-preview-09-2025"

# Parallel Download Settings (Optimized for 20Mbps+)
DOWNLOAD_WORKERS = 4      # Simultaneous connections
CHUNK_SIZE = 1024 * 1024  # 1MB chunks

# API Keys
TG_BOT_TOKEN = os.environ.get('TG_BOT_TOKEN', '').strip()
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY', '').strip()
OMDB_API_KEY = os.environ.get('OMDB_API_KEY', '').strip()

# --- UTILS ---

def run_command(command):
    """Run shell commands and return output."""
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

    def update(self, current_chunk_size=0, exact_pos=None):
        if exact_pos is not None:
            self.processed_bytes = exact_pos
        else:
            self.processed_bytes += current_chunk_size
            
        now = time.time()
        # Update UI max 5 times a second to save CPU
        if now - self.last_ui_update < 0.2 and self.processed_bytes < self.total_size:
            return
        
        self.last_ui_update = now
        elapsed = now - self.start_time
        speed = (self.processed_bytes / 1024 / 1024) / max(elapsed, 0.1)
        percentage = (self.processed_bytes / self.total_size) * 100
        
        # Clamp percentage
        percentage = min(100.0, max(0.0, percentage))
        
        bar_length = 25
        filled = int(bar_length * percentage / 100)
        bar = '█' * filled + '░' * (bar_length - filled)
        
        status = (
            f"\r{self.prefix} [{bar}] {percentage:5.1f}% | "
            f"{self.processed_bytes/1024/1024:7.1f}/{self.total_size/1024/1024:7.1f} MB | "
            f"⚡ {speed:5.2f} MB/s"
        )
        sys.stdout.write(status)
        sys.stdout.flush()

# --- PARALLEL DOWNLOAD ENGINE ---

async def parallel_download(client, message, output_path):
    """
    Downloads file using parallel connections for maximum speed.
    """
    print(f"📡 Initializing High-Speed Connection...")
    
    file_size = message.file.size
    tracker = ProgressTracker(file_size, prefix='📥 FastDL')
    
    # Pre-allocate file
    async with aiofiles.open(output_path, 'wb') as f:
        await f.seek(file_size - 1)
        await f.write(b'\0')
    
    part_size = 512 * 1024  # 512KB parts for granular parallelization
    total_parts = math.ceil(file_size / part_size)
    queue = asyncio.Queue()
    
    # Fill queue with part indices
    for i in range(total_parts):
        queue.put_nowait(i)
        
    async def worker(worker_id):
        while not queue.empty():
            part_index = await queue.get()
            offset = part_index * part_size
            current_part_size = min(part_size, file_size - offset)
            
            try:
                async for chunk in client.iter_download(
                    message.media, 
                    offset=offset, 
                    request_size=current_part_size,
                    limit=current_part_size
                ):
                    async with aiofiles.open(output_path, 'r+b') as f:
                        await f.seek(offset)
                        await f.write(chunk)
                    tracker.update(len(chunk))
            except Exception as e:
                # Simple retry logic
                await asyncio.sleep(1)
                await queue.put(part_index)
            finally:
                queue.task_done()

    # Launch workers
    tasks = [asyncio.create_task(worker(i)) for i in range(DOWNLOAD_WORKERS)]
    await queue.join()
    for t in tasks: t.cancel()
    
    print(f"\n✅ Download Complete. Size: {file_size/1024/1024:.2f} MB")

# --- SMART VIDEO PIPELINE ---

def get_video_info(input_path):
    cmd = f"ffprobe -v quiet -print_format json -show_streams -show_format '{input_path}'"
    out, _, _ = run_command(cmd)
    try:
        data = json.loads(out)
        video_stream = next((s for s in data['streams'] if s['codec_type'] == 'video'), None)
        return video_stream, data.get('format', {})
    except:
        return None, None

def process_video_pipeline(input_path):
    print(f"\n🛠️  PIPELINE: Inspecting Media...")
    
    video_stream, fmt_info = get_video_info(input_path)
    if not video_stream:
        print("❌ Error: Invalid video file or no video stream.")
        return None, None

    codec = video_stream.get('codec_name', 'unknown')
    width = video_stream.get('width', 0)
    height = video_stream.get('height', 0)
    duration = float(fmt_info.get('duration', 0))
    
    print(f"   📊 Specs: {codec.upper()} | {width}x{height} | {duration/60:.1f} mins")

    output_video = "processed_video.mp4"
    needs_transcode = False
    
    # HEVC/x265 Detection -> Auto Fix
    if 'hevc' in codec or 'h265' in codec:
        print("   ⚠️  HEVC (x265) Detected: Transcoding to x264 for YouTube compatibility...")
        needs_transcode = True
    
    # Subtitle Extraction
    print("   📜 Checking for subtitles...")
    sub_file = "subs.srt"
    run_command(f"ffmpeg -i '{input_path}' -map 0:s:0 -c:s srt '{sub_file}' -y")
    has_sub = os.path.exists(sub_file) and os.path.getsize(sub_file) > 50

    if needs_transcode:
        # CRF 20 is visually lossless, preset fast balances speed/compression
        cmd = (
            f"ffmpeg -i '{input_path}' "
            f"-c:v libx264 -crf 20 -preset fast -profile:v high -level 4.0 "
            f"-c:a aac -b:a 192k "
            f"-movflags +faststart -y '{output_video}'"
        )
    else:
        # Fast copy if codec is already okay
        print("   ✅ Codec is safe. Remuxing container...")
        cmd = f"ffmpeg -i '{input_path}' -c:v copy -c:a copy -movflags +faststart -y '{output_video}'"

    # Run FFmpeg
    t0 = time.time()
    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    print(f"   ⏳ Processing video (This may take time)...")
    while proc.poll() is None:
        time.sleep(2)
        sys.stdout.write(".")
        sys.stdout.flush()
    print()
    
    if proc.returncode != 0:
        print("   ❌ FFmpeg Error. Attempting to upload original as fallback.")
        return input_path, (sub_file if has_sub else None)

    if os.path.exists(output_video) and os.path.getsize(output_video) > 1024:
        print(f"   ✨ Pipeline Success in {time.time()-t0:.1f}s")
        return output_video, (sub_file if has_sub else None)
    
    return input_path, None

# --- METADATA & UPLOAD ---

async def get_cinematic_metadata(filename):
    print(f"🧠 AI: Generating Premium Metadata...")
    clean_name = os.path.splitext(filename)[0].replace('.', ' ').replace('_', ' ')
    
    if not GEMINI_API_KEY:
        return {"title": clean_name[:95], "description": "High Quality Upload", "tags": ["video"]}

    gemini_url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
    
    # Strict prompt for Fancy UI
    prompt = (
        f"Target Video: '{filename}'.\n"
        "Role: Metadata Editor for Netflix/HBO.\n"
        "Rules:\n"
        "1. TITLE: Must be clean. Format: 'Title (Year) | 4K' or 'Show - S01E01 - Title'. Max 100 chars.\n"
        "2. FORBIDDEN: Do not use 'Trailer', 'Teaser', 'Download', 'Link', 'Official'.\n"
        "3. DESCRIPTION: 3-sentence engaging synopsis. Then a 'Cast:' list.\n"
        "4. Output JSON: {'title': '...', 'description': '...', 'tags': ['tag1']}"
    )

    try:
        payload = {"contents": [{"parts": [{"text": prompt}]}], "generationConfig": {"responseMimeType": "application/json"}}
        res = requests.post(gemini_url, json=payload, timeout=20)
        if res.status_code == 200:
            data = json.loads(res.json()['candidates'][0]['content']['parts'][0]['text'])
            return data
    except Exception as e:
        print(f"   ⚠️ Metadata Error: {e}")
    
    return {"title": clean_name[:95], "description": "Video Upload", "tags": ["video"]}

def upload_to_youtube(video_path, metadata, sub_path):
    try:
        # Pre-Upload Dashboard
        size_mb = os.path.getsize(video_path) / (1024 * 1024)
        print("\n" + "▒"*50)
        print(f" 🎬  READY FOR UPLOAD")
        print(f" 🏷️   Title:   {metadata['title']}")
        print(f" 📦  Size:    {size_mb:.2f} MB")
        print(f" 📝  Subs:    {'Yes' if sub_path else 'No'}")
        print("▒"*50 + "\n")

        creds = Credentials(
            token=None, refresh_token=os.environ.get('YOUTUBE_REFRESH_TOKEN'),
            token_uri='https://oauth2.googleapis.com/token',
            client_id=os.environ.get('YOUTUBE_CLIENT_ID'),
            client_secret=os.environ.get('YOUTUBE_CLIENT_SECRET'),
            scopes=YOUTUBE_SCOPES
        )
        creds.refresh(Request())
        youtube = build('youtube', 'v3', credentials=creds)
        
        body = {
            'snippet': {
                'title': metadata['title'][:100],
                'description': metadata['description'],
                'tags': metadata.get('tags', []),
                'categoryId': '24'
            },
            'status': {'privacyStatus': 'private'}
        }
        
        media = MediaFileUpload(video_path, chunksize=1024*1024*5, resumable=True)
        request = youtube.videos().insert(part="snippet,status", body=body, media_body=media)
        
        tracker = ProgressTracker(os.path.getsize(video_path), prefix='☁️  Uploading')
        
        response = None
        while response is None:
            status, response = request.next_chunk()
            if status:
                tracker.update(exact_pos=status.resumable_progress)

        print(f"\n\n🎉 UPLOAD COMPLETE: https://youtu.be/{response['id']}")

        if sub_path:
            print("   INFO: Attaching subtitles...")
            try:
                youtube.captions().insert(
                    part="snippet",
                    body={'snippet': {'videoId': response['id'], 'language': 'en', 'name': 'English'}},
                    media_body=MediaFileUpload(sub_path),
                    sync=True 
                ).execute()
                print("   ✅ Subtitles Attached.")
            except: pass

    except Exception as e:
        print(f"\n🔴 Upload Failed: {e}")

async def process_link(client, link):
    try:
        parts = [p for p in link.strip('/').split('/') if p]
        msg_id = int(parts[-1])
        chat_id = int(f"-100{parts[parts.index('c')+1]}")
        message = await client.get_messages(chat_id, ids=msg_id)
        
        if not message or not message.file:
            print("❌ No media found.")
            return

        raw_name = message.file.name or f"video_{msg_id}.mkv"
        temp_input = f"raw_{raw_name}"
        
        await parallel_download(client, message, temp_input)
        
        final_video, sub_file = process_video_pipeline(temp_input)
        metadata = await get_cinematic_metadata(raw_name)
        
        if final_video:
            upload_to_youtube(final_video, metadata, sub_file)

        # Cleanup
        for f in [temp_input, final_video, sub_file]:
            if f and os.path.exists(f): os.remove(f)

    except Exception as e:
        print(f"\n❌ Error: {e}")

async def main():
    if len(sys.argv) < 2: return
    links = sys.argv[1].split(',')
    
    print("🤖 Telegram Client Starting...")
    client = TelegramClient(
        'bot_session', 
        os.environ['TG_API_ID'], 
        os.environ['TG_API_HASH']
    )
    await client.start(bot_token=TG_BOT_TOKEN)
    
    for link in links:
        await process_link(client, link)
        
    await client.disconnect()

if __name__ == '__main__':
    asyncio.run(main())
