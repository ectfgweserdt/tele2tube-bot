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
from telethon.tl.types import DocumentAttributeVideo
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials
import googleapiclient.errors

# --- 🚀 CONFIGURATION ZONE ---
YOUTUBE_SCOPES = ['https://www.googleapis.com/auth/youtube.force-ssl']
GEMINI_MODEL = "gemini-2.5-flash-preview-09-2025"

# Network & Download Settings
PARALLEL_CONNECTIONS = 8   # Number of simultaneous connections (Telegram limit is usually ~4-8 per DC)
CHUNK_SIZE = 1024 * 1024 * 2 # 2MB chunks for granular control
MAX_RETRIES = 10

# Fetching API Keys
TG_BOT_TOKEN = os.environ.get('TG_BOT_TOKEN', '').strip()
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY', '').strip()
OMDB_API_KEY = os.environ.get('OMDB_API_KEY', '').strip()

# --- 🛠️ UTILS ---
def run_command(command):
    """Runs a shell command and returns output."""
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    output, error = process.communicate()
    return output.decode(), error.decode(), process.returncode

class ProgressTracker:
    def __init__(self, total_size, prefix='🚀'):
        self.total_size = total_size
        self.downloaded = 0
        self.start_time = time.time()
        self.prefix = prefix
        self.last_ui_update = 0
        self.lock = asyncio.Lock()

    async def update(self, size):
        async with self.lock:
            self.downloaded += size
            now = time.time()
            if now - self.last_ui_update < 0.5 and self.downloaded < self.total_size:
                return

            self.last_ui_update = now
            elapsed = now - self.start_time
            if elapsed == 0: elapsed = 0.1
            speed = (self.downloaded / 1024 / 1024) / elapsed
            percentage = (self.downloaded / self.total_size) * 100
            
            # CLI Animation
            bar_len = 25
            filled = int(bar_len * percentage / 100)
            bar = '█' * filled + '░' * (bar_len - filled)
            
            sys.stdout.write(
                f"\r{self.prefix} |{bar}| {percentage:5.1f}% "
                f"[{self.downloaded//1024//1024}/{self.total_size//1024//1024}MB] "
                f"⚡ {speed:5.2f} MB/s "
            )
            sys.stdout.flush()

# --- 📥 SMART PARALLEL DOWNLOADER ---
class SmartDownloader:
    @staticmethod
    async def download_worker(client, location, start, end, file_path, tracker, part_id):
        """Downloads a specific byte range of the file."""
        current_offset = start
        retry_count = 0
        
        while current_offset < end:
            try:
                # Calculate chunk size (don't overflow 'end')
                request_size = min(CHUNK_SIZE, end - current_offset)
                
                # Telethon iter_download with offset/limit
                async for chunk in client.iter_download(
                    location, offset=current_offset, limit=request_size, chunk_size=request_size, request_size=request_size
                ):
                    # Write directly to specific file position (Thread-safe file IO required)
                    # We open the file in r+b mode for each write to avoid seeking conflicts or use a shared handle
                    # For safety in async, we'll open-seek-write-close or use aiofiles with a lock if needed.
                    # Since OS file handles have state, we need to be careful.
                    # Fast approach: specific open for this write or shared handle with lock.
                    
                    with open(file_path, "r+b") as f:
                        f.seek(current_offset)
                        f.write(chunk)
                    
                    written = len(chunk)
                    current_offset += written
                    await tracker.update(written)
                    
                    if current_offset >= end:
                        break
            except Exception as e:
                retry_count += 1
                if retry_count > MAX_RETRIES:
                    print(f"\n❌ Chunk {part_id} failed after retries: {e}")
                    raise e
                await asyncio.sleep(1 * retry_count)

    @staticmethod
    async def download(client, message, file_path):
        print(f"\n📡 Initiating 50MBps+ Parallel Download Pipeline...")
        
        file_size = message.file.size
        
        # 1. Pre-allocate disk space (prevents fragmentation and download overload)
        with open(file_path, "wb") as f:
            f.truncate(file_size)
            
        # 2. Split into parts
        part_size = file_size // PARALLEL_CONNECTIONS
        tasks = []
        tracker = ProgressTracker(file_size, prefix='📥 DL')
        
        location = message.document
        
        for i in range(PARALLEL_CONNECTIONS):
            start = i * part_size
            end = start + part_size if i < PARALLEL_CONNECTIONS - 1 else file_size
            
            # Create async worker for this chunk
            task = asyncio.create_task(
                SmartDownloader.download_worker(client, location, start, end, file_path, tracker, i)
            )
            tasks.append(task)
            
        # 3. Wait for all parts
        await asyncio.gather(*tasks)
        
        print(f"\n✅ Download Verified: {file_size / 1024 / 1024:.2f} MB")

# --- 🎥 INTELLIGENT VIDEO PIPELINE ---
class VideoPipeline:
    @staticmethod
    def get_stream_info(input_path):
        cmd = f"ffprobe -v quiet -print_format json -show_streams -show_format '{input_path}'"
        out, _, _ = run_command(cmd)
        try:
            return json.loads(out)
        except:
            return {}

    @staticmethod
    def process(input_path):
        print("\n🔍 Analyzing Codecs & Container...")
        info = VideoPipeline.get_stream_info(input_path)
        streams = info.get('streams', [])
        video_stream = next((s for s in streams if s['codec_type'] == 'video'), None)
        
        if not video_stream:
            print("⚠️ No video stream found!")
            return input_path, None

        codec = video_stream.get('codec_name', 'unknown')
        width = int(video_stream.get('width', 0))
        print(f"   Detected: Codec [{codec}] | Res [{width}p]")

        output_path = "processed_video.mp4"
        sub_path = "extracted_subs.srt"
        
        # --- LOGIC: x265/HEVC Detection ---
        # YouTube often drops frames on x265 MKVs. We enforce H.264 if HEVC is found.
        needs_transcode = False
        if codec == 'hevc' or codec == 'vp9':
            print("🚨 High-Efficiency Codec detected (HEVC/VP9). Transcoding to H.264 for YouTube stability...")
            needs_transcode = True
        
        # Extract English Subs if available
        print("📜 Extracting subtitles...")
        run_command(f"ffmpeg -y -i '{input_path}' -map 0:s:0? -c:s srt '{sub_path}'")
        has_subs = os.path.exists(sub_path) and os.path.getsize(sub_path) > 100
        
        # Build FFmpeg Command
        if needs_transcode:
            # High Quality, Fast Preset for GitHub Actions
            # CRF 23 is visual standard. Preset 'fast' gives 50fps+ on modern CPUs.
            cmd = (
                f"ffmpeg -y -v error -i '{input_path}' "
                f"-c:v libx264 -preset fast -crf 23 -pix_fmt yuv420p "
                f"-c:a copy " # Copy audio (usually AAC/AC3 is fine)
                f"'{output_path}'"
            )
        else:
            # Fast Remux (Copy) - Changes container to MP4 without re-encoding
            print("✅ Codec is safe. Performing Fast Remux...")
            cmd = f"ffmpeg -y -v error -i '{input_path}' -c copy -map 0:v:0 -map 0:a:0? '{output_path}'"

        # Execute
        t0 = time.time()
        out, err, code = run_command(cmd)
        
        if code != 0:
            print(f"❌ FFmpeg Error: {err}")
            # Fallback: Try a super-safe re-encode if copy failed
            print("⚠️ Retrying with Safe Mode...")
            run_command(f"ffmpeg -y -i '{input_path}' -c:v libx264 -preset veryfast -c:a aac '{output_path}'")

        print(f"⏱️ Processing finished in {time.time() - t0:.2f}s")
        
        # Gather stats for UI
        final_info = VideoPipeline.get_stream_info(output_path)
        stats = {
            "res": f"{width}p",
            "size": f"{os.path.getsize(output_path)/1024/1024:.1f} MB",
            "codec": "H.264 (YouTube Optimized)"
        }
        
        return output_path, (sub_path if has_subs else None), stats

# --- 🧠 AI METADATA ENGINE ---
class MetadataEngine:
    @staticmethod
    def get_clean_metadata(filename, file_stats, omdb_key=None, gemini_key=None):
        print("🤖 AI is designing the YouTube page...")
        
        # 1. Clean filename locally first
        clean_name = re.sub(r'\.|_|20\d\d|1080p|720p|WEBRip|Bluray|x265|HEVC|AAC|5\.1', ' ', filename)
        clean_name = re.sub(r'\s+', ' ', clean_name).strip()
        
        omdb_data = {}
        if omdb_key:
            try:
                # Basic OMDb lookup
                search = clean_name.split('S0')[0].strip()
                res = requests.get(f"http://www.omdbapi.com/?t={search}&apikey={omdb_key}")
                if res.status_code == 200: omdb_data = res.json()
            except: pass

        if not gemini_key:
            return {"title": clean_name, "description": "Uploaded via FastBot", "tags": ["video"]}

        # 2. Advanced Gemini Prompt
        prompt = f"""
        Analyze this filename: "{filename}"
        OMDb Data: {json.dumps(omdb_data)}
        
        Task: Create a Premium YouTube Video Metadata set (Netflix/HBO style).
        
        Guidelines:
        1. TITLE: Clean, Professional. Format: "Movie Name (Year) | 4K HDR" or "Show Name - S01E01 - Episode Title". 
           - REMOVE words like: Trailer, Teaser, Official, Hindi, Lat, Eng, MKV, MP4.
           - If it's a TV show, identify Season/Episode from filename.
        
        2. DESCRIPTION:
           - "🎬 Synopsis": A gripping 3-sentence summary.
           - "🎭 Cast": Main actors.
           - "🍿 Genre": e.g., Action, Thriller.
           - "📅 Release": Year.
        
        3. TAGS: Comma separated list of 15 relevant tags.
        
        Return STRICT JSON: {{ "title": "...", "description": "...", "tags": [...] }}
        """
        
        try:
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={gemini_key}"
            payload = {"contents": [{"parts": [{"text": prompt}]}], "generationConfig": {"responseMimeType": "application/json"}}
            res = requests.post(url, json=payload, timeout=20)
            data = res.json()['candidates'][0]['content']['parts'][0]['text']
            meta = json.loads(data)
            
            # 3. Append Technical Stats (User Request: "Add more information")
            tech_footer = (
                f"\n\n━━━━━━━━━━━━━━━━━━━━━━\n"
                f"⚙️ File Information:\n"
                f"🔹 Resolution: {file_stats.get('res', 'HD')}\n"
                f"🔹 File Size: {file_stats.get('size', 'N/A')}\n"
                f"🔹 Processing: {file_stats.get('codec', 'Standard')}\n"
            )
            meta['description'] += tech_footer
            return meta
            
        except Exception as e:
            print(f"⚠️ AI Failed: {e}. Using fallback.")
            return {"title": clean_name, "description": f"Processed Upload.\n{json.dumps(file_stats)}", "tags": ["video"]}

# --- 📤 YOUTUBE UPLOADER ---
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
        
        print(f"\n🚀 Starting YouTube Upload: {metadata['title']}")
        
        body = {
            'snippet': {
                'title': metadata['title'][:95], # YT limit 100
                'description': metadata['description'],
                'tags': metadata['tags'][:15],
                'categoryId': '24' # Entertainment
            },
            'status': {'privacyStatus': 'private'}
        }
        
        media = MediaFileUpload(video_path, chunksize=1024*1024*5, resumable=True)
        request = youtube.videos().insert(part="snippet,status", body=body, media_body=media)
        
        tracker = ProgressTracker(os.path.getsize(video_path), prefix='📤 Uploading')
        response = None
        while response is None:
            status, response = request.next_chunk()
            if status:
                asyncio.run(tracker.update(status.resumable_progress - tracker.downloaded))

        video_id = response['id']
        print(f"\n✨ Upload Complete! Video ID: {video_id}")

        if sub_path:
            print("📜 Uploading Subtitles...")
            try:
                youtube.captions().insert(
                    part="snippet",
                    body={'snippet': {'videoId': video_id, 'language': 'en', 'name': 'English', 'isDraft': False}},
                    media_body=MediaFileUpload(sub_path),
                    sync=True 
                ).execute()
            except: print("⚠️ Subtitle upload failed (might be duplicate).")
            
        print(f"🎉 FINAL LINK: https://youtu.be/{video_id}")
        
    except Exception as e:
        print(f"🔴 Upload Error: {e}")

# --- 🏃‍♂️ MAIN LOOP ---
async def process_link(client, link):
    try:
        # Link Parsing
        parts = [p for p in link.strip('/').split('/') if p]
        if 't.me' not in link: return
        
        # Support for public and private links
        if 'c' in parts:
            chat_id = int(f"-100{parts[parts.index('c')+1]}")
            msg_id = int(parts[-1])
        else:
            chat_id = parts[-2]
            msg_id = int(parts[-1])

        message = await client.get_messages(chat_id, ids=msg_id)
        if not message or not message.media:
            print("❌ No media found in message.")
            return

        raw_file = f"downloaded_media_{msg_id}.mkv"
        
        # 1. Download
        await SmartDownloader.download(client, message, raw_file)
        
        # 2. Process
        final_video, sub_file, stats = VideoPipeline.process(raw_file)
        
        # 3. Metadata
        meta = MetadataEngine.get_clean_metadata(message.file.name or "Unknown Video", stats, OMDB_API_KEY, GEMINI_API_KEY)
        
        # 4. Upload
        upload_to_youtube(final_video, meta, sub_file)

        # Cleanup
        for f in [raw_file, final_video, sub_file, "processed_video.mp4"]:
            if f and os.path.exists(f): os.remove(f)
            
    except Exception as e:
        print(f"❌ Critical Error: {e}")
        import traceback
        traceback.print_exc()

async def main():
    if len(sys.argv) < 2: 
        print("Usage: python uploader.py <links>")
        return
        
    links = sys.argv[1].split(',')
    
    print("🔌 Connecting to Telegram...")
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
