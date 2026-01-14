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
# 8 connections is optimal for Telegram's DC limits
PARALLEL_CONNECTIONS = 8   
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
            # Update UI every 0.5s or if complete
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

# --- 📥 SMART PARALLEL DOWNLOADER (v2.1 Fix) ---
class SmartDownloader:
    @staticmethod
    async def download_worker(client, location, start, end, file_path, tracker, part_id, io_lock):
        """Downloads a large segment using a persistent file handle to prevent locking issues."""
        current = start
        
        # Open file ONCE per worker (fixes open/close overhead hang)
        async with aiofiles.open(file_path, "r+b") as f:
            while current < end:
                try:
                    bytes_left = end - current
                    if bytes_left <= 0: break
                    
                    # Request chunks
                    async for chunk in client.iter_download(
                        location,
                        offset=current,
                        limit=bytes_left,
                        chunk_size=512 * 1024, 
                        request_size=512 * 1024 
                    ):
                        if not chunk: break
                        
                        chunk_len = len(chunk)
                        
                        # ATOMIC WRITE: Ensure no two workers seek/write at the same time
                        async with io_lock:
                            await f.seek(current)
                            await f.write(chunk)
                        
                        current += chunk_len
                        await tracker.update(chunk_len)
                        
                        if current >= end: break
                    
                    # Double check loop exit
                    if current >= end: break
                    
                except Exception as e:
                    print(f"\n⚠️ Part {part_id} error: {e}. Retrying...")
                    await asyncio.sleep(2)

    @staticmethod
    async def download(client, message, file_path):
        print(f"\n📡 Initiating High-Speed Parallel Download...")
        
        file_size = message.file.size
        
        # 1. Pre-allocate disk space
        with open(file_path, "wb") as f:
            f.truncate(file_size)
            
        # 2. Setup workers
        part_size = file_size // PARALLEL_CONNECTIONS
        tasks = []
        tracker = ProgressTracker(file_size, prefix='📥 DL')
        io_lock = asyncio.Lock() # Global lock for file IO
        
        location = message.document
        
        for i in range(PARALLEL_CONNECTIONS):
            start = i * part_size
            end = start + part_size if i < PARALLEL_CONNECTIONS - 1 else file_size
            
            task = asyncio.create_task(
                SmartDownloader.download_worker(client, location, start, end, file_path, tracker, i, io_lock)
            )
            tasks.append(task)
            
        # 3. Wait for all parts
        await asyncio.gather(*tasks)
        
        # Force a final newline for UI cleanup
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
            return input_path, None, {}

        codec = video_stream.get('codec_name', 'unknown')
        width = int(video_stream.get('width', 0))
        print(f"   Detected: Codec [{codec}] | Res [{width}p]")

        output_path = "processed_video.mp4"
        sub_path = "extracted_subs.srt"
        
        # --- LOGIC: x265/HEVC Detection ---
        needs_transcode = False
        if codec in ['hevc', 'vp9', 'av1']:
            print("🚨 High-Efficiency Codec detected. Transcoding to H.264 for YouTube stability...")
            needs_transcode = True
        
        # Extract English Subs
        print("📜 Extracting subtitles...")
        run_command(f"ffmpeg -y -i '{input_path}' -map 0:s:0? -c:s srt '{sub_path}'")
        has_subs = os.path.exists(sub_path) and os.path.getsize(sub_path) > 100
        
        if needs_transcode:
            # CRF 23 = High Quality, Preset Fast = Good Speed
            cmd = (
                f"ffmpeg -y -v error -i '{input_path}' "
                f"-c:v libx264 -preset fast -crf 23 -pix_fmt yuv420p "
                f"-c:a copy "
                f"'{output_path}'"
            )
        else:
            # Fast Remux
            print("✅ Codec is safe. Performing Fast Remux...")
            cmd = f"ffmpeg -y -v error -i '{input_path}' -c copy -map 0:v:0 -map 0:a:0? '{output_path}'"

        t0 = time.time()
        out, err, code = run_command(cmd)
        
        if code != 0:
            print(f"❌ FFmpeg Error: {err}")
            print("⚠️ Retrying with Safe Mode...")
            run_command(f"ffmpeg -y -i '{input_path}' -c:v libx264 -preset veryfast -c:a aac '{output_path}'")

        print(f"⏱️ Processing finished in {time.time() - t0:.2f}s")
        
        # Gather stats
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
        
        clean_name = re.sub(r'\.|_|20\d\d|1080p|720p|WEBRip|Bluray|x265|HEVC|AAC|5\.1', ' ', filename)
        clean_name = re.sub(r'\s+', ' ', clean_name).strip()
        
        omdb_data = {}
        if omdb_key:
            try:
                search = clean_name.split('S0')[0].strip()
                res = requests.get(f"http://www.omdbapi.com/?t={search}&apikey={omdb_key}")
                if res.status_code == 200: omdb_data = res.json()
            except: pass

        if not gemini_key:
            return {"title": clean_name, "description": "Uploaded via FastBot", "tags": ["video"]}

        prompt = f"""
        Analyze this filename: "{filename}"
        OMDb Data: {json.dumps(omdb_data)}
        
        Task: Create a Premium YouTube Video Metadata set (Netflix/HBO style).
        
        Guidelines:
        1. TITLE: Clean, Professional. Format: "Movie Name (Year) | 4K HDR" or "Show Name - S01E01 - Episode Title". 
           - REMOVE words like: Trailer, Teaser, Official, Hindi, Lat, Eng, MKV, MP4.
        
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
                'title': metadata['title'][:95],
                'description': metadata['description'],
                'tags': metadata['tags'][:15],
                'categoryId': '24'
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
            except: print("⚠️ Subtitle upload failed.")
            
        print(f"🎉 FINAL LINK: https://youtu.be/{video_id}")
        
    except Exception as e:
        print(f"🔴 Upload Error: {e}")

# --- 🏃‍♂️ MAIN LOOP ---
async def process_link(client, link):
    try:
        parts = [p for p in link.strip('/').split('/') if p]
        if 't.me' not in link: return
        
        if 'c' in parts:
            chat_id = int(f"-100{parts[parts.index('c')+1]}")
            msg_id = int(parts[-1])
        else:
            chat_id = parts[-2]
            msg_id = int(parts[-1])

        message = await client.get_messages(chat_id, ids=msg_id)
        if not message or not message.media:
            print("❌ No media found.")
            return

        raw_file = f"downloaded_media_{msg_id}.mkv"
        
        await SmartDownloader.download(client, message, raw_file)
        
        final_video, sub_file, stats = VideoPipeline.process(raw_file)
        
        meta = MetadataEngine.get_clean_metadata(message.file.name or "Unknown Video", stats, OMDB_API_KEY, GEMINI_API_KEY)
        
        upload_to_youtube(final_video, meta, sub_file)

        for f in [raw_file, final_video, sub_file, "processed_video.mp4"]:
            if f and os.path.exists(f): os.remove(f)
            
    except Exception as e:
        print(f"❌ Critical Error: {e}")
        import traceback
        traceback.print_exc()

async def main():
    if len(sys.argv) < 2: return
    links = sys.argv[1].split(',')
    
    print("🔌 Connecting...")
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
