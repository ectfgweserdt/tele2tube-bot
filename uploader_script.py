import os
import sys
import time
import asyncio
import subprocess
import json
import re
from telethon import TelegramClient, errors, utils
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials

# --- PREMIUM CONFIGURATION ---
PARALLEL_CHUNKS = 16 
CHUNK_SIZE = 1024 * 1024 # 1MB

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

    def update(self, current_size):
        now = time.time()
        if now - self.last_ui_update < 3: return
        self.last_ui_update = now
        
        percent = (current_size / self.total_size) * 100
        elapsed = now - self.start_time
        speed = (current_size / 1024 / 1024) / elapsed if elapsed > 0 else 0
        print(f"{self.prefix} {percent:.1f}% | {speed:.2f} MB/s | {current_size/(1024**2):.1f}/{self.total_size/(1024**2):.1f} MB")

async def fast_download(client, message, file_path):
    """Stable High-Speed Downloader."""
    print(f"📥 Starting High-Speed Download ({message.file.size / 1024**2:.1f} MB)...")
    tracker = ProgressTracker(message.file.size, prefix='📡')
    
    try:
        # Telethon's download_media is optimized when cryptg is present
        await client.download_media(
            message,
            file=file_path,
            progress_callback=lambda c, t: tracker.update(c)
        )
    except Exception as e:
        print(f"⚠️ High-speed fail, retrying: {e}")
        await message.download_media(file=file_path)

def process_video_advanced(input_path):
    """Error correction and Codec Upgrade Pipeline."""
    print("🔍 Pipeline: Analyzing streams and metadata...")
    
    # 1. Probe the file
    probe_cmd = f"ffprobe -v quiet -print_format json -show_streams -show_format '{input_path}'"
    out, _, code = run_command(probe_cmd)
    if code != 0: raise Exception("File Probing Failed: Potential Corruption")
    
    info = json.loads(out)
    v_stream = next((s for s in info['streams'] if s['codec_type'] == 'video'), None)
    has_subs = any(s['codec_type'] == 'subtitle' for s in info['streams'])
    
    codec = v_stream.get('codec_name', 'unknown')
    needs_fix = ('265' in codec or 'hevc' in codec)
    
    # Check for internal corruption
    check_cmd = f"ffmpeg -v error -i '{input_path}' -f null - 2>&1"
    stream_errors, _, _ = run_command(check_cmd)
    if stream_errors.strip():
        print("🚨 Stream errors detected! Forcing repair transcode...")
        needs_fix = True

    output_path = f"ready_{input_path}"
    
    if needs_fix:
        print(f"⚙️ Transcoding {codec} to H.264 (YouTube Optimized - Max Quality)...")
        # -crf 17 is visually lossless, -preset fast balances speed/quality
        # -map 0 ensures all streams (including subs) are carried over
        ffmpeg_cmd = (
            f"ffmpeg -y -i '{input_path}' -c:v libx264 -crf 17 -preset fast "
            f"-pix_fmt yuv420p -c:a aac -b:a 192k -movflags +faststart '{output_path}'"
        )
        _, err, code = run_command(ffmpeg_cmd)
        if code != 0: 
            print(f"❌ Transcode failed: {err}")
            return input_path, info, has_subs
        return output_path, info, has_subs
    
    return input_path, info, has_subs

async def generate_premium_metadata(filename, info, has_subs):
    """Generates Netflix/HBO style interface data."""
    # Clean the title (Regex to remove "Teaser", "Trailer", "1080p", etc.)
    clean_title = re.sub(r'(?i)(trailer|teaser|web-dl|hdtv|1080p|720p|x26\d|hevc|bluray|[\.\-_])', ' ', filename).strip()
    clean_title = ' '.join(clean_title.split()).title()
    
    size_gb = int(info['format']['size']) / (1024**3)
    duration = float(info['format']['duration']) / 60
    resolution = f"{info['streams'][0].get('width', '?')}p"
    
    description = (
        f"💎 PREMIUM UPLOAD: {clean_title}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🎬 Title: {clean_title}\n"
        f"📏 Size: {size_gb:.2f} GB\n"
        f"⏱️ Runtime: {duration:.1f} Minutes\n"
        f"📺 Quality: {resolution}\n"
        f"⌨️ Subtitles: {'✅ Embedded' if has_subs else '❌ None'}\n"
        f"🚀 Optimized for YouTube playback\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"#Cinema #PremiumQuality #YouTubeUpload"
    )
    
    print(f"\n--- PRE-UPLOAD SUMMARY ---\n{description}\n--------------------------")
    return clean_title, description

def upload_to_youtube(file_path, title, description):
    """Uploads with resumable media to prevent crashes."""
    print(f"📤 Uploading to YouTube...")
    
    creds = Credentials(
        None,
        refresh_token=os.environ['YOUTUBE_REFRESH_TOKEN'],
        token_uri="https://oauth2.googleapis.com/token",
        client_id=os.environ['YOUTUBE_CLIENT_ID'],
        client_secret=os.environ['YOUTUBE_CLIENT_SECRET']
    )
    
    youtube = build("youtube", "v3", credentials=creds)
    
    body = {
        'snippet': {'title': title[:100], 'description': description, 'categoryId': '24'},
        'status': {'privacyStatus': 'unlisted', 'selfDeclaredMadeForKids': False}
    }
    
    media = MediaFileUpload(file_path, chunksize=1024*1024*5, resumable=True)
    request = youtube.videos().insert(part=','.join(body.keys()), body=body, media_body=media)
    
    response = None
    while response is None:
        status, response = request.next_chunk()
        if status:
            print(f"🚀 Upload: {int(status.progress() * 100)}%")
            
    print(f"✅ Upload Success! ID: {response['id']}")

async def process_link(client, link):
    try:
        parts = [p for p in link.strip('/').split('/') if p]
        msg_id, chat_id = int(parts[-1]), int(f"-100{parts[parts.index('c')+1]}")
        message = await client.get_messages(chat_id, ids=msg_id)
        
        raw_file = f"dl_{msg_id}.mkv"
        
        # 1. High Speed Stable Download
        await fast_download(client, message, raw_file)
        
        # 2. Pipeline: Fix x265 and Corruption
        final_file, info, has_subs = process_video_advanced(raw_file)
        
        # 3. UI/Metadata Prep
        title, desc = await generate_premium_metadata(message.file.name or raw_file, info, has_subs)
        
        # 4. YouTube Upload
        upload_to_youtube(final_file, title, desc)

        # Cleanup
        for f in [raw_file, final_file]:
            if f and os.path.exists(f): os.remove(f)
            
    except Exception as e:
        print(f"❌ Error in pipeline: {e}")

async def main():
    if len(sys.argv) < 2: return
    links = sys.argv[1].split(',')
    
    client = TelegramClient('session', os.environ['TG_API_ID'], os.environ['TG_API_HASH'])
    await client.start(bot_token=os.environ['TG_BOT_TOKEN'])
    
    for link in links:
        await process_link(client, link)
    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
