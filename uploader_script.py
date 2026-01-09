import os
import sys
import time
import asyncio
import subprocess
import json
import math
from telethon import TelegramClient
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials

# --- CONFIGURATION ---
YOUTUBE_SCOPES = ['https://www.googleapis.com/auth/youtube.force-ssl']
TG_BOT_TOKEN = os.environ.get('TG_BOT_TOKEN', '').strip()

def log(message):
    print(message, flush=True)

def get_file_size_formatted(file_path):
    if not os.path.exists(file_path): return "0 Bytes"
    size_bytes = os.path.getsize(file_path)
    i = int(math.floor(math.log(size_bytes, 1024))) if size_bytes > 0 else 0
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return f"{s} {('Bytes', 'KB', 'MB', 'GB', 'TB')[i]}"

def run_command(command):
    """Executes system command and returns output."""
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    output, error = process.communicate()
    return output.decode(), error.decode(), process.returncode

async def fast_download(client, message, file_path):
    log(f"📡 Downloading from Telegram...")
    await client.download_media(message, file_path)
    log("✅ Download finished.")

def process_video_ultra_fast(input_path):
    """
    Uses FFmpeg filters to apply all PMR transforms in a single parallel pass.
    This is ~20x faster than processing frames in Python.
    """
    log(f"🛠️  Starting High-Speed Manifold Reconstruction...")
    final_output = "reconstructed_final.mp4"
    
    # EXTREME DECONSTRUCTION FILTER CHAIN:
    # 1. hflip: Mirror video
    # 2. crop/scale: Zoom 5% to break edge signatures
    # 3. noise: Add temporal grain to destroy pixel hashes
    # 4. setsar: Normalize aspect ratio
    # 5. rubberband/atempo: Shift pitch/speed
    
    video_filters = (
        "hflip,"                                 # Mirroring
        "crop=iw*0.95:ih*0.95:iw*0.025:ih*0.025," # Zoom/Crop
        "scale=1280:720:force_original_aspect_ratio=decrease," # Normalize resolution
        "pad=1280:720:(ow-iw)/2:(oh-ih)/2,"      # Letterbox if needed
        "noise=alls=3:allf=t"                    # Dynamic grain
    )
    
    # Audio: pitch shift +0.3 semitones + slight speed increase (1.01x)
    audio_filters = "rubberband=pitch=1.02,atempo=1.01"

    log("🎞️  Running Parallel Filter Pipeline...")
    
    # Using 'ultrafast' preset and multiple threads
    cmd = (
        f"ffmpeg -i '{input_path}' "
        f"-vf \"{video_filters}\" "
        f"-af \"{audio_filters}\" "
        f"-c:v libx264 -preset ultrafast -crf 23 -threads 0 "
        f"-c:a aac -b:a 128k "
        f"-map_metadata -1 -y '{final_output}'"
    )
    
    stdout, stderr, code = run_command(cmd)
    
    if code != 0:
        log(f"❌ FFmpeg Error: {stderr}")
        return input_path
    
    log(f"✅ Reconstruction Complete: {get_file_size_formatted(final_output)}")
    return final_output

def upload_to_youtube(video_path):
    try:
        log(f"📤 Initiating YouTube Upload...")
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
                'title': f'Parallel Render {int(time.time())}',
                'description': 'Processed via High-Speed PMR Pipeline.',
                'categoryId': '24'
            },
            'status': {'privacyStatus': 'private'}
        }
        
        media = MediaFileUpload(video_path, chunksize=1024*1024*5, resumable=True)
        request = youtube.videos().insert(part="snippet,status", body=body, media_body=media)
        
        response = None
        while response is None:
            status, response = request.next_chunk()
            if status: log(f"   > Upload Progress: {int(status.progress() * 100)}%")

        log(f"✅ SUCCESS: https://youtu.be/{response['id']}")
    except Exception as e:
        log(f"🔴 YouTube Error: {e}")

async def process_link(client, link):
    try:
        parts = [p for p in link.strip('/').split('/') if p]
        msg_id, chat_id = int(parts[-1]), int(f"-100{parts[parts.index('c')+1]}")
        message = await client.get_messages(chat_id, ids=msg_id)
        
        raw_file = f"temp_{msg_id}.mkv"
        await fast_download(client, message, raw_file)
        
        # Process using the new high-speed pipeline
        final_video = process_video_ultra_fast(raw_file)
        
        upload_to_youtube(final_video)

        if os.path.exists(raw_file): os.remove(raw_file)
        if os.path.exists(final_video): os.remove(final_video)
    except Exception as e:
        log(f"❌ Main Error: {e}")

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
