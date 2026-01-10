import os
import sys
import time
import asyncio
import subprocess
import cv2
import numpy as np
import math
from telethon import TelegramClient
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials

# --- STEGANOGRAPHY CONFIG ---
YOUTUBE_SCOPES = ['https://www.googleapis.com/auth/youtube.force-ssl']
TG_BOT_TOKEN = os.environ.get('TG_BOT_TOKEN', '').strip()

def log(message):
    print(message, flush=True)

async def fast_download(client, message, file_path):
    log(f"📡 Downloading from Telegram...")
    await client.download_media(message, file_path)
    log("✅ Download finished.")

def encode_steganography(input_path):
    """
    Converts the video into a 'visually corrupted' version.
    """
    log("🔐 Encoding Video into Steganographic Manifold...")
    output_path = "encoded_manifold.mp4"
    
    # FIX: Corrected 'lutrgb' syntax for negating channels
    # Using explicit '255-val' instead of 'neg'
    v_filter = (
        "geq=r='bitand(X,Y)*1.5':g='bitand(X,Y)*1.1':b='bitand(X,Y)*0.9'," 
        "lutrgb=r='255-val':g='255-val':b='255-val'," 
        "hue=h=180:s=2,"
        "boxblur=1:1"
    )
    
    # Audio: Falling back to 'asetrate' and 'atempo' if rubberband isn't available,
    # though we've added rubberband to main.yml.
    a_filter = "asetrate=44100*1.1,atempo=0.9,aecho=0.8:0.88:60:0.4"

    cmd = (
        f"ffmpeg -i '{input_path}' "
        f"-vf \"{video_filters if 'video_filters' in locals() else v_filter}\" "
        f"-af \"{a_filter}\" "
        f"-c:v libx264 -preset ultrafast -crf 18 -threads 0 "
        f"-c:a aac -b:a 128k -y '{output_path}'"
    )
    
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    if result.returncode != 0:
        log(f"❌ FFmpeg Failed: {result.stderr}")
        return None
        
    if not os.path.exists(output_path) or os.path.getsize(output_path) < 1000:
        log("❌ Output file is empty or too small. Check filters.")
        return None

    return output_path

def upload_to_youtube(video_path):
    if not video_path:
        log("⚠️ No valid video file to upload.")
        return

    try:
        log(f"📤 Uploading Encrypted Manifold...")
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
                'title': f'Data Stream {int(time.time())}',
                'description': 'RECOVERY_KEY: ALPHA-9',
                'categoryId': '24'
            },
            'status': {'privacyStatus': 'unlisted'}
        }
        
        media = MediaFileUpload(video_path, chunksize=1024*1024*5, resumable=True)
        request = youtube.videos().insert(part="snippet,status", body=body, media_body=media)
        
        response = None
        while response is None:
            status, response = request.next_chunk()
            if status: log(f"   > Uploading: {int(status.progress() * 100)}%")

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
        
        encoded_video = encode_steganography(raw_file)
        if encoded_video:
            upload_to_youtube(encoded_video)
            if os.path.exists(encoded_video): os.remove(encoded_video)
        
        if os.path.exists(raw_file): os.remove(raw_file)
    except Exception as e:
        log(f"❌ Error: {e}")

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
