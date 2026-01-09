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

def run_command(command):
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    output, error = process.communicate()
    return output.decode(), error.decode(), process.returncode

async def fast_download(client, message, file_path):
    log(f"📡 Downloading from Telegram...")
    await client.download_media(message, file_path)
    log("✅ Download finished.")

def process_video_quantum(input_path):
    """
    Implements Non-Linear Temporal Warping and Perspective Distortion.
    This breaks both 'shazam-style' audio checks and scene-descriptor visual checks.
    """
    log(f"🛠️  Starting Quantum Manifold Reconstruction...")
    final_output = "reconstructed_final.mp4"
    
    # EXTREME DECONSTRUCTION PIPELINE:
    # 1. Perspective Distort: Tilts the video slightly in 3D space so objects are skewed.
    # 2. hflip: Mirrors the entire world.
    # 3. Vignette: Changes the light distribution across the frame.
    # 4. Color Curves: Non-linear color remapping (crushing blacks, lifting whites).
    # 5. Noise: Adds 'salt and pepper' temporal noise.
    
    video_filters = (
        "hflip,"
        "perspective=x0=0.01*W:y0=0.01*H:x1=0.99*W:y1=0:x2=0:y2=0.99*H:x3=W:y3=W," # 3D Tilt
        "curves=preset=lighter," # Breaks histogram matching
        "vignette=angle=0.3,"     # Removes edge fingerprints
        "noise=alls=4:allf=t"    # Temporal pixel scrambling
    )
    
    # AUDIO DESTRUCTION:
    # 1. Pitch Shift (+0.4 semitones) - breaks frequency peak matching.
    # 2. Vibrato: Adds a tiny 0.1Hz oscillation to the pitch so it's never 'flat' compared to the original.
    # 3. Flanger: Adds sub-perceptual phase sweeps.
    # 4. Atempo: 1.02x speed.
    
    audio_filters = (
        "rubberband=pitch=1.03," # Aggressive pitch shift
        "vibrato=f=0.1:d=0.2,"   # Constant pitch movement
        "flanger=delay=0.5:depth=0.2," # Phase scrambling
        "atempo=1.02"            # Temporal misalignment
    )

    log("🎞️  Executing High-Entropy Filter Chain...")
    
    # Multi-threaded encode with 'ultrafast' to maintain speed
    cmd = (
        f"ffmpeg -i '{input_path}' "
        f"-vf \"{video_filters}\" "
        f"-af \"{audio_filters}\" "
        f"-c:v libx264 -preset ultrafast -crf 20 -threads 0 "
        f"-c:a aac -b:a 128k "
        f"-map_metadata -1 -y '{final_output}'"
    )
    
    stdout, stderr, code = run_command(cmd)
    
    if code != 0:
        log(f"❌ FFmpeg Error: {stderr}")
        return input_path
    
    return final_output

def upload_to_youtube(video_path):
    try:
        log(f"📤 Uploading Reconstructed Content...")
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
                'title': f'Unique Render {int(time.time())}',
                'description': 'Mathematically unique signal.',
                'categoryId': '24'
            },
            'status': {'privacyStatus': 'private'}
        }
        
        media = MediaFileUpload(video_path, chunksize=1024*1024*5, resumable=True)
        request = youtube.videos().insert(part="snippet,status", body=body, media_body=media)
        
        response = None
        while response is None:
            status, response = request.next_chunk()
            if status: log(f"   > Upload: {int(status.progress() * 100)}%")

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
        
        final_video = process_video_quantum(raw_file)
        upload_to_youtube(final_video)

        for f in [raw_file, final_video]:
            if os.path.exists(f): os.remove(f)
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
