import os
import sys
import time
import asyncio
import subprocess
import json
import re
import requests
import math
import numpy as np
import cv2
import librosa
import soundfile as sf
from telethon import TelegramClient, errors, utils
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials
import googleapiclient.errors

# --- AGGRESSIVE RECONSTRUCTION CONFIG ---
YOUTUBE_SCOPES = ['https://www.googleapis.com/auth/youtube.force-ssl']
TG_BOT_TOKEN = os.environ.get('TG_BOT_TOKEN', '').strip()

def log(message):
    print(message, flush=True)

class PMREngine:
    def __init__(self, intensity=0.015): # Increased default intensity
        self.intensity = intensity

    def apply_transforms(self, frame):
        h, w = frame.shape[:2]
        
        # 1. Aggressive Diffeomorphic Warp
        x, y = np.meshgrid(np.arange(w), np.arange(h))
        # More complex sine-wave interference for deeper warping
        dx = (np.sin(x / 30.0) + np.sin(y / 50.0)) * self.intensity * 10
        dy = (np.cos(y / 30.0) + np.cos(x / 50.0)) * self.intensity * 10
        map_x = (x + dx).astype(np.float32)
        map_y = (y + dy).astype(np.float32)
        frame = cv2.remap(frame, map_x, map_y, interpolation=cv2.INTER_NEAREST)
        
        # 2. Chroma & Gamma Jitter (Breaks Histogram Matching)
        # Shift slightly toward warmer/cooler tones
        frame = frame.astype(np.float32)
        frame[:, :, 0] *= (1.0 + (self.intensity * 0.5)) # Blue channel
        frame[:, :, 2] *= (1.0 - (self.intensity * 0.5)) # Red channel
        frame = np.clip(frame, 0, 255).astype(np.uint8)
        
        return frame

    def reconstruct_audio(self, input_audio, output_audio):
        log("🎵 Aggressive Audio Spectrum Masking...")
        y, sr = librosa.load(input_audio, sr=44100) # Full quality
        
        # Add sub-perceptual high-frequency dither to audio
        noise = np.random.normal(0, 0.0005, y.shape)
        y = y + noise
        
        # Phase Randomization
        stft = librosa.stft(y)
        magnitude, phase = librosa.magphase(stft)
        # Shift phase by a wider margin
        random_phase = np.exp(1j * (np.angle(phase) + np.random.uniform(-0.01, 0.01, phase.shape)))
        y_out = librosa.istft(magnitude * random_phase)
        
        sf.write(output_audio, y_out, sr)

def run_command(command):
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    output, error = process.communicate()
    return output.decode(), error.decode(), process.returncode

async def fast_download(client, message, file_path):
    log(f"📡 Downloading from Telegram...")
    await client.download_media(message, file_path)

def process_video_advanced(input_path):
    log(f"🛠️  Stage 1: Aggressive Manifold Reconstruction...")
    pmr = PMREngine(intensity=0.018)
    
    # Audio Path
    run_command(f"ffmpeg -i '{input_path}' -vn -acodec pcm_s16le -y temp_audio.wav")
    pmr.reconstruct_audio('temp_audio.wav', 'recon_audio.wav')

    # Video Path
    cap = cv2.VideoCapture(input_path)
    fps = cap.get(cv2.CAP_PROP_FPS)
    # Temporal Jitter: Slightly modify FPS (e.g., 23.976 -> 24.0 or similar)
    # This breaks frame-by-frame fingerprinting
    target_fps = fps * 1.001 
    
    w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    
    video_only = "recon_video_only.mp4"
    out = cv2.VideoWriter(video_only, cv2.VideoWriter_fourcc(*'mp4v'), target_fps, (w, h))
    
    log(f"🎞️ Processing {total_frames} frames at {target_fps:.2f} FPS...")
    count = 0
    while cap.isOpened():
        ret, frame = cap.read()
        if not ret: break
        
        processed = pmr.apply_transforms(frame)
        out.write(processed)
        
        count += 1
        if count % 200 == 0:
            log(f"   > Reconstruction Progress: {int((count/total_frames)*100)}%")
    
    cap.release()
    out.release()

    final_output = "reconstructed_final.mp4"
    log("📦 Stage 2: Deep Re-Encoding...")
    # Using libx264 with specific flags to strip original metadata and rewrite stream
    # -vf "noise=..." adds a tiny bit of grain to further confuse Content ID
    cmd = (
        f"ffmpeg -i {video_only} -i recon_audio.wav "
        f"-c:v libx264 -preset ultrafast -crf 22 "
        f"-vf \"noise=alls=1:allf=t\" "
        f"-c:a aac -b:a 128k -shortest -map_metadata -1 -y '{final_output}'"
    )
    run_command(cmd)
    
    for f in ['temp_audio.wav', 'recon_audio.wav', video_only]:
        if os.path.exists(f): os.remove(f)

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
                'title': f'Unique Content {int(time.time())}',
                'description': 'PMR Manifold Processed',
                'categoryId': '24'
            },
            'status': {'privacyStatus': 'private'}
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
        
        final_video = process_video_advanced(raw_file)
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
