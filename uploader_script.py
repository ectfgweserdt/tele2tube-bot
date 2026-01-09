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

# --- OPTIMIZED CONFIGURATION ---
YOUTUBE_SCOPES = ['https://www.googleapis.com/auth/youtube.force-ssl']

# Fetching API Keys
TG_BOT_TOKEN = os.environ.get('TG_BOT_TOKEN', '').strip()

def get_file_size_formatted(file_path):
    if not os.path.exists(file_path): return "0 Bytes"
    size_bytes = os.path.getsize(file_path)
    if size_bytes == 0: return "0 Bytes"
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return f"{s} {('Bytes', 'KB', 'MB', 'GB', 'TB')[i]}"

class PMREngine:
    def __init__(self, intensity=0.005):
        self.intensity = intensity

    def apply_transforms(self, frame):
        # Reduced complexity for GitHub Actions CPU
        h, w = frame.shape[:2]
        x, y = np.meshgrid(np.arange(w), np.arange(h))
        dx = np.sin(x / 40.0) * self.intensity * 5
        dy = np.cos(y / 40.0) * self.intensity * 5
        map_x = (x + dx).astype(np.float32)
        map_y = (y + dy).astype(np.float32)
        # Using INTER_NEAREST for speed on low-resource runners
        frame = cv2.remap(frame, map_x, map_y, interpolation=cv2.INTER_NEAREST)
        return frame

    def reconstruct_audio(self, input_audio, output_audio):
        print("🎵 Reconstructing Audio Spectrum...")
        y, sr = librosa.load(input_audio, sr=22050) # Downsample slightly for speed
        stft = librosa.stft(y)
        magnitude, phase = librosa.magphase(stft)
        random_phase = np.exp(1j * (np.angle(phase) + np.random.uniform(-0.002, 0.002, phase.shape)))
        y_out = librosa.istft(magnitude * random_phase)
        sf.write(output_audio, y_out, sr)

def run_command(command):
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    output, error = process.communicate()
    return output.decode(), error.decode(), process.returncode

class ProgressTracker:
    def __init__(self, total_size):
        self.total_size = total_size
        self.start_time = time.time()
        self.last_ui_update = 0

    def update(self, current_size):
        now = time.time()
        if now - self.last_ui_update < 2.0: return
        self.last_ui_update = now
        percentage = (current_size / self.total_size) * 100
        print(f"🚀 Download: {percentage:.1f}%")

async def fast_download(client, message, file_path):
    print(f"📡 Downloading...")
    await client.download_media(message, file_path, progress_callback=lambda c, t: ProgressTracker(t).update(c))

def process_video_advanced(input_path):
    print(f"🛠️  Stage 1: PMR Manifold Reconstruction...")
    pmr = PMREngine()
    
    # Audio Step
    run_command(f"ffmpeg -i '{input_path}' -vn -acodec pcm_s16le -ar 22050 -y temp_audio.wav")
    pmr.reconstruct_audio('temp_audio.wav', 'recon_audio.wav')

    # Video Step
    cap = cv2.VideoCapture(input_path)
    fps = cap.get(cv2.CAP_PROP_FPS)
    w, h = int(cap.get(cv2.CAP_PROP_WIDTH)), int(cap.get(cv2.CAP_PROP_HEIGHT))
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    
    video_only = "recon_video_only.mp4"
    # Using 'mp4v' for speed, merging later with x264
    out = cv2.VideoWriter(video_only, cv2.VideoWriter_fourcc(*'mp4v'), fps, (w, h))
    
    print(f"🎞️ Processing {total_frames} frames...")
    count = 0
    while cap.isOpened():
        ret, frame = cap.read()
        if not ret: break
        out.write(pmr.apply_transforms(frame))
        count += 1
        if count % 100 == 0:
            print(f"   > Processed {count}/{total_frames} frames...")
    
    cap.release()
    out.release()

    final_output = "reconstructed_final.mp4"
    print("📦 Finalizing Encodes...")
    # Use 'ultrafast' preset to prevent timeout on 40MB+ files
    run_command(f"ffmpeg -i {video_only} -i recon_audio.wav -c:v libx264 -preset ultrafast -crf 23 -c:a aac -shortest -y '{final_output}'")
    
    for f in ['temp_audio.wav', 'recon_audio.wav', video_only]:
        if os.path.exists(f): os.remove(f)

    return final_output

def upload_to_youtube(video_path):
    try:
        print(f"📤 Uploading {get_file_size_formatted(video_path)} to YouTube...")
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
            'snippet': {'title': 'Reconstructed Video', 'categoryId': '24'},
            'status': {'privacyStatus': 'private'}
        }
        
        media = MediaFileUpload(video_path, chunksize=1024*1024*5, resumable=True)
        request = youtube.videos().insert(part="snippet,status", body=body, media_body=media)
        
        response = None
        while response is None:
            status, response = request.next_chunk()
            if status: print(f"   > Uploading: {int(status.progress() * 100)}%")

        print(f"✅ SUCCESS: https://youtu.be/{response['id']}")
    except Exception as e:
        print(f"🔴 YouTube Error: {e}")

async def process_link(client, link):
    try:
        parts = [p for p in link.strip('/').split('/') if p]
        msg_id, chat_id = int(parts[-1]), int(f"-100{parts[parts.index('c')+1]}")
        message = await client.get_messages(chat_id, ids=msg_id)
        
        raw_file = f"temp_{msg_id}.mkv"
        await fast_download(client, message, raw_file)
        
        final_video = process_video_advanced(raw_file)
        upload_to_youtube(final_video)

        if os.path.exists(raw_file): os.remove(raw_file)
        if os.path.exists(final_video): os.remove(final_video)
    except Exception as e:
        print(f"❌ Error: {e}")

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
