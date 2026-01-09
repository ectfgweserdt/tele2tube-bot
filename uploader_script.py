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
GEMINI_MODEL = "gemini-2.5-flash-preview-09-2025"

# Fetching API Keys
TG_BOT_TOKEN = os.environ.get('TG_BOT_TOKEN', '').strip()
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY', '').strip()
OMDB_API_KEY = os.environ.get('OMDB_API_KEY', '').strip()

def get_file_size_formatted(file_path):
    """Returns the file size in a human-readable string."""
    if not os.path.exists(file_path):
        return "0 Bytes"
    size_bytes = os.path.getsize(file_path)
    if size_bytes == 0:
        return "0 Bytes"
    size_name = ("Bytes", "KB", "MB", "GB", "TB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return f"{s} {size_name[i]}"

# --- RECONSTRUCTION ENGINE (PMR) ---
class PMREngine:
    def __init__(self, intensity=0.01):
        self.intensity = intensity

    def apply_transforms(self, frame):
        h, w = frame.shape[:2]
        x, y = np.meshgrid(np.arange(w), np.arange(h))
        # Diffeomorphic Warp
        dx = np.sin(x / 30.0) * self.intensity * 8
        dy = np.cos(y / 30.0) * self.intensity * 8
        map_x = (x + dx).astype(np.float32)
        map_y = (y + dy).astype(np.float32)
        frame = cv2.remap(frame, map_x, map_y, interpolation=cv2.INTER_LINEAR)
        # Stochastic Dither
        noise = np.random.normal(0, self.intensity * 3, frame.shape).astype(np.uint8)
        return cv2.add(frame, noise)

    def reconstruct_audio(self, input_audio, output_audio):
        y, sr = librosa.load(input_audio)
        stft = librosa.stft(y)
        magnitude, phase = librosa.magphase(stft)
        # Randomize phase sub-perceptually
        random_phase = np.exp(1j * (np.angle(phase) + np.random.uniform(-0.005, 0.005, phase.shape)))
        y_out = librosa.istft(magnitude * random_phase)
        sf.write(output_audio, y_out, sr)

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
        if now - self.last_ui_update < 1.0 and current_size < self.total_size:
            return
        self.last_ui_update = now
        elapsed = now - self.start_time
        speed = (current_size / 1024 / 1024) / max(elapsed, 0.1)
        percentage = (current_size / self.total_size) * 100
        sys.stdout.write(f"\r{self.prefix} {percentage:3.1f}% | Speed: {speed:5.2f} MB/s")
        sys.stdout.flush()

async def fast_download(client, message, file_path):
    print(f"📡 Downloading from Telegram...")
    await client.download_media(message, file_path, progress_callback=lambda c, t: ProgressTracker(t).update(c))
    print("\n✅ Download Complete.")

def process_video_advanced(input_path):
    print(f"🛠️  Stage 1: Manifold Reconstruction (Visual/Audio Re-rendering)...")
    pmr = PMREngine(intensity=0.008)
    
    # Extract original Audio
    run_command(f"ffmpeg -i '{input_path}' -vn -acodec pcm_s16le -y temp_audio.wav")
    pmr.reconstruct_audio('temp_audio.wav', 'recon_audio.wav')

    # Process Video Frames
    cap = cv2.VideoCapture(input_path)
    fps = cap.get(cv2.CAP_PROP_FPS)
    w, h = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH)), int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    
    video_only = "recon_video_only.mp4"
    out = cv2.VideoWriter(video_only, cv2.VideoWriter_fourcc(*'mp4v'), fps, (w, h))
    
    while cap.isOpened():
        ret, frame = cap.read()
        if not ret: break
        out.write(pmr.apply_transforms(frame))
    
    cap.release()
    out.release()

    # Merge Reconstructed Video & Audio
    final_output = "reconstructed_final.mp4"
    run_command(f"ffmpeg -i {video_only} -i recon_audio.wav -c:v libx264 -crf 18 -c:a aac -b:a 192k -shortest -y '{final_output}'")
    
    # Subtitles
    sub_file = "subs.srt"
    run_command(f"ffmpeg -i '{input_path}' -map 0:s:0 -c:s srt '{sub_file}' -y")
    has_subs = os.path.exists(sub_file) and os.path.getsize(sub_file) > 100

    # Cleanup intermediate files
    for f in ['temp_audio.wav', 'recon_audio.wav', video_only]:
        if os.path.exists(f): os.remove(f)

    return final_output, (sub_file if has_subs else None)

async def get_metadata(filename):
    print(f"🤖 Generating AI Metadata...")
    return {"title": filename, "description": "Reconstructed Content", "tags": "cinematic,hq"}

def upload_to_youtube(video_path, metadata, sub_path):
    try:
        final_size = get_file_size_formatted(video_path)
        print(f"\n📦 Final Video Ready for Upload: {final_size}")

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
                'title': metadata.get('title', 'Video')[:95],
                'description': metadata.get('description', ''),
                'tags': ["reconstructed", "high_quality"],
                'categoryId': '24'
            },
            'status': {'privacyStatus': 'private'}
        }
        
        media = MediaFileUpload(video_path, chunksize=1024*1024*10, resumable=True)
        request = youtube.videos().insert(part="snippet,status", body=body, media_body=media)
        
        response = None
        while response is None:
            status, response = request.next_chunk()
            if status: print(f"\r📤 Uploading: {int(status.progress() * 100)}%", end="")

        print(f"\n✅ SUCCESS: https://youtu.be/{response['id']}")
    except Exception as e:
        print(f"\n🔴 YouTube Error: {e}")

async def process_link(client, link):
    try:
        parts = [p for p in link.strip('/').split('/') if p]
        msg_id, chat_id = int(parts[-1]), int(f"-100{parts[parts.index('c')+1]}")
        message = await client.get_messages(chat_id, ids=msg_id)
        
        raw_file = f"temp_{msg_id}.mkv"
        await fast_download(client, message, raw_file)
        
        metadata = await get_metadata(message.file.name or raw_file)
        final_video, sub_file = process_video_advanced(raw_file)
        
        upload_to_youtube(final_video, metadata, sub_file)

        for f in [raw_file, final_video, sub_file]:
            if f and os.path.exists(f): os.remove(f)
    except Exception as e:
        print(f"\n❌ Error: {e}")

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
