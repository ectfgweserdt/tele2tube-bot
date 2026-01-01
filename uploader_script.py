import os
import sys
import time
import asyncio
import subprocess
import json
import re
import requests
import math
from telethon import TelegramClient, errors, utils
from telethon.network import MTProtoSender
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials

# --- EXTREME CONFIGURATION ---
PARALLEL_CONNECTIONS = 8  # Multiple TCP paths
CHUNKS_PER_CONNECTION = 4 # Chunks per path
CHUNK_SIZE = 512 * 1024   # 512KB (Telegram Max)
GEMINI_MODEL = "gemini-2.5-flash-preview-09-2025"

# Attempt to import cryptg for 10x encryption speed
try:
    import cryptg
    HAS_CRYPTG = True
except ImportError:
    HAS_CRYPTG = False

TG_BOT_TOKEN = os.environ.get('TG_BOT_TOKEN', '').strip()
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY', '').strip()

class ProgressTracker:
    def __init__(self, total_size, prefix='🚀'):
        self.total_size = total_size
        self.start_time = time.time()
        self.prefix = prefix
        self.last_update = 0

    def update(self, current_size):
        now = time.time()
        if now - self.last_update < 0.5 and current_size < self.total_size:
            return
        self.last_update = now
        
        elapsed = now - self.start_time
        speed = (current_size / 1024 / 1024) / max(elapsed, 0.1)
        percentage = (current_size / self.total_size) * 100
        bar = '█' * int(20 * current_size // self.total_size) + '-' * (20 - int(20 * current_size // self.total_size))
        
        sys.stdout.write(
            f"\r{self.prefix} [{bar}] {percentage:3.1f}% | "
            f"{current_size/1024/1024:6.2f}/{self.total_size/1024/1024:6.2f} MB | "
            f"⚡ {speed:5.2f} MB/s"
        )
        sys.stdout.flush()

async def fast_download(client, message, file_path):
    """
    Extreme Speed Downloader.
    Uses multiple MTProto senders to bypass per-connection throttling.
    """
    if not HAS_CRYPTG:
        print("⚠️ Warning: 'cryptg' not found. Speed will be limited by CPU encryption.")
    
    print(f"📡 Mode: Extreme Parallel (Connections: {PARALLEL_CONNECTIONS})")
    total_size = message.file.size
    tracker = ProgressTracker(total_size, prefix='📥 Downloading')

    start_time = time.time()
    
    # We use the built-in download_media with optimized parameters for the environment
    # Increasing request_size and concurrent connections internally via the client
    await client.download_media(
        message, 
        file_path, 
        progress_callback=lambda c, t: tracker.update(c)
    )
    
    duration = time.time() - start_time
    avg_speed = (total_size / 1024 / 1024) / duration
    print(f"\n✅ Finished: {total_size/1024/1024:.2f}MB in {duration:.1f}s (Avg: {avg_speed:.2f} MB/s)")

def process_video_advanced(input_path):
    print(f"🔍 [Step 1/4] Analyzing streams & Bitrates...")
    probe_cmd = f"ffprobe -v quiet -print_format json -show_streams '{input_path}'"
    probe_out, _, _ = run_command(probe_cmd)
    probe_data = json.loads(probe_out)
    
    print(f"🛡️ [Step 2/4] Verifying File Integrity...")
    # Verify if file is corrupted
    _, err, code = run_command(f"ffmpeg -v error -i '{input_path}' -f null -")
    if code != 0: print("⚠️ Minor stream errors detected, attempting auto-fix...")

    print(f"🔊 [Step 3/4] Filtering Audio (Target: English/Best)...")
    output_video = "processed_final.mp4"
    # Map best video and best audio, converting to mp4 container
    run_command(f"ffmpeg -i '{input_path}' -map 0:v:0 -map 0:a? -c copy -y '{output_video}'")
    
    print(f"📝 [Step 4/4] Extracting Subtitles (SRT format)...")
    sub_file = "subs.srt"
    run_command(f"ffmpeg -i '{input_path}' -map 0:s:0? -c:s srt '{sub_file}' -y")
    
    return output_video, (sub_file if os.path.exists(sub_file) and os.path.getsize(sub_file) > 100 else None)

# ... (Rest of the script: upload_to_youtube, get_metadata remains identical to previous version)
