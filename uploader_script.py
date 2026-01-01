import os
import sys
import time
import asyncio
import subprocess
import json
import re
import requests
from telethon import TelegramClient, errors
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials
import googleapiclient.errors

# --- CONFIGURATION ---
YOUTUBE_SCOPES = ['https://www.googleapis.com/auth/youtube.upload']
GEMINI_MODEL = "gemini-2.5-flash-preview-09-2025"

# Fetching API Keys
TG_BOT_TOKEN = os.environ.get('TG_BOT_TOKEN', '').strip()
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY', '').strip()
OMDB_API_KEY = os.environ.get('OMDB_API_KEY', '').strip()

def run_command(command):
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    output, error = process.communicate()
    return output.decode(), error.decode(), process.returncode

def download_progress_callback(current, total):
    print(f"🚀 High-Speed Download: {current/1024/1024:.2f}MB / {total/1024/1024:.2f}MB ({current*100/total:.2f}%)", end='\r', flush=True)

async def get_metadata(filename):
    # Basic filename cleaning for AI
    clean_name = os.path.splitext(filename)[0].replace('_', ' ').replace('.', ' ')
    if GEMINI_API_KEY:
        gemini_url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
        prompt = f"Create professional YouTube metadata for the video file: '{filename}'. Return JSON: title, description, tags."
        try:
            payload = {"contents": [{"parts": [{"text": prompt}]}], "generationConfig": {"responseMimeType": "application/json"}}
            res = requests.post(gemini_url, json=payload, timeout=30)
            if res.status_code == 200:
                return json.loads(res.json()['candidates'][0]['content']['parts'][0]['text'])
        except: pass
    return {"title": clean_name, "description": "High-quality backup.", "tags": "video"}

def process_video_advanced(input_path):
    """
    1. Scans for English audio.
    2. Keeps English if found, otherwise keeps the first track.
    3. Extracts subtitles.
    4. NO QUALITY LOSS (Copy codecs).
    """
    print(f"🛠️ Analyzing streams...")
    
    # Get stream info
    probe_cmd = f"ffprobe -v quiet -print_format json -show_streams '{input_path}'"
    probe_out, _, _ = run_command(probe_cmd)
    probe_data = json.loads(probe_out)
    
    audio_streams = [s for s in probe_data.get('streams', []) if s['codec_type'] == 'audio']
    eng_track = next((s['index'] for s in audio_streams if s.get('tags', {}).get('language') == 'eng'), None)
    
    # Determine audio mapping: use English if exists, else keep original track 0
    audio_map = f"0:a:{eng_track}" if eng_track is not None else "0:a:0"
    
    output_video = "processed_video.mp4"
    print(f"⚡ Processing (Stream Copy)...")
    
    # -c copy ensures NO quality loss and maximum speed
    ffmpeg_cmd = (
        f"ffmpeg -i '{input_path}' -map 0:v:0 -map {audio_map} "
        f"-c:v copy -c:a copy -y '{output_video}'"
    )
    run_command(ffmpeg_cmd)
    
    # Extract Subtitles (Optional function)
    sub_file = "subs.srt"
    sub_cmd = f"ffmpeg -i '{input_path}' -map 0:s:0? -c:s srt '{sub_file}' -y"
    run_command(sub_cmd)
    
    return output_video, (sub_file if os.path.exists(sub_file) else None)

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
        
        body = {
            'snippet': {
                'title': metadata.get('title', 'Video')[:95],
                'description': metadata.get('description', ''),
                'tags': metadata.get('tags', '').split(','),
                'categoryId': '24'
            },
            'status': {'privacyStatus': 'private'}
        }
        
        print(f"🚀 Uploading to YouTube...")
        media = MediaFileUpload(video_path, chunksize=1024*1024*5, resumable=True)
        request = youtube.videos().insert(part="snippet,status", body=body, media_body=media)
        response = None
        while response is None:
            status, response = request.next_chunk()
            if status: print(f"Uploaded {int(status.progress() * 100)}%")

        if sub_path:
            print("📜 Uploading Subtitles...")
            try:
                youtube.captions().insert(
                    part="snippet",
                    body={'snippet': {'videoId': response['id'], 'language': 'en', 'name': 'English'}},
                    media_body=MediaFileUpload(sub_path)
                ).execute()
            except: pass
            
        print(f"🎉 SUCCESS! https://youtu.be/{response['id']}")
        return True
    except Exception as e:
        print(f"🔴 YT Error: {e}")
        return False

async def process_link(client, link):
    try:
        parts = [p for p in link.strip('/').split('/') if p]
        msg_id, chat_id = int(parts[-1]), int(f"-100{parts[parts.index('c')+1]}")
        message = await client.get_messages(chat_id, ids=msg_id)
        
        raw_file = f"vid_{msg_id}.mkv"
        await client.download_media(message, raw_file, progress_callback=download_progress_callback)
        
        metadata = await get_metadata(message.file.name or raw_file)
        final_video, sub_file = process_video_advanced(raw_file)
        
        upload_to_youtube(final_video, metadata, sub_file)

        for f in [raw_file, final_video, sub_file]:
            if f and os.path.exists(f): os.remove(f)
    except Exception as e:
        print(f"Error processing {link}: {e}")

async def main():
    if len(sys.argv) < 2: return
    links = sys.argv[1].split(',')
    
    # Initialize Bot Client for Uncapped Speed
    client = TelegramClient('bot_session', os.environ['TG_API_ID'], os.environ['TG_API_HASH'])
    await client.start(bot_token=TG_BOT_TOKEN)
    
    for link in links:
        await process_link(client, link)
    await client.disconnect()

if __name__ == '__main__':
    asyncio.run(main())
