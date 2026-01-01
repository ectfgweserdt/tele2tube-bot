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
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials
import googleapiclient.errors

# --- CONFIGURATION ---
YOUTUBE_SCOPES = ['https://www.googleapis.com/auth/youtube.force-ssl']
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
    print(f"🚀 Parallel Download: {current/1024/1024:.2f}MB / {total/1024/1024:.2f}MB ({current*100/total:.2f}%)", end='\r', flush=True)

async def fast_download(client, message, file_path):
    start_time = time.time()
    await client.download_media(message, file_path, progress_callback=download_progress_callback)
    duration = time.time() - start_time
    size_mb = os.path.getsize(file_path) / (1024 * 1024)
    print(f"\n✅ Download Complete: {size_mb:.2f} MB in {duration:.2f}s ({size_mb/max(duration, 1):.2f} MB/s)")

def parse_filename(filename):
    clean_name = os.path.splitext(filename)[0].replace('_', ' ').replace('.', ' ')
    match = re.search(r'S(\d+)E(\d+)', clean_name, re.IGNORECASE)
    season, episode = None, None
    if match:
        season, episode = match.group(1), match.group(2)
        search_title = clean_name[:match.start()].strip()
    else:
        tags = [r'\d{3,4}p', 'HD', 'NF', 'WEB-DL', 'Dual Audio', 'x264', 'x265', 'HEVC']
        search_title = clean_name
        for tag in tags: search_title = re.sub(tag, '', search_title, flags=re.IGNORECASE)
        search_title = ' '.join(search_title.split()).strip()
    return search_title, season, episode

async def get_metadata(filename):
    search_title, season, episode = parse_filename(filename)
    omdb_data = None
    if OMDB_API_KEY:
        try:
            url = f"http://www.omdbapi.com/?t={search_title}&apikey={OMDB_API_KEY}"
            if season: url += f"&Season={season}&Episode={episode}"
            res = requests.get(url, timeout=10)
            data = res.json()
            if data.get("Response") == "True": omdb_data = data
        except: pass

    if GEMINI_API_KEY:
        gemini_url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
        prompt = (
            f"Context: Filename '{filename}'. IMDb Data: {json.dumps(omdb_data) if omdb_data else 'None'}.\n"
            "Task: Generate YouTube metadata in JSON format. Return EXACTLY this format:\n"
            "TITLE: Alice in Borderland - S03E06 - The Game Master's Trap\n"
            "DESCRIPTION:\n🃏 Synopsis:\n[Detailed Plot Summary]\n\n👥 Cast:\n[List of Main Actors]\n\n🔍 Details:\nGenre: ... | Network: ... | Origin: ...\n\n"
            "Return JSON keys: 'title', 'description', 'tags' (string)."
        )
        try:
            payload = {"contents": [{"parts": [{"text": prompt}]}], "generationConfig": {"responseMimeType": "application/json"}}
            res = requests.post(gemini_url, json=payload, timeout=30)
            if res.status_code == 200:
                return json.loads(res.json()['candidates'][0]['content']['parts'][0]['text'])
        except: pass
    return {"title": search_title, "description": "High-speed upload.", "tags": "video"}

def process_video_advanced(input_path):
    print(f"🛠️ Analyzing streams and extracting subtitles...")
    probe_cmd = f"ffprobe -v quiet -print_format json -show_streams '{input_path}'"
    probe_out, _, _ = run_command(probe_cmd)
    probe_data = json.loads(probe_out)
    
    streams = probe_data.get('streams', [])
    audio_streams = [s for s in streams if s['codec_type'] == 'audio']
    eng_track = next((i for i, s in enumerate(audio_streams) if s.get('tags', {}).get('language') in ['eng', 'en']), None)
    
    audio_map = f"0:a:{eng_track}" if eng_track is not None else "0:a:0"
    output_video = "processed_video.mp4"
    
    # Process Video/Audio
    run_command(f"ffmpeg -i '{input_path}' -map 0:v:0 -map {audio_map} -c:v copy -c:a copy -y '{output_video}'")
    
    # Extract Subs (Try to force SRT format from first subtitle stream)
    sub_file = "subs.srt"
    run_command(f"ffmpeg -i '{input_path}' -map 0:s:0 -c:s srt '{sub_file}' -y")
    
    # Verify if sub_file is valid and not empty
    has_subs = os.path.exists(sub_file) and os.path.getsize(sub_file) > 100
    if has_subs:
        # Final check: does it look like an SRT?
        with open(sub_file, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read(100)
            if '-->' not in content and '1' not in content:
                has_subs = False

    return output_video, (sub_file if has_subs else None)

def generate_thumbnail(video_path):
    output_thumb = "thumbnail.jpg"
    run_command(f"ffmpeg -ss 00:00:25 -i '{video_path}' -vframes 1 -q:v 2 -y {output_thumb}")
    return output_thumb if os.path.exists(output_thumb) else None

def upload_to_youtube(video_path, metadata, sub_path, thumb_path):
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
        
        tags = metadata.get('tags', 'video')
        if isinstance(tags, str): tags = tags.split(',')

        body = {
            'snippet': {
                'title': metadata.get('title', 'Video')[:95],
                'description': metadata.get('description', ''),
                'tags': tags,
                'categoryId': '24'
            },
            'status': {'privacyStatus': 'private'}
        }
        
        print(f"🚀 Uploading Video...")
        media = MediaFileUpload(video_path, chunksize=1024*1024*10, resumable=True)
        request = youtube.videos().insert(part="snippet,status", body=body, media_body=media)
        response = None
        while response is None:
            status, response = request.next_chunk()
            if status: print(f"Uploaded {int(status.progress() * 100)}%")

        video_id = response['id']

        if thumb_path:
            try: youtube.thumbnails().set(videoId=video_id, media_body=MediaFileUpload(thumb_path)).execute()
            except: pass

        if sub_path:
            print("📜 Uploading Subtitles (Forcing Publication)...")
            try:
                # We add sync=True to tell YouTube to process it immediately if possible
                youtube.captions().insert(
                    part="snippet",
                    body={
                        'snippet': {
                            'videoId': video_id, 
                            'language': 'en', 
                            'name': 'English',
                            'isDraft': False # Ensure it's not a draft
                        }
                    },
                    media_body=MediaFileUpload(sub_path),
                    sync=True 
                ).execute()
            except Exception as e:
                print(f"⚠️ Subtitle upload failed: {e}")
            
        print(f"🎉 SUCCESS: https://youtu.be/{video_id}")
    except Exception as e:
        print(f"🔴 YouTube Error: {e}")

async def process_link(client, link):
    try:
        parts = [p for p in link.strip('/').split('/') if p]
        msg_id, chat_id = int(parts[-1]), int(f"-100{parts[parts.index('c')+1]}")
        message = await client.get_messages(chat_id, ids=msg_id)
        
        raw_file = f"temp_{msg_id}.mkv"
        await fast_download(client, message, raw_file)
        
        metadata = await get_metadata(message.file.name or raw_file)
        final_video, sub_file = process_video_advanced(raw_file)
        thumb_file = generate_thumbnail(final_video)
        
        upload_to_youtube(final_video, metadata, sub_file, thumb_file)

        for f in [raw_file, final_video, sub_file, thumb_file]:
            if f and os.path.exists(f): os.remove(f)
    except Exception as e:
        print(f"Error: {e}")

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
