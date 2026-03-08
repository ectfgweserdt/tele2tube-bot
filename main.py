import os
import sys
import subprocess
import requests
import json
import re
import time
from google import genai
from pyrogram import Client
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# --- Environment Variables ---
TG_API_ID = os.environ.get("TELEGRAM_API_ID")
TG_API_HASH = os.environ.get("TELEGRAM_API_HASH")
TG_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN")
TMDB_API_KEY = os.environ.get("TMDB_API_KEY")
OMDB_API_KEY = os.environ.get("OMDB_API_KEY")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

YT_CLIENT_ID = os.environ.get("YOUTUBE_CLIENT_ID")
YT_CLIENT_SECRET = os.environ.get("YOUTUBE_CLIENT_SECRETS")
YT_REFRESH_TOKEN = os.environ.get("YOUTUBE_REFRESH_TOKEN")

TG_POST_LINKS_ENV = os.environ.get("TG_POST_LINKS") # Supports multiple links

# Global dictionary to track log throttling
last_print_time = {}

# --- Initialization ---
gemini_client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None

def get_youtube_service():
    """Authenticates and returns the YouTube API service."""
    print("[Auth] Authenticating with YouTube API...")
    creds_data = {
        "client_id": YT_CLIENT_ID,
        "client_secret": YT_CLIENT_SECRET,
        "refresh_token": YT_REFRESH_TOKEN,
        "token_uri": "https://oauth2.googleapis.com/token",
    }
    creds = Credentials.from_authorized_user_info(creds_data)
    return build('youtube', 'v3', credentials=creds, cache_discovery=False)

def extract_season_episode(filename):
    """Extracts Season and Episode numbers from the filename."""
    match = re.search(r'[sS](\d{2})[eE](\d{2})', filename)
    if match:
        return match.group(1), match.group(2)
    return None, None

def fetch_movie_metadata(title):
    """Fetches movie/series details from TMDB."""
    clean_title = re.sub(r'[sS]\d{2}[eE]\d{2}.*', '', title).replace('.', ' ').strip()
    print(f"[Metadata] Fetching TMDB details for: '{clean_title}'")
    url = f"https://api.themoviedb.org/3/search/multi?api_key={TMDB_API_KEY}&query={clean_title}"
    try:
        response = requests.get(url).json()
        if response.get("results"):
            return response["results"][0], clean_title 
    except Exception as e:
        print(f"[Metadata] Error fetching TMDB data: {e}")
    return None, clean_title

def generate_youtube_details(raw_metadata, series_name, season, episode):
    """Uses Gemini to generate fancy, emoji-free YouTube titles and descriptions."""
    print("[Gemini] Generating engaging title and description...")
    prompt = f"""
    Generate a fancy, engaging, and user-friendly YouTube video title and description based on the following metadata.

    Metadata provided: {json.dumps(raw_metadata)}
    Series Name: {series_name}
    Season: {season if season else 'Unknown'}
    Episode: {episode if episode else 'Unknown'}

    CRITICAL RULES:
    1. DO NOT use ANY emojis in the title or the description. Zero emojis.
    2. The title MUST include the series name, season, and episode. Example format: "Series Name - S01E01 - Episode Title"
    3. The description should read like a professional streaming service summary. It should be formatted cleanly with paragraphs, without robotic data dumping.

    Respond STRICTLY in JSON format with two keys: 'title' and 'description'.
    """
    try:
        if not gemini_client:
            raise ValueError("Gemini client not initialized.")
        response = gemini_client.models.generate_content(
            model='gemini-2.5-flash',
            contents=prompt
        )
        # Clean up json output
        raw_text = response.text.strip()
        if raw_text.startswith("```json"):
            raw_text = raw_text[7:-3]
        return json.loads(raw_text.strip())
    except Exception as e:
        print(f"[Gemini] Generation failed: {e}")
        fallback_title = f"{series_name} - S{season}E{episode}" if season else series_name
        return {"title": fallback_title, "description": "Auto-uploaded archive."}

async def download_progress(current, total, filename):
    """Callback to print download progress every 5 seconds to avoid log spam."""
    now = time.time()
    # Print if 5 seconds have passed, or if the download just finished
    if now - last_print_time.get(filename, 0) > 5.0 or current == total:
        percent = 100 * (current / total)
        current_mb = current / (1024 * 1024)
        total_mb = total / (1024 * 1024)
        print(f"[Telegram] Downloading... {percent:.1f}% ({current_mb:.1f} MB / {total_mb:.1f} MB)")
        last_print_time[filename] = now

async def download_from_telegram(post_link, app):
    """Downloads the video file from a given Telegram post link."""
    parts = post_link.rstrip('/').split('/')
    try:
        message_id = int(parts[-1])
        chat_id_str = parts[-2]
        if len(parts) >= 3 and parts[-3] == 'c':
            chat_id = int("-100" + chat_id_str)
        else:
            chat_id = chat_id_str 
    except (IndexError, ValueError) as e:
        print(f"[Telegram] Failed to parse link: {post_link} ({e})")
        return None

    print(f"[Telegram] Fetching message ID {message_id} from chat {chat_id}...")
    try:
        message = await app.get_messages(chat_id, message_id)
        if message and (message.video or message.document):
            filename = getattr(message.video or message.document, 'file_name', f"video_{message_id}.mp4")
            print(f"[Telegram] Found file: {filename}. Starting download...")
            file_path = await message.download(progress=download_progress, progress_args=(filename,))
            print(f"[Telegram] Download complete: {file_path}")
            return file_path
        else:
            print("[Telegram] Error: Message does not contain a video or document.")
    except Exception as e:
         print(f"[Telegram] Failed to download: {e}")
    return None

def get_best_streams(video_path):
    """Uses ffprobe to specifically locate the English audio and subtitle streams."""
    cmd = ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_streams", video_path]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        streams = json.loads(result.stdout).get('streams', [])
    except Exception as e:
        print(f"[FFmpeg] ffprobe failed to analyze streams: {e}")
        return "0:a:0", None  # Fallback to the very first audio track

    audio_stream = None
    sub_stream = None

    audio_streams = [s for s in streams if s['codec_type'] == 'audio']
    sub_streams = [s for s in streams if s['codec_type'] == 'subtitle']

    # Find English Audio
    for i, s in enumerate(audio_streams):
        lang = s.get('tags', {}).get('language', '')
        if 'eng' in lang.lower() or 'en' in lang.lower():
            audio_stream = f"0:a:{i}"
            break

    # Fallback to the first audio stream if English isn't found
    if not audio_stream and audio_streams:
        audio_stream = "0:a:0" 

    # Find English Subtitles
    for i, s in enumerate(sub_streams):
        lang = s.get('tags', {}).get('language', '')
        if 'eng' in lang.lower() or 'en' in lang.lower():
            sub_stream = f"0:s:{i}"
            break

    return audio_stream, sub_stream

def process_video_and_extract_subs(video_path):
    """Extracts the selected audio track and subtitles into clean files."""
    base_name = os.path.splitext(video_path)[0]
    processed_video = f"{base_name}_processed.mp4"
    sub_path = f"{base_name}_sub.srt"

    print("[FFmpeg] Analyzing video streams...")
    audio_map, sub_map = get_best_streams(video_path)

    # Extract English Subtitles if they exist
    if sub_map:
        print(f"[FFmpeg] Extracting English Subtitles (Stream {sub_map})...")
        try:
             subprocess.run([
                 "ffmpeg", "-y", "-i", video_path, 
                 "-map", sub_map, 
                 "-c:s", "srt", sub_path
             ], check=True, capture_output=True)
             if not os.path.exists(sub_path) or os.path.getsize(sub_path) == 0:
                 sub_path = None
             else:
                 print("[FFmpeg] Subtitles extracted successfully.")
        except subprocess.CalledProcessError:
             print("[FFmpeg] Subtitle extraction failed.")
             sub_path = None
    else:
        print("[FFmpeg] No English subtitle track found in the file.")
        sub_path = None

    # Process Video: Keep video and the selected audio
    print(f"[FFmpeg] Processing video (Keeping Audio Stream: {audio_map}). This may take a few minutes depending on file size...")
    try:
        subprocess.run([
            "ffmpeg", "-y", "-i", video_path,
            "-map", "0:v:0",                 
            "-map", audio_map, 
            "-c:v", "copy",                  
            "-c:a", "aac",                   
            "-strict", "experimental",
            processed_video
        ], check=True, capture_output=True)
        print("[FFmpeg] Video processing complete.")
        return processed_video, sub_path
    except subprocess.CalledProcessError as e:
        print(f"[FFmpeg] Video processing failed: {e}. Falling back to original video.")
        return video_path, sub_path

def get_or_create_playlist(youtube, series_title):
    """Finds an existing playlist for the series, or creates a new one."""
    print(f"[YouTube] Checking for existing playlist: '{series_title}'...")
    try:
        request = youtube.playlists().list(part="snippet", mine=True, maxResults=50)
        response = request.execute()

        # Search existing
        for item in response.get('items', []):
            if item['snippet']['title'].lower() == series_title.lower():
                print(f"[YouTube] Found existing playlist (ID: {item['id']}).")
                return item['id']

        # Create new
        print("[YouTube] Playlist not found. Creating a new one...")
        body = {
            'snippet': {
                'title': series_title, 
                'description': f'Private archive for the series {series_title}'
            },
            'status': {
                'privacyStatus': 'private'
            }
        }
        request = youtube.playlists().insert(part="snippet,status", body=body)
        response = request.execute()
        print(f"[YouTube] Created new playlist! (ID: {response['id']})")
        return response['id']
    except Exception as e:
        print(f"[YouTube] Failed to manage playlist: {e}")
        return None

def add_video_to_playlist(youtube, video_id, playlist_id):
    """Adds a video to a specific playlist."""
    print(f"[YouTube] Adding video {video_id} to playlist {playlist_id}...")
    body = {
        'snippet': {
            'playlistId': playlist_id,
            'resourceId': {
                'kind': 'youtube#video', 
                'videoId': video_id
            }
        }
    }
    try:
        youtube.playlistItems().insert(part="snippet", body=body).execute()
        print("[YouTube] Video successfully added to playlist!")
    except Exception as e:
        print(f"[YouTube] Failed to add video to playlist: {e}")

def upload_to_youtube(youtube, file_path, title, description, category_id="22"):
    """Uploads a video file to YouTube as a private video with progress tracking."""
    print(f"[YouTube] Preparing to upload video: {title}")
    body = {
        'snippet': {
            'title': title[:100], 
            'description': description,
            'categoryId': category_id
        },
        'status': {
            'privacyStatus': 'private'
        }
    }
    
    # 5MB chunk size for resumable uploads
    chunk_size = 5 * 1024 * 1024 
    media = MediaFileUpload(file_path, chunksize=chunk_size, resumable=True)
    
    try:
        request = youtube.videos().insert(part=','.join(body.keys()), body=body, media_body=media)
        response = None
        
        while response is None:
            status, response = request.next_chunk()
            if status:
                print(f"[YouTube] Uploading... {int(status.progress() * 100)}%")

        print(f"[YouTube] Upload complete! Video ID: {response['id']}")
        return response['id']
    except Exception as e:
        print(f"[YouTube] Failed to upload {file_path}: {e}")
        return None

def upload_caption_to_youtube(youtube, video_id, caption_path):
    """Uploads a subtitle file as a caption track."""
    print(f"[YouTube] Uploading English Subtitles (CC) to video {video_id}...")
    body = {
        'snippet': {
            'videoId': video_id,
            'language': 'en',
            'name': 'English',
            'isDraft': False
        }
    }
    media = MediaFileUpload(caption_path, mimetype='text/plain', chunksize=-1, resumable=True)
    try:
        request = youtube.captions().insert(part='snippet', body=body, media_body=media)
        request.execute()
        print("[YouTube] Subtitles uploaded successfully!")
    except Exception as e:
        print(f"[YouTube] Failed to upload subtitles: {e}")

async def process_single_link(link, app, youtube):
    print("\n" + "="*50)
    print(f"[SYSTEM] Starting Process for Link: {link}")
    print("="*50)

    video_path = await download_from_telegram(link, app)
    if not video_path:
        print("[SYSTEM] Skipping link due to download failure.")
        return

    # 1. Process Video (Audio isolation & Subtitles)
    processed_video_path, sub_path = process_video_and_extract_subs(video_path)

    # 2. Get Metadata
    filename = os.path.basename(video_path)
    season, episode = extract_season_episode(filename)
    raw_meta, series_name = fetch_movie_metadata(filename)

    # 3. Generate Fancy Data
    yt_details = generate_youtube_details(raw_meta, series_name, season, episode)

    # 4. Upload to YouTube
    main_video_id = upload_to_youtube(youtube, processed_video_path, yt_details['title'], yt_details['description'])

    if main_video_id:
        # 5. Upload Subtitles
        if sub_path:
            upload_caption_to_youtube(youtube, main_video_id, sub_path)

        # 6. Add to Playlist (Only if it's a series)
        if season and series_name:
            playlist_id = get_or_create_playlist(youtube, series_name)
            if playlist_id:
                add_video_to_playlist(youtube, main_video_id, playlist_id)

    # Clean up local files to save disk space on the GitHub runner
    print(f"[SYSTEM] Cleaning up temporary files for {filename}...")
    try:
        os.remove(video_path)
        if processed_video_path != video_path:
            os.remove(processed_video_path)
        if sub_path:
            os.remove(sub_path)
        print("[SYSTEM] Cleanup finished.")
    except OSError as e:
        print(f"[SYSTEM] Cleanup error: {e}")

async def main():
    if not TG_POST_LINKS_ENV:
        print("[SYSTEM] No Telegram links provided. Exiting.")
        return

    # Clean up user input (handles comma-separated and multiline inputs)
    links = [link.strip() for link in TG_POST_LINKS_ENV.replace(',', '\n').split('\n') if link.strip()]
    
    print(f"[SYSTEM] Found {len(links)} link(s) to process.")

    youtube = get_youtube_service()
    app = Client("my_bot", api_id=TG_API_ID, api_hash=TG_API_HASH, bot_token=TG_BOT_TOKEN, in_memory=True)

    async with app:
        for link in links:
            await process_single_link(link, app, youtube)

    print("\n" + "="*50)
    print("[SYSTEM] All tasks completed successfully!")
    print("="*50)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
