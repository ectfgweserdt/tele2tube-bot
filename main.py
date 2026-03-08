import os
import sys
import subprocess
import requests
import json
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

TG_POST_LINK = os.environ.get("TG_POST_LINK") # Passed from workflow trigger

# --- Initialization ---
gemini_client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None

def get_youtube_service():
    """Authenticates and returns the YouTube API service."""
    creds_data = {
        "client_id": YT_CLIENT_ID,
        "client_secret": YT_CLIENT_SECRET,
        "refresh_token": YT_REFRESH_TOKEN,
        "token_uri": "https://oauth2.googleapis.com/token",
    }
    creds = Credentials.from_authorized_user_info(creds_data)
    return build('youtube', 'v3', credentials=creds)

def fetch_movie_metadata(title):
    """Fetches movie/series details from TMDB or OMDB."""
    url = f"https://api.themoviedb.org/3/search/multi?api_key={TMDB_API_KEY}&query={title}"
    response = requests.get(url).json()
    if response.get("results"):
        return response["results"][0] # Return top result
    return None

def generate_youtube_details(raw_metadata):
    """Uses Gemini to generate SEO-friendly YouTube title and description."""
    prompt = f"""
    Given the following movie/series metadata, generate a YouTube video title and a detailed description. 
    Make it professional and suitable for private archiving. Keep it concise to save tokens.
    Metadata: {json.dumps(raw_metadata)}
    Format response as JSON with keys: 'title', 'description'
    """
    try:
        if not gemini_client:
            raise ValueError("Gemini client not initialized.")
        response = gemini_client.models.generate_content(
            model='gemini-2.5-flash',
            contents=prompt
        )
        return json.loads(response.text.strip('```json').strip('```').strip())
    except Exception as e:
        print(f"Gemini generation failed: {e}")
        title = raw_metadata.get('name') or raw_metadata.get('title') if raw_metadata else "Unknown Title"
        return {"title": title, "description": str(raw_metadata)}

async def download_from_telegram(post_link):
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
        print(f"Failed to parse Telegram link: {post_link}\nError: {e}")
        return None

    print(f"Downloading message {message_id} from chat {chat_id}...")
    app = Client("my_bot", api_id=TG_API_ID, api_hash=TG_API_HASH, bot_token=TG_BOT_TOKEN, in_memory=True)

    async with app:
        try:
            message = await app.get_messages(chat_id, message_id)
            if message and (message.video or message.document):
                file_path = await message.download()
                print(f"Downloaded to {file_path}")
                return file_path
            else:
                print("Message does not contain a video or document.")
        except Exception as e:
             print(f"Failed to download from Telegram: {e}")
    return None

def process_video_and_extract_subs(video_path):
    """Strips foreign languages, keeping ONLY English audio, and extracts English subs."""
    base_name = os.path.splitext(video_path)[0]
    processed_video = f"{base_name}_english_only.mp4"
    sub_path = f"{base_name}_eng_sub.srt"

    # Extract English Subtitles
    print("Extracting English Subtitles...")
    try:
         subprocess.run([
             "ffmpeg", "-y", "-i", video_path, 
             "-map", "0:s:m:language:eng:0?", 
             "-c:s", "srt", sub_path
         ], check=True)
         if not os.path.exists(sub_path) or os.path.getsize(sub_path) == 0:
             sub_path = None
    except subprocess.CalledProcessError:
         print("No English subtitles found or extraction failed.")
         sub_path = None

    # Process Video: Keep video and English audio, drop everything else
    print("Processing video to keep ONLY English audio...")
    try:
        subprocess.run([
            "ffmpeg", "-y", "-i", video_path,
            "-map", "0:v:0",                 # Map the video stream
            "-map", "0:a:m:language:eng:0?", # Map the English audio stream
            "-c:v", "copy",                  # Copy video directly (fast, no re-encoding)
            "-c:a", "aac",                   # Convert audio to AAC (YouTube friendly)
            "-strict", "experimental",
            processed_video
        ], check=True)
        return processed_video, sub_path
    except subprocess.CalledProcessError as e:
        print(f"Video processing failed: {e}. Falling back to original video.")
        return video_path, sub_path

def upload_to_youtube(youtube, file_path, title, description, category_id="22"):
    """Uploads a video/audio file to YouTube as a private video."""
    print(f"Uploading {file_path} to YouTube...")
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
    media = MediaFileUpload(file_path, chunksize=-1, resumable=True)
    try:
        request = youtube.videos().insert(part=','.join(body.keys()), body=body, media_body=media)
        response = request.execute()
        print(f"Uploaded! Video ID: {response['id']}")
        return response['id']
    except Exception as e:
        print(f"Failed to upload {file_path} to YouTube: {e}")
        return None

def upload_caption_to_youtube(youtube, video_id, caption_path):
    """Uploads a subtitle file as a caption track to a specific YouTube video."""
    print(f"Uploading English caption to video {video_id}...")
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
        print("Caption uploaded successfully!")
    except Exception as e:
        print(f"Failed to upload caption: {e}")

async def main():
    if not TG_POST_LINK:
        print("No Telegram link provided.")
        return

    # 1. Download
    video_path = await download_from_telegram(TG_POST_LINK)
    if not video_path:
        print("Failed to download video. Exiting.")
        return

    # 2. Process Video (Keep English audio only) & Extract Subtitles
    processed_video_path, sub_path = process_video_and_extract_subs(video_path)

    # 3. Get Metadata & Generate Title/Desc
    search_query = os.path.basename(video_path).replace('.', ' ').split()[0] 
    raw_meta = fetch_movie_metadata(search_query)
    yt_details = generate_youtube_details(raw_meta) if raw_meta else {"title": search_query, "description": "Auto-uploaded content."}

    # 4. Upload to YouTube
    youtube = get_youtube_service()

    # Upload the cleaned video
    main_video_id = upload_to_youtube(youtube, processed_video_path, yt_details['title'], yt_details['description'])

    # Upload English subtitle as CC to the main video
    if main_video_id and sub_path:
        upload_caption_to_youtube(youtube, main_video_id, sub_path)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
