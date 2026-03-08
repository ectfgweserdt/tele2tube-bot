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
app = Client("my_bot", api_id=TG_API_ID, api_hash=TG_API_HASH, bot_token=TG_BOT_TOKEN)
gemini_client = genai.Client(api_key=GEMINI_API_KEY)

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
        response = gemini_client.models.generate_content(
            model='gemini-2.5-flash',
            contents=prompt
        )
        # Assuming Gemini returns clean JSON
        return json.loads(response.text.strip('```json').strip('```').strip())
    except Exception as e:
        print(f"Gemini generation failed: {e}")
        title = raw_metadata.get('name') if raw_metadata else "Unknown Title"
        title = title or raw_metadata.get('title') if raw_metadata else "Unknown Title"
        return {"title": title, "description": str(raw_metadata)}

async def download_from_telegram(post_link):
    """Downloads the video file from a given Telegram post link."""
    # Robust link parsing for formats like: https://t.me/c/123456789/123 or https://t.me/username/123
    parts = post_link.rstrip('/').split('/')
    
    try:
        message_id = int(parts[-1])
        chat_id_str = parts[-2]
        
        # Check if it is a private channel link (contains '/c/')
        if len(parts) >= 3 and parts[-3] == 'c':
            chat_id = int("-100" + chat_id_str)
        else:
            # Public channel
            chat_id = chat_id_str 
    except (IndexError, ValueError) as e:
        print(f"Failed to parse Telegram link: {post_link}\nError: {e}")
        return None

    print(f"Downloading message {message_id} from chat {chat_id}...")
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

def extract_streams(video_path):
    """Uses FFmpeg to extract audio and subtitles from the video file."""
    base_name = os.path.splitext(video_path)[0]
    audio_path = f"{base_name}_audio.aac"
    sub_path = f"{base_name}_sub.srt"

    # Extract Audio
    print("Extracting Audio...")
    try:
        subprocess.run(["ffmpeg", "-y", "-i", video_path, "-vn", "-acodec", "copy", audio_path], check=True)
    except subprocess.CalledProcessError:
        print("Audio extraction failed. Proceeding without separate audio.")
        audio_path = None

    # Extract Subtitles (Assuming the video has an embedded subtitle stream)
    print("Extracting Subtitles...")
    try:
         subprocess.run(["ffmpeg", "-y", "-i", video_path, "-map", "0:s:0", sub_path], check=True)
    except subprocess.CalledProcessError:
         print("No subtitles found or extraction failed.")
         sub_path = None

    return audio_path, sub_path

def upload_to_youtube(youtube, file_path, title, description, category_id="22"):
    """Uploads a video/audio file to YouTube as a private video."""
    print(f"Uploading {file_path} to YouTube...")
    body = {
        'snippet': {
            'title': title,
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
    except Exception as e:
        print(f"Failed to upload {file_path} to YouTube: {e}")

async def main():
    if not TG_POST_LINK:
        print("No Telegram link provided.")
        return

    # 1. Download
    video_path = await download_from_telegram(TG_POST_LINK)
    if not video_path:
        print("Failed to download video. Exiting.")
        return

    # 2. Extract Streams
    audio_path, sub_path = extract_streams(video_path)

    # 3. Get Metadata & Generate Title/Desc via Gemini
    search_query = os.path.basename(video_path).replace('.', ' ').split()[0] 
    raw_meta = fetch_movie_metadata(search_query)
    yt_details = generate_youtube_details(raw_meta) if raw_meta else {"title": search_query, "description": "Auto-uploaded content."}

    # 4. Upload to YouTube
    youtube = get_youtube_service()

    # Upload main video
    upload_to_youtube(youtube, video_path, yt_details['title'] + " (Video)", yt_details['description'])

    # Upload extracted audio 
    if audio_path:
        upload_to_youtube(youtube, audio_path, yt_details['title'] + " (Audio Track)", "Extracted Audio Track\n\n" + yt_details['description'])

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
