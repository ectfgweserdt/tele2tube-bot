import json
import os
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError

def get_authenticated_service_non_interactive():
    """
    Authenticates with YouTube API using environment variables (Secrets)
    to allow non-interactive login in CI/CD.
    """
    client_secrets_str = os.getenv('YOUTUBE_CLIENT_SECRETS')
    token_str = os.getenv('YOUTUBE_TOKEN')
    
    if not client_secrets_str or not token_str:
        print("YouTube credentials not found in environment variables.")
        return None
    
    # Load secrets from JSON string
    client_config = json.loads(client_secrets_str)
    creds_data = json.loads(token_str)
    
    scopes = ["https://www.googleapis.com/auth/youtube.force-ssl"]
    
    # Reconstruct Credentials object
    credentials = Credentials(
        token=creds_data['token'],
        refresh_token=creds_data['refresh_token'],
        token_uri=creds_data['token_uri'],
        client_id=creds_data['client_id'],
        client_secret=creds_data['client_secret'],
        scopes=scopes
    )
    return build("youtube", "v3", credentials=credentials)

def upload_video_to_youtube(youtube_service, video_path, title, description, tags):
    """Uploads a video file to YouTube and returns the video ID."""
    print(f"Uploading video '{title}' to YouTube...")
    
    body = {
        "snippet": {
            "title": title, 
            "description": description, 
            "tags": tags, 
            "categoryId": "22" # Category 22 is 'People & Blogs' (standard default)
        },
        "status": {
            "privacyStatus": "private" # Always upload as private initially for safety
        }
    }
    
    media = MediaFileUpload(video_path, chunksize=-1, resumable=True)
    request = youtube_service.videos().insert(
        part=",".join(body.keys()), 
        body=body, 
        media_body=media
    )
    
    response = None
    while response is None:
        status, response = request.next_chunk()
        if status:
            print(f"Uploaded {int(status.progress() * 100)}%")
            
    print(f"Upload complete! Video ID: {response['id']}")
    return response['id']

def upload_subtitle_to_youtube(youtube_service, video_id, srt_file_path):
    """Uploads an SRT subtitle file to a specific YouTube video."""
    print(f"Uploading subtitles for video ID: {video_id}")
    try:
        body = {
            'snippet': {
                'videoId': video_id,
                'language': 'en', 
                'name': "English" 
            }
        }
        
        media_body = MediaFileUpload(srt_file_path, mimetype='application/octet-stream', resumable=True)
        
        request = youtube_service.captions().insert(
            part='snippet', 
            body=body,
            media_body=media_body
        )
        response = request.execute()
        print(f"Successfully uploaded caption track with ID: {response['id']}")
    except HttpError as e:
        print(f"An HTTP error occurred during subtitle upload: {e.content}")
    except FileNotFoundError:
        print(f"SRT file not found: {srt_file_path}")
