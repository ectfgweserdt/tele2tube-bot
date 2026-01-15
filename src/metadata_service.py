import PTN
import requests
from google import genai # Modern library to fix deprecation warning

def get_omdb_metadata(api_key, title, year=None, season=None, episode=None):
    """Queries the OMDB API for movie or TV show metadata."""
    base_url = "http://www.omdbapi.com/"
    params = {'apikey': api_key, 't': title, 'plot': 'full'}
    
    if year: params['y'] = year
    if season and episode:
        params['Season'] = season
        params['Episode'] = episode

    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()
        return data if data.get('Response') == 'True' else None
    except Exception as e:
        print(f"OMDB Error: {e}")
        return None

def generate_youtube_title(metadata, parsed_info):
    """Generates a standardized YouTube title."""
    if metadata.get('Type') == 'movie':
        return f"{metadata.get('Title')} ({metadata.get('Year')})"
    elif metadata.get('Type') == 'episode':
        return f"{parsed_info.get('title')} S{metadata.get('Season')}E{metadata.get('Episode')}"
    return parsed_info.get('title', 'Video Upload')

def generate_youtube_description_with_gemini(api_key, metadata):
    """Generates description using the new google-genai client."""
    try:
        # Initializing the modern client
        client = genai.Client(api_key=api_key)
        
        prompt = f"Write a catchy YouTube description for the movie/show: {metadata.get('Title')}. Plot: {metadata.get('Plot')}"
        
        response = client.models.generate_content(
            model='gemini-2.0-flash-exp', # Using the latest flash model
            contents=prompt
        )
        return response.text
    except Exception as e:
        print(f"Gemini Error: {e}")
        return metadata.get('Plot', 'No description available.')
