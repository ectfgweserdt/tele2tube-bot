import PTN
import requests
import google.generativeai as genai

def get_omdb_metadata(api_key, title, year=None, season=None, episode=None):
    """Queries the OMDB API for movie or TV show metadata."""
    base_url = "http://www.omdbapi.com/"
    params = {'apikey': api_key, 't': title, 'plot': 'full'}
    
    if year: 
        params['y'] = year
    if season and episode:
        params['Season'] = season
        params['Episode'] = episode

    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()
        return data if data.get('Response') == 'True' else None
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while querying the OMDB API: {e}")
        return None

def generate_youtube_title(metadata, parsed_info):
    """Generates a standardized YouTube title from OMDB metadata."""
    if metadata.get('Type') == 'movie':
        title, year = metadata.get('Title'), metadata.get('Year')
        return f"{title} ({year})" if title and year else None
        
    elif metadata.get('Type') == 'episode':
        series_title = parsed_info.get('title')
        # Ensure season/episode are zero-padded
        season = str(metadata.get('Season', '')).zfill(2)
        episode_num = str(metadata.get('Episode', '')).zfill(2)
        episode_title = metadata.get('Title')
        
        if all([series_title, season, episode_num, episode_title]):
            return f"{series_title} - S{season}E{episode_num} - {episode_title}"
    
    # Fallback to Parsed Info if OMDB logic fails but we have basic info
    return parsed_info.get('title', 'Unknown Video')

def generate_youtube_description_with_gemini(api_key, metadata):
    """Generates a YouTube description using the Google Gemini API."""
    genai.configure(api_key=api_key)
    # Using 'gemini-pro' as specified in the paper
    model = genai.GenerativeModel('gemini-pro')

    prompt = f"""
    You are an expert at writing engaging and SEO-friendly YouTube video descriptions, in a style similar to IMDb.
    Generate a YouTube video description using the provided metadata.

    **Metadata:**
    - Title: {metadata.get('Title')}
    - Year: {metadata.get('Year')}
    - Genre: {metadata.get('Genre')}
    - Plot: {metadata.get('Plot')}
    - Actors: {metadata.get('Actors')}
    - Director: {metadata.get('Director')}
    - IMDb Rating: {metadata.get('imdbRating')}

    **Instructions:**
    1. Start with an exciting and catchy hook that includes the title and year. Use relevant emojis (e.g., 🎬, ✨).
    2. Provide a concise and engaging summary of the plot.
    3. Create a "Key Details" section with headings and emojis:
       - 🌟 Starring: [List of main actors]
       - 🎬 Director: [Name of the director]
       - 🎭 Genre: [List of genres]
    4. Add a call to action encouraging viewers to like, comment, and subscribe.
    5. Include 3-5 relevant YouTube hashtags for SEO.
    6. The tone should be enthusiastic and informative. Format the description clearly for readability.
    """
    try:
        response = model.generate_content(prompt)
        return response.text
    except Exception as e:
        print(f"An error occurred while calling the Gemini API: {e}")
        # Fallback to basic plot
        return f"{metadata.get('Title')} - {metadata.get('Plot')}"
