import os
import sys

# Load environment variables
API_ID = os.getenv('TELEGRAM_API_ID')
API_HASH = os.getenv('TELEGRAM_API_HASH')
BOT_TOKEN = os.getenv('TG_BOT_TOKEN') # Added for Bot Authentication
OMDB_API_KEY = os.getenv('OMDB_API_KEY')
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
CHANNEL_ENTITY = os.getenv('CHANNEL_LINK')
YOUTUBE_CLIENT_SECRETS = os.getenv('YOUTUBE_CLIENT_SECRETS')

# Consolidate token naming
YOUTUBE_TOKEN = os.getenv('YOUTUBE_REFRESH_TOKEN') or os.getenv('YOUTUBE_TOKEN')

# Local paths
DOWNLOAD_PATH = 'downloads'
PROCESSED_PATH = 'processed'
SESSION_NAME = 'bot_session' # Changed to avoid conflict with user sessions

def validate_config():
    """Ensures all necessary environment variables are present."""
    required_vars = [
        'TELEGRAM_API_ID', 'TELEGRAM_API_HASH', 'TG_BOT_TOKEN',
        'OMDB_API_KEY', 'GEMINI_API_KEY', 
        'YOUTUBE_CLIENT_SECRETS', 'CHANNEL_LINK'
    ]
    
    missing = [var for var in required_vars if not os.getenv(var)]
    if not YOUTUBE_TOKEN:
        missing.append('YOUTUBE_REFRESH_TOKEN')

    if missing:
        print(f"Error: Missing environment variables: {', '.join(missing)}")
        sys.exit(1)

    os.makedirs(DOWNLOAD_PATH, exist_ok=True)
    os.makedirs(PROCESSED_PATH, exist_ok=True)
