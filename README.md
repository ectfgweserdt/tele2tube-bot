Telegram to YouTube Automation PipelineThis project automates the process of downloading video content from a private or public Telegram channel, processing it for YouTube compliance, generating AI-powered metadata, and uploading it directly to YouTube.System OverviewMonitor & Download: Telethon connects to Telegram to fetch the latest video.Process: FFmpeg ensures the video is H.264/AAC and extracts embedded English subtitles to .srt.Metadata: parse-torrent-name cleans the filename, OMDB API fetches details, and Google Gemini API writes the description.Upload: YouTube Data API v3 handles the video and subtitle upload.PrerequisitesPython 3.10+FFmpeg installed on the system.API Credentials (See below).Setup & Configuration1. API Keys RequiredYou need to obtain the following keys:Telegram: API_ID and API_HASH from my.telegram.org.OMDB: API Key from omdbapi.com.Google Gemini: API Key from Google AI Studio.YouTube: OAuth 2.0 Client Credentials (client_secrets.json) from Google Cloud Console.2. Generating the YouTube TokenSince YouTube OAuth requires a browser login, you must run a script locally once to generate the refresh token for the server.Download client_secrets.json to the project root.Run a simple auth script (using google-auth-oauthlib) to login.This creates a token.json file.Copy the content of token.json for the GitHub Secret.3. GitHub SecretsGo to your Repository -> Settings -> Secrets and variables -> Actions and add:TELEGRAM_API_IDTELEGRAM_API_HASHOMDB_API_KEYGEMINI_API_KEYYOUTUBE_CLIENT_SECRETS: (Paste content of client_secrets.json)YOUTUBE_TOKEN: (Paste content of token.json)UsageRunning via GitHub ActionsGo to the Actions tab in your repository.Select Manual Video Processing Pipeline.Click Run workflow.Enter the Channel Link (e.g., https://t.me/mychannel or an invite link).The pipeline will spin up an Ubuntu runner, process the video, and upload it.Running Locally# Install dependencies
pip install -r requirements.txt

# Set environment variables (Linux/Mac)
export TELEGRAM_API_ID="your_id"
export TELEGRAM_API_HASH="your_hash"
export OMDB_API_KEY="your_key"
export GEMINI_API_KEY="your_key"
export CHANNEL_LINK="[https://t.me/](https://t.me/)..."
# ... ensure YOUTUBE variables are set or logic is adjusted to read files

# Run
python main.py
Structuresrc/: Contains all logic modules..github/workflows: CI/CD configuration.downloads/: Temporary folder for raw downloads.processed/: Temporary folder for transcoded video and subtitles.
