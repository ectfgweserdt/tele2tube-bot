import asyncio
import os
import sys
from telethon import TelegramClient
import PTN

# Import modules
import src.config as config
from src.telegram_service import download_latest_video
from src.media_service import (
    inspect_and_select_streams, 
    transcode_for_youtube, 
    inspect_and_extract_subtitles
)
from src.metadata_service import (
    get_omdb_metadata, 
    generate_youtube_title, 
    generate_youtube_description_with_gemini
)
from src.youtube_service import (
    get_authenticated_service_non_interactive, 
    upload_video_to_youtube, 
    upload_subtitle_to_youtube
)

async def main_workflow():
    """Main async function to run the complete automation pipeline."""
    
    # 0. Validation
    config.validate_config()
    print("Starting Telegram-to-YouTube automation workflow...")

    # 1. Download from Telegram
    async with TelegramClient(config.SESSION_NAME, config.API_ID, config.API_HASH) as client:
        downloaded_file = await download_latest_video(client, config.CHANNEL_ENTITY, config.DOWNLOAD_PATH)
        
        if not downloaded_file:
            print("Workflow finished: No new video to process.")
            return

    # 2. Inspect & Process Media
    video_index, audio_index = inspect_and_select_streams(downloaded_file)
    
    if video_index is None or audio_index is None:
        print("Could not identify valid streams.")
        return

    filename = os.path.basename(downloaded_file)
    processed_file_path = os.path.join(config.PROCESSED_PATH, f"processed_{filename}")
    
    # Transcode
    final_video_path = transcode_for_youtube(downloaded_file, processed_file_path, video_index, audio_index)
    if not final_video_path:
        return

    # Extract Subtitles (if any)
    srt_output_path = os.path.join(config.PROCESSED_PATH, f"{os.path.splitext(filename)[0]}.srt")
    extracted_srt_path = inspect_and_extract_subtitles(downloaded_file, srt_output_path)

    # 3. Enrich Metadata
    parsed_info = PTN.parse(filename)
    title_to_search = parsed_info.get('title')
    
    if not title_to_search:
        print("Could not parse a title from the filename. Using raw filename.")
        title_to_search = os.path.splitext(filename)[0]
    
    print(f"Querying metadata for: {title_to_search}")
    metadata = get_omdb_metadata(
        config.OMDB_API_KEY, 
        title_to_search, 
        parsed_info.get('year'), 
        parsed_info.get('season'), 
        parsed_info.get('episode')
    )
    
    # Handle missing metadata gracefully
    if not metadata:
        print("Warning: Failed to retrieve metadata from OMDB. Using basic info.")
        metadata = {
            "Title": title_to_search,
            "Year": parsed_info.get('year', ''),
            "Type": "movie", # Assumption
            "Plot": "Uploaded via automation pipeline."
        }
        
    video_title = generate_youtube_title(metadata, parsed_info)
    if not video_title:
        video_title = title_to_search

    # 4. Generate Description
    print("Generating YouTube description with Gemini Pro...")
    video_description = generate_youtube_description_with_gemini(config.GEMINI_API_KEY, metadata)

    # 5. Upload Video
    youtube_service = get_authenticated_service_non_interactive()
    if not youtube_service:
        print("Could not authenticate YouTube service.")
        return
    
    video_tags = ["telegram", "automation", "python", "api"]
    if metadata.get('Type') == 'movie': 
        video_tags.append('movie')
    elif metadata.get('Type') == 'episode': 
        video_tags.append('tv show')

    video_id = upload_video_to_youtube(youtube_service, final_video_path, video_title, video_description, video_tags)

    # 6. Upload Subtitles (if available)
    if video_id and extracted_srt_path:
        upload_subtitle_to_youtube(youtube_service, video_id, extracted_srt_path)

    print("Workflow completed successfully!")

if __name__ == '__main__':
    try:
        asyncio.run(main_workflow())
    except KeyboardInterrupt:
        print("Workflow stopped by user.")
    except Exception as e:
        print(f"Critical Error: {e}")
        sys.exit(1)
