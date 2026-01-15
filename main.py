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
    config.validate_config()
    print("Starting Telegram-to-YouTube automation workflow...")

    # Initialize the client
    client = TelegramClient(config.SESSION_NAME, config.API_ID, config.API_HASH)
    
    # FIX: Correct way to start the bot and use it as a context manager
    await client.start(bot_token=config.BOT_TOKEN)
    
    async with client:
        print(f"Logged in successfully as bot. Checking channel: {config.CHANNEL_ENTITY}")
        downloaded_file = await download_latest_video(client, config.CHANNEL_ENTITY, config.DOWNLOAD_PATH)
        
        if not downloaded_file:
            print("Workflow finished: No new video found or error during download.")
            return

    # 2. Inspect & Process Media (Outside the 'async with client' block to free Telegram resources)
    video_index, audio_index = inspect_and_select_streams(downloaded_file)
    if video_index is None or audio_index is None:
        print("Could not find valid video/audio streams.")
        return

    filename = os.path.basename(downloaded_file)
    processed_file_path = os.path.join(config.PROCESSED_PATH, f"processed_{filename}")
    
    final_video_path = transcode_for_youtube(downloaded_file, processed_file_path, video_index, audio_index)
    if not final_video_path:
        return

    srt_output_path = os.path.join(config.PROCESSED_PATH, f"{os.path.splitext(filename)[0]}.srt")
    extracted_srt_path = inspect_and_extract_subtitles(downloaded_file, srt_output_path)

    # 3. Metadata Generation
    parsed_info = PTN.parse(filename)
    metadata = get_omdb_metadata(config.OMDB_API_KEY, parsed_info.get('title'), parsed_info.get('year'))
    
    if not metadata:
        metadata = {"Title": parsed_info.get('title'), "Plot": "Automatic upload via Tele2Tube bot."}
        
    video_title = generate_youtube_title(metadata, parsed_info)
    video_description = generate_youtube_description_with_gemini(config.GEMINI_API_KEY, metadata)

    # 4. YouTube Upload
    youtube_service = get_authenticated_service_non_interactive()
    if not youtube_service:
        print("Failed to initialize YouTube service.")
        return
    
    video_id = upload_video_to_youtube(
        youtube_service, 
        final_video_path, 
        video_title, 
        video_description, 
        ["telegram", "automation", "movie"]
    )

    if video_id and extracted_srt_path:
        upload_subtitle_to_youtube(youtube_service, video_id, extracted_srt_path)

    print("Workflow completed successfully!")

if __name__ == '__main__':
    asyncio.run(main_workflow())
