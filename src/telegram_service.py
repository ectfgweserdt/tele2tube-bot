import os
import re
import asyncio
from telethon import events
from telethon.tl.types import MessageMediaDocument

async def download_latest_video(client, channel_link, download_path):
    """
    Downloads the most recent video from a specified Telegram channel.
    Supports usernames, public links, and private channel IDs/message links.
    """
    print(f"DEBUG: Starting download_latest_video for: {channel_link}")
    
    try:
        # Check connection status
        if not client.is_connected():
            print("DEBUG: Client not connected, attempting to connect...")
            await client.connect()
        
        message_id = None
        entity_id = channel_link

        # 1. Parse the link to check if it's a specific message link
        if isinstance(channel_link, str) and "/c/" in channel_link:
            print("DEBUG: Detected private message link format.")
            parts = channel_link.split('/')
            try:
                message_id = int(parts[-1])
                raw_id = parts[parts.index('c') + 1]
                entity_id = int(f"-100{raw_id}")
                print(f"DEBUG: Parsed Entity ID: {entity_id}, Message ID: {message_id}")
            except (ValueError, IndexError) as e:
                print(f"DEBUG: Parsing failed: {e}")
        elif isinstance(channel_link, str) and (channel_link.startswith('-100') or channel_link.isdigit()):
            entity_id = int(channel_link)

        # 2. Access the entity
        print(f"DEBUG: Resolving entity for ID: {entity_id}...")
        entity = await client.get_entity(entity_id)
        entity_title = getattr(entity, 'title', 'Private Channel')
        print(f"Accessed entity: {entity_title}")

        # 3. Fetch messages
        messages = []
        if message_id:
            print(f"DEBUG: Fetching specific message {message_id}...")
            # We use a list to handle multiple or single returns
            result = await client.get_messages(entity, ids=message_id)
            messages = [result] if result else []
        else:
            print("DEBUG: Fetching latest message (limit 1)...")
            messages = await client.get_messages(entity, limit=1)

        if not messages:
            print("DEBUG: No messages returned from Telegram.")
            return None

        # 4. Process the messages
        for message in messages:
            if message and (message.video or (message.document and message.document.mime_type and message.document.mime_type.startswith('video/'))):
                video_name = message.file.name or f"video_{message.id}.mp4"
                print(f"Found video: {video_name}")
                
                os.makedirs(download_path, exist_ok=True)
                print(f"DEBUG: Downloading media to {download_path}...")
                
                # Using a progress callback to prevent GitHub Actions from thinking the task is dead
                def callback(received, total):
                    if total:
                        percent = (received / total) * 100
                        if int(percent) % 25 == 0: # Log every 25%
                            print(f"Download Progress: {percent:.2f}%")

                path = await message.download_media(file=download_path, progress_callback=callback)
                print(f"Downloaded to: {path}")
                return path
        
        print("No video found in the specified location or latest message.")
        return None

    except Exception as e:
        print(f"ERROR in download_latest_video: {type(e).__name__}: {e}")
        return None
