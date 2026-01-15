import os
import re
from telethon import events
from telethon.tl.types import MessageMediaDocument

async def download_latest_video(client, channel_link, download_path):
    """
    Downloads the most recent video from a specified Telegram channel.
    Supports usernames, public links, and private channel IDs/message links.
    """
    print(f"Successfully connected to Telegram.")
    
    try:
        message_id = None
        entity_id = channel_link

        # 1. Parse the link to check if it's a specific message link
        # Example: https://t.me/c/123456789/41
        if isinstance(channel_link, str) and "/c/" in channel_link:
            parts = channel_link.split('/')
            try:
                # Extract message ID (last part)
                message_id = int(parts[-1])
                # Extract channel ID and prefix with -100
                raw_id = parts[parts.index('c') + 1]
                entity_id = int(f"-100{raw_id}")
            except (ValueError, IndexError):
                pass
        elif isinstance(channel_link, str) and (channel_link.startswith('-100') or channel_link.isdigit()):
            entity_id = int(channel_link)

        # 2. Access the entity
        entity = await client.get_entity(entity_id)
        print(f"Accessed entity: {entity.title if hasattr(entity, 'title') else 'Private Channel'}")

        # 3. If a specific message ID was found in the link, try to get just that message
        if message_id:
            print(f"Attempting to fetch specific message ID: {message_id}")
            messages = await client.get_messages(entity, ids=message_id)
            # get_messages returns a list or a single object; wrap in list if needed
            if not isinstance(messages, list):
                messages = [messages]
        else:
            # Otherwise, try to get the latest message
            # Bots sometimes fail at iter_messages(limit=10) but succeed at get_messages(limit=1)
            messages = await client.get_messages(entity, limit=1)

        # 4. Process the messages
        for message in messages:
            if message and (message.video or (message.document and message.document.mime_type.startswith('video/'))):
                video_name = message.file.name or f"video_{message.id}.mp4"
                print(f"Found video: {video_name}")
                
                os.makedirs(download_path, exist_ok=True)
                path = await message.download_media(file=download_path)
                print(f"Downloaded to: {path}")
                return path
        
        print("No video found in the specified location/latest message.")
        return None

    except Exception as e:
        print(f"Could not get channel entity or message: {e}")
        return None
