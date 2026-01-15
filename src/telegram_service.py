import os
from telethon import events
from telethon.tl.types import MessageMediaDocument

async def download_latest_video(client, channel_link, download_path):
    """
    Downloads the most recent video from a specified Telegram channel.
    Supports usernames, public links, and private channel IDs.
    """
    print(f"Successfully connected to Telegram.")
    
    try:
        # If the input looks like a private channel ID (-100...), convert to int
        if isinstance(channel_link, str) and (channel_link.startswith('-100') or channel_link.isdigit()):
            entity_id = int(channel_link)
        else:
            # Handle links like https://t.me/c/12345/67 by extracting the ID
            if "/c/" in channel_link:
                parts = channel_link.split('/')
                # The ID is usually the part after 'c'
                idx = parts.index('c') + 1
                entity_id = int(f"-100{parts[idx]}")
            else:
                entity_id = channel_link

        entity = await client.get_entity(entity_id)
        print(f"Accessed entity: {entity.title if hasattr(entity, 'title') else 'Private Channel'}")

        # Fetch the last 10 messages to find a video
        async for message in client.iter_messages(entity, limit=10):
            if message.video or (message.document and message.document.mime_type.startswith('video/')):
                print(f"Found video: {message.file.name or 'untitled_video'}")
                
                # Ensure the download path exists
                os.makedirs(download_path, exist_ok=True)
                
                path = await message.download_media(file=download_path)
                print(f"Downloaded to: {path}")
                return path
        
        print("No videos found in the last 10 messages.")
        return None

    except Exception as e:
        print(f"Could not get channel entity: {e}")
        return None
