import os
from telethon import TelegramClient
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.errors import UserAlreadyParticipantError

async def download_latest_video(client, channel_entity, download_path):
    """
    Connects to a channel and downloads the latest video file.
    """
    print("Successfully connected to Telegram.")
    
    channel = None
    try:
        # Check if it's a private join link or a public username
        if 'joinchat' in channel_entity or '+' in channel_entity:
            # Simple heuristic for private links
            try:
                invite_hash = channel_entity.split('/')[-1].replace('+', '')
                updates = await client(ImportChatInviteRequest(invite_hash))
                channel = updates.chats[0]
                print(f"Successfully joined private channel: {channel.title}")
            except UserAlreadyParticipantError:
                print("Already a member of this private channel.")
                # We need to resolve the entity if we are already a member
                # This might require the user to pass the channel ID or cached entity in a real scenario
                # For this implementation, we assume the entity is accessible after join
                pass 
        
        if not channel:
            # Try getting entity directly (public channel or already joined)
            channel = await client.get_entity(channel_entity)
            
    except Exception as e:
        print(f"Could not get channel entity: {e}")
        return None

    print(f"Searching for videos in '{channel.title if hasattr(channel, 'title') else channel_entity}'...")
    
    # Iterate through messages to find the newest video
    async for message in client.iter_messages(channel, limit=50):
        if message.video:
            print(f"Found video with ID: {message.id}. Downloading...")
            
            # Determine filename
            original_filename = "unknown_video.mp4"
            if message.video.attributes:
                # attributes[-1] is usually DocumentAttributeFilename
                for attr in message.video.attributes:
                    if hasattr(attr, 'file_name'):
                        original_filename = attr.file_name
                        break
            
            file_path = await message.download_media(
                file=os.path.join(download_path, f"{message.id}_{original_filename}")
            )
            print(f"Video downloaded successfully to: {file_path}")
            return file_path
            
    print("No new videos found in the recent messages.")
    return None
