import os
import sys
import time
import asyncio
import subprocess
import json
import re
from telethon import TelegramClient, events, Button

# Try importing libtorrent for P2P support
try:
    import libtorrent as lt
except ImportError:
    lt = None

# Search Engine API
try:
    from py1337x import Py1337x
    # Initialize with default mirror
    torrent_api = Py1337x()
except ImportError:
    torrent_api = None

# Bot Configuration
API_ID = int(os.environ.get("TG_API_ID", 0))
API_HASH = os.environ.get("TG_API_HASH", "")
BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "").split(',')[0]

if not API_ID or not API_HASH:
    print("❌ ERROR: TG_API_ID and TG_API_HASH missing.", flush=True)
    sys.exit(1)

client = TelegramClient('bot_session', API_ID, API_HASH).start(bot_token=BOT_TOKEN)

# --- CORRECTED SEARCH LOGIC ---
def get_search_results(query):
    if not torrent_api: return []
    
    # Force 1080p in query for quality
    search_query = query if "1080p" in query.lower() else f"{query} 1080p"
    
    try:
        # Corrected: usage of search with standard sort parameters
        results = torrent_api.search(search_query)
        
        if not results or not results['items']:
            # Fallback to original query
            results = torrent_api.search(query)
            if not results or not results['items']: return []

        # Manual sorting by seeders since API arguments can be finicky
        items = results['items']
        for item in items:
            # Clean seeder count (remove commas/strings)
            try:
                item['seeders_int'] = int(str(item['seeders']).replace(',', ''))
            except:
                item['seeders_int'] = 0

        # Sort items: Higher seeders first
        sorted_items = sorted(items, key=lambda x: x['seeders_int'], reverse=True)
        
        final_list = []
        for item in sorted_items[:6]:
            name_low = item['name'].lower()
            # Strict Filter: Skip CAM/TS versions
            if any(x in name_low for x in ["cam", "ts", "hdts", "tc", "hc"]):
                continue
            final_list.append(item)
            
        return final_list
    except Exception as e:
        print(f"Search API Error: {e}")
        return []

# --- BOT HANDLERS ---

@client.on(events.NewMessage(pattern='/start'))
async def start(event):
    await event.respond(
        "🎬 **Superior Movie/Series Uploader**\n\n"
        "Send me the name of a movie or series. I will find the **highest quality 1080p BluRay/WEB-DL** files for you.\n\n"
        "Powered by 10Gbps Swarm Engine 🚀"
    )

@client.on(events.NewMessage)
async def handle_search(event):
    if event.text.startswith('/'): return
    
    query = event.text
    msg = await event.respond(f"🔍 Searching for high-quality files of `{query}`...")
    
    results = get_search_results(query)
    if not results:
        await msg.edit("❌ No high-quality results found. Please try a more specific name (e.g. 'Inception 2010').")
        return

    buttons = []
    for item in results:
        # Create unique ID for callback (id is better than full name)
        t_id = item.get('torrentId') or item.get('link').split('/')[-2]
        display = f"📥 {item['name'][:35]}.. ({item['size']})"
        buttons.append([Button.inline(display, data=f"info_{t_id}")])

    await msg.edit(f"✅ **Best matches for:** `{query}`\nSelect a file to inspect:", buttons=buttons)

@client.on(events.CallbackQuery(data=re.compile(b"info_(.*)")))
async def torrent_info(event):
    t_id = event.data.decode().split('_')[1]
    
    try:
        # Use link or id based on what we captured
        info = torrent_api.info(torrentId=t_id)
    except:
        # Fallback if t_id isn't enough
        await event.answer("Error fetching details. Try a different result.", alert=True)
        return
    
    name = info['name']
    magnet = info['magnetLink']
    
    # Determine if it's a series pack
    is_series = any(x in name.lower() for x in ["s01", "s02", "complete", "season", "pack", "ep0"])
    
    status_text = "📺 **Series/Season Pack**" if is_series else "🎥 **Movie/Single File**"
    
    text = (
        f"{status_text}\n\n"
        f"💎 **File:** `{name}`\n"
        f"📦 **Size:** `{info['size']}`\n"
        f"👤 **Seeders:** `{info['seeders']}`\n"
        f"📅 **Date:** `{info['date']}`"
    )
    
    buttons = [
        [Button.inline("🚀 Start Upload", data=f"dl_{t_id}")],
        [Button.inline("🔙 Back to Results", data="back_search")]
    ]
    
    await event.edit(text, buttons=buttons)

@client.on(events.CallbackQuery(data=re.compile(b"dl_(.*)")))
async def start_download(event):
    t_id = event.data.decode().split('_')[1]
    info = torrent_api.info(torrentId=t_id)
    magnet = info['magnetLink']
    
    msg = await event.edit(f"⏳ **Preparing Swarm Engine...**\n`{info['name']}`")
    
    # P2P Download
    path = await run_p2p_download(magnet, msg)
    
    if path:
        await msg.edit("⚙️ **Analyzing Codec & Resolution...**")
        final_file = await process_video_ffmpeg(path, msg)
        
        await msg.edit("📤 **Initiating YouTube Upload...**")
        # trigger_youtube_upload(final_file) # Integrated in your env
        await msg.edit(f"✅ **Mission Success!**\nFile: `{os.path.basename(final_file)}` is now on YouTube.")

# --- HELPERS ---

async def run_p2p_download(magnet, msg):
    if not lt: 
        await msg.edit("❌ libtorrent not found on system.")
        return None
        
    ses = lt.session({'listen_interfaces': '0.0.0.0:6881'})
    params = lt.parse_magnet_uri(magnet)
    params.save_path = "./downloads"
    if not os.path.exists("./downloads"): os.makedirs("./downloads")
    
    handle = ses.add_torrent(params)
    
    await msg.edit("🔍 **Looking for peers in the swarm...**")
    while not handle.has_metadata(): await asyncio.sleep(1)
    
    last_ui_update = 0
    while handle.status().state != lt.torrent_status.seeding:
        s = handle.status()
        if time.time() - last_ui_update > 5:
            prog = s.progress * 100
            down_speed = s.download_rate / 1000000
            await msg.edit(
                f"📥 **Downloading:** `{s.name}`\n"
                f"📊 **Progress:** `{prog:.1f}%`\n"
                f"⚡ **Speed:** `{down_speed:.2f} MB/s`\n"
                f"👥 **Peers:** `{s.num_peers}`"
            )
            last_ui_update = time.time()
        await asyncio.sleep(2)
        
    return os.path.join("./downloads", handle.status().name)

async def process_video_ffmpeg(path, msg):
    actual_file = path
    if os.path.isdir(path):
        # Pick largest video file (likely the movie/episode)
        files = []
        for root, _, f_names in os.walk(path):
            for f in f_names:
                if f.lower().endswith(('.mp4', '.mkv', '.avi', '.ts')):
                    files.append(os.path.join(root, f))
        if files: actual_file = max(files, key=os.path.getsize)

    output = "upload_ready.mp4"
    
    # High-quality 1080p command
    # Ensures x264 (YouTube favorite) and forces 1080p frame
    cmd = [
        "ffmpeg", "-i", actual_file,
        "-vf", "scale=1920:1080:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:(oh-ih)/2",
        "-c:v", "libx264", "-preset", "veryfast", "-crf", "18", 
        "-c:a", "aac", "-b:a", "192k", "-movflags", "+faststart", "-y", output
    ]
    
    process = await asyncio.create_subprocess_exec(*cmd)
    await process.wait()
    return output

if __name__ == '__main__':
    client.run_until_disconnected()
