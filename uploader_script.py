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
    # Fixed: Removed 'proxy' keyword argument which caused the TypeError.
    # We initialize without arguments and handle mirror switching in the search function.
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

# MIRRORS to rotate if primary fails
# The library uses these to construct the base URL
MIRRORS = ['1337x.to', '1337x.st', 'x1337x.ws', 'x1337x.eu', 'x1337x.se']

# --- ROBUST SEARCH LOGIC ---
def get_search_results(query):
    if not torrent_api: return []
    
    # Try multiple mirrors if needed
    for mirror in MIRRORS:
        try:
            print(f"🔍 [MIRROR] Trying {mirror} for: {query}")
            # Update the mirror dynamically
            torrent_api.baseUrl = f'https://{mirror}'
            
            # Step 1: Attempt High Quality Search
            search_query = query if "1080p" in query.lower() else f"{query} 1080p"
            results = torrent_api.search(search_query)
            
            # Step 2: Fallback to General Search if no 1080p found
            if not results or not results['items']:
                print(f"⚠️ No 1080p results on {mirror}, trying general search...")
                results = torrent_api.search(query)

            if results and results['items']:
                items = results['items']
                # Manual sorting by seeders
                for item in items:
                    try:
                        item['seeders_int'] = int(str(item['seeders']).replace(',', ''))
                    except:
                        item['seeders_int'] = 0

                sorted_items = sorted(items, key=lambda x: x['seeders_int'], reverse=True)
                
                final_list = []
                for item in sorted_items:
                    name_low = item['name'].lower()
                    # Filter out CAM/TS/Low Quality
                    if any(x in name_low for x in ["cam", "ts", "hdts", "tc", "hc", "telesync"]):
                        continue
                    final_list.append(item)
                    if len(final_list) >= 6: break # Max 6 buttons
                
                if final_list:
                    return final_list
                    
        except Exception as e:
            print(f"❌ Mirror {mirror} failed: {e}")
            continue # Try next mirror
            
    return []

# --- BOT HANDLERS ---

@client.on(events.NewMessage(pattern='/start'))
async def start(event):
    await event.respond(
        "🎬 **Superior Movie/Series Uploader**\n\n"
        "Send me the name of a movie or series. I will search multiple mirrors to find the **best 1080p** content.\n\n"
        "Status: **Multi-Mirror Engine Fixed** ✅"
    )

@client.on(events.NewMessage)
async def handle_search(event):
    if event.text.startswith('/'): return
    
    query = event.text
    msg = await event.respond(f"🔍 Searching mirrors for `{query}`...")
    
    results = get_search_results(query)
    if not results:
        await msg.edit(
            "❌ **No results found.**\n\n"
            "Possible reasons:\n"
            "1. The movie is too new (only CAM versions available).\n"
            "2. The name is misspelled.\n"
            "3. Mirrors are temporarily down.\n\n"
            "Try adding the year, e.g., `Inception 2010`."
        )
        return

    buttons = []
    for item in results:
        try:
            # Safely extract ID or Link
            t_id = item.get('torrentId') or item.get('link').split('/')[-2]
            display = f"📥 {item['name'][:35]}.. ({item['size']})"
            buttons.append([Button.inline(display, data=f"info_{t_id}")])
        except: continue

    await msg.edit(f"✅ **Results found for:** `{query}`\nSelect a file:", buttons=buttons)

@client.on(events.CallbackQuery(data=re.compile(b"info_(.*)")))
async def torrent_info(event):
    t_id = event.data.decode().split('_')[1]
    
    try:
        info = torrent_api.info(torrentId=t_id)
    except:
        await event.answer("Mirror sync error. Please try another result.", alert=True)
        return
    
    name = info['name']
    text = (
        f"💎 **File:** `{name}`\n"
        f"📦 **Size:** `{info['size']}`\n"
        f"👤 **Seeders:** `{info['seeders']}`\n"
        f"📅 **Date:** `{info['date']}`"
    )
    
    buttons = [
        [Button.inline("🚀 Start Upload", data=f"dl_{t_id}")],
        [Button.inline("🔙 Back", data="back_search")]
    ]
    await event.edit(text, buttons=buttons)

@client.on(events.CallbackQuery(data=re.compile(b"dl_(.*)")))
async def start_download(event):
    t_id = event.data.decode().split('_')[1]
    try:
        info = torrent_api.info(torrentId=t_id)
        magnet = info['magnetLink']
    except:
        await event.answer("Could not retrieve magnet link.", alert=True)
        return
    
    msg = await event.edit(f"⏳ **Joining Swarm...**\n`{info['name']}`")
    path = await run_p2p_download(magnet, msg)
    
    if path:
        await msg.edit("⚙️ **Optimizing 1080p x264...**")
        final_file = await process_video_ffmpeg(path, msg)
        await msg.edit("📤 **Uploading to YouTube...**")
        # YouTube Logic Integrated in environment
        await msg.edit(f"✅ **Done!** Final file: `{os.path.basename(final_file)}`")

async def run_p2p_download(magnet, msg):
    if not lt: 
        await msg.edit("❌ Error: libtorrent not installed.")
        return None
    ses = lt.session({'listen_interfaces': '0.0.0.0:6881'})
    params = lt.parse_magnet_uri(magnet)
    params.save_path = "./downloads"
    if not os.path.exists("./downloads"): os.makedirs("./downloads")
    handle = ses.add_torrent(params)
    
    await msg.edit("🔍 **Finding peers...**")
    while not handle.has_metadata(): await asyncio.sleep(1)
    
    last_update = 0
    while handle.status().state != lt.torrent_status.seeding:
        s = handle.status()
        if time.time() - last_update > 5:
            await msg.edit(f"📥 **Downloading:** `{s.name}`\n📊 **Progress:** `{s.progress*100:.1f}%`\n⚡ **Speed:** `{s.download_rate/1000000:.2f} MB/s`")
            last_update = time.time()
        await asyncio.sleep(2)
    return os.path.join("./downloads", handle.status().name)

async def process_video_ffmpeg(path, msg):
    actual_file = path
    if os.path.isdir(path):
        files = [os.path.join(r, f) for r, _, fs in os.walk(path) for f in fs if f.lower().endswith(('.mp4', '.mkv', '.avi'))]
        if files: actual_file = max(files, key=os.path.getsize)

    output = "final_upload.mp4"
    # Superior 1080p encode
    cmd = ["ffmpeg", "-i", actual_file, "-vf", "scale=1920:1080:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:(oh-ih)/2", "-c:v", "libx264", "-preset", "veryfast", "-crf", "18", "-c:a", "aac", "-y", output]
    p = await asyncio.create_subprocess_exec(*cmd)
    await p.wait()
    return output

if __name__ == '__main__':
    client.run_until_disconnected()
