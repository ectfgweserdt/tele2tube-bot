import json
import subprocess
import os

def inspect_and_select_streams(video_path):
    """Uses ffprobe to inspect the video file and identify the best video and audio streams."""
    command = ['ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_streams', video_path]
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        streams = json.loads(result.stdout)['streams']
        
        video_stream_index = None
        audio_stream_index = None
        
        # Find Video Stream
        for stream in streams:
            if stream.get('codec_type') == 'video' and video_stream_index is None:
                video_stream_index = stream['index']
        
        # Find Audio Stream (Prioritize English)
        for stream in streams:
            if stream.get('codec_type') == 'audio':
                lang = stream.get('tags', {}).get('language', '').lower()
                if lang == 'eng':
                    audio_stream_index = stream['index']
                    break
        
        # Fallback audio stream if no English found
        if audio_stream_index is None:
            for stream in streams:
                if stream.get('codec_type') == 'audio':
                    audio_stream_index = stream['index']
                    break
                    
        print(f"Selected Video Stream Index: {video_stream_index}, Audio Stream Index: {audio_stream_index}")
        return video_stream_index, audio_stream_index
    except Exception as e:
        print(f"Error parsing ffprobe output: {e}")
        return None, None

def transcode_for_youtube(input_path, output_path, video_index, audio_index):
    """Transcodes a video to H.264/AAC using the selected stream indices."""
    command = [
        'ffmpeg', '-i', input_path,
        '-map', f'0:{video_index}',      # Select identified video
        '-map', f'0:{audio_index}',      # Select identified audio
        '-c:v', 'libx264',               # H.264 Video
        '-preset', 'slow',               
        '-crf', '18',                    
        '-c:a', 'aac',                   # AAC Audio
        '-b:a', '192k',
        '-pix_fmt', 'yuv420p',           # Compatibility pixel format
        '-y', output_path
    ]
    print("Starting FFmpeg transcoding...")
    try:
        subprocess.run(command, check=True, capture_output=True)
        print(f"Transcoding complete. Output file: {output_path}")
        return output_path
    except subprocess.CalledProcessError as e:
        print(f"Error during FFmpeg transcoding: {e.stderr.decode()}")
        return None

def inspect_and_extract_subtitles(video_path, output_srt_path):
    """
    Inspects for an English subtitle stream and extracts it to an SRT file.
    """
    print("Checking for embedded English subtitles...")
    
    # 1. Identify Stream
    ffprobe_command = [
        'ffprobe', '-loglevel', 'error', '-select_streams', 's',
        '-show_entries', 'stream=index:stream_tags=language', '-of', 'json', video_path
    ]
    try:
        result = subprocess.run(ffprobe_command, capture_output=True, text=True, check=True)
        data = json.loads(result.stdout)
        streams = data.get('streams', [])
        
        eng_subtitle_stream = None
        for stream in streams:
            # Check for 'eng' or 'en'
            lang = stream.get('tags', {}).get('language', '').lower()
            if 'en' in lang:
                eng_subtitle_stream = stream
                break
        
        if not eng_subtitle_stream:
            print("No English subtitle stream found.")
            return None

        print(f"Found English subtitle stream at index {eng_subtitle_stream['index']}. Extracting...")
        
        # 2. Extract Stream
        ffmpeg_command = [
            'ffmpeg', '-i', video_path,
            '-map', f"0:{eng_subtitle_stream['index']}",
            '-c:s', 'srt',
            '-y', output_srt_path
        ]
        subprocess.run(ffmpeg_command, check=True, capture_output=True, text=True)
        print(f"Successfully extracted subtitles to {output_srt_path}")
        return output_srt_path

    except (subprocess.CalledProcessError, FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Could not process subtitles: {e}")
        return None
