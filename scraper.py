# scraper.py
import yt_dlp
import requests
import xml.etree.ElementTree as ET

def get_youtube_transcript_url(video_id):
    """ Fetches the transcript URL for a given YouTube video ID. """
    video_url = f"https://www.youtube.com/watch?v={video_id}"
    ydl_opts = {
        'verbose': False,
        'quiet': True,
        'writesubtitles': True,
        'skip_download': True,
        'subtitleslangs': ['en'],
    }
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(video_url, download=False)
            # Check for automatic captions first
            if 'automatic_captions' in info and 'en' in info['automatic_captions']:
                for transcript in info['automatic_captions']['en']:
                    if transcript.get('ext') == 'srv1':
                        return transcript['url']
            # Fallback to requested subtitles if auto captions are not found
            elif 'requested_subtitles' in info and info['requested_subtitles'] and 'en' in info['requested_subtitles']:
                 return info['requested_subtitles']['en']['url']
        return None
    except Exception as e:
        print(f"Error extracting transcript URL for {video_id}: {e}")
        return None

def get_text_from_xml(xml_data):
    """ Parses XML transcript data and extracts the text. """
    try:
        root = ET.fromstring(xml_data)
        text_segments = []
        for text_elem in root.findall(".//text"):
            text = text_elem.text
            if text is not None:
                text = text.strip()
                if text and text != "[Music]":  # Ensure text is not empty
                    text_segments.append(text)
        return " ".join(text_segments)
    except ET.ParseError as e:
        print(f"Error parsing XML: {e}")
        return ""

def gen_word_groups(sentence, group_size):
    """ Splits a long string of text into chunks of a specified word count. """
    if not sentence:
        return []
    words = sentence.split()
    return [" ".join(words[i : i + group_size]) for i in range(0, len(words), group_size)]

def process_video(video_db_id, video_id):
    """
    Main worker function to process a single video.
    Returns the video's database ID and a list of transcript chunks.
    """
    print(f"Processing video: {video_id} (DB ID: {video_db_id})")
    transcript_url = get_youtube_transcript_url(video_id)
    
    if not transcript_url:
        print(f"Transcript not found for video: {video_id}")
        return video_db_id, None

    try:
        response = requests.get(transcript_url, timeout=10)
        response.raise_for_status()
        
        full_text = get_text_from_xml(response.content)
        if not full_text:
            print(f"No text content found in transcript for video: {video_id}")
            return video_db_id, None
            
        # Split the full text into chunks of 500 words
        transcript_chunks = gen_word_groups(full_text, 500)
        return video_db_id, transcript_chunks
    except requests.RequestException as e:
        print(f"Failed to download transcript for {video_id}: {e}")
        return video_db_id, None
    except Exception as e:
        print(f"An unexpected error occurred while processing {video_id}: {e}")
        return video_db_id, None