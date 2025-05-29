import os
import sys
import time
import logging
import tempfile
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
from google.auth.exceptions import GoogleAuthError
import requests
import whisper
from dotenv import load_dotenv

# Import utility function to get current IP
try:
    from fb_antidetect_utils import get_current_ip
except ImportError:
    # Fallback if fb_antidetect_utils not available
    def get_current_ip():
        try:
            import requests
            response = requests.get("https://httpbin.org/ip", timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data and 'origin' in data:
                    return data['origin']
            return "Unknown IP"
        except Exception:
            return "Unknown IP"

# ================== CONFIGURATION ==================
SHEET_NAME = "Master Auto Swipe - Test ankur"
SOURCE_TAB = "Ads Details"
TARGET_TAB = "Transcript"
CREDENTIALS_PATH = "credentials.json"
BATCH_SIZE = float('inf')  # Process all videos
WHISPER_MODEL = "base"  # or "small", "medium", "large" as needed

# Check if we're running in Cloud Shell
IS_CLOUD_SHELL = os.environ.get('CLOUD_SHELL') == 'true' or '/google/devshell/' in os.getcwd()
if IS_CLOUD_SHELL:
    FFMPEG_PATH = "/usr/bin"  # Default location in Linux/Cloud Shell
else:
    FFMPEG_PATH = r"C:\ffmpeg\bin"  # Windows path

# ============== LOGGING SETUP =====================
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"transcript_bot_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

def log(msg, level="info"):
    getattr(logging, level)(msg)

# ============ ENVIRONMENT SETUP ===================
load_dotenv()
os.environ["PATH"] += os.pathsep + FFMPEG_PATH

# ============ GOOGLE SHEETS SETUP =================
def get_gsheet_client():
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=scope)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        log(f"Google Sheets authentication failed: {e}", "error")
        sys.exit(1)

def get_sheet(client, tab_name):
    try:
        sheet = client.open(SHEET_NAME).worksheet(tab_name)
        return sheet
    except Exception as e:
        log(f"Failed to open tab '{tab_name}': {e}", "error")
        sys.exit(1)

# =========== COLUMN NAMES (TRAILING SPACES) =======
# Adjust as per your actual sheet (see memory)
COLUMNS = {
    "Name of page": "Name of page",
    "page_id": "page id",  # Column name has a space in the sheet
    "library_id": "library_id",
    "ads_count": "ads_count",
    "media_url": "media_url",
    "media_type": "media_type",
}
# If your real columns have trailing spaces, update here (e.g. "Name of page ")

# =========== TRANSCRIPT TAB HEADERS ===============
TRANSCRIPT_HEADERS = [
    "Name of page", "page_id", "library_id", "ads_count", "media_url", "Transcript", "Last Update Time", "IP Address"
]

def ensure_transcript_headers(sheet):
    headers = sheet.row_values(1)
    if headers != TRANSCRIPT_HEADERS:
        sheet.update('A1:G1', [TRANSCRIPT_HEADERS])
        log(f"Updated headers in '{TARGET_TAB}' tab.")

# ============ VIDEO DOWNLOADER ====================
def download_video(url, out_dir):
    try:
        local_filename = os.path.join(out_dir, url.split("/")[-1].split("?")[0])
        log(f"Downloading video: {url}")
        
        # Direct connection to download the video
        with requests.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        return local_filename
    except Exception as e:
        log(f"Failed to download video: {url} | Error: {e}", "error")
        return None

# ============ WHISPER TRANSCRIBE ==================
def transcribe_video(file_path, model=None):
    try:
        if model is None:
            model = whisper.load_model(WHISPER_MODEL)
        result = model.transcribe(file_path)
        return result["text"]
    except Exception as e:
        log(f"Failed to transcribe video: {file_path} | Error: {e}", "error")
        return None

# ============ MAIN BOT LOGIC ======================
def main():
    log("==== TRANSCRIPT BOT START ====")
    
    # Initialize Google Sheets client
    client = get_gsheet_client()
    
    # Get current IP address
    current_ip = get_current_ip()
    log(f"Using IP: {current_ip}")
    
    ads_sheet = get_sheet(client, SOURCE_TAB)
    transcript_sheet = get_sheet(client, TARGET_TAB)
    ensure_transcript_headers(transcript_sheet)
    
    # Get ScrapeOps proxy IP address (not your actual IP)
    current_ip = get_current_ip()
    log(f"Using ScrapeOps Proxy IP: {current_ip}")

    # Read all data from Ads Details
    all_rows = ads_sheet.get_all_records()
    log(f"Loaded {len(all_rows)} rows from '{SOURCE_TAB}'.")

    # Filter for videos
    video_rows = [row for row in all_rows if str(row.get(COLUMNS["media_type"], "")).strip().lower() == "video"]
    log(f"Found {len(video_rows)} video ads.")


    # Get already processed videos with their dates and URLs
    # Pass expected_headers to handle non-unique headers
    existing_records = transcript_sheet.get_all_records(expected_headers=TRANSCRIPT_HEADERS)
    today = datetime.now().strftime("%Y-%m-%d")
    
    # Create dicts to track both library_ids and URLs
    existing = {}
    existing_urls = {}
    for row in existing_records:
        lib_id = row.get("library_id")
        media_url = row.get("media_url")
        last_update = row.get("Last Update Time", "")
        if lib_id and last_update:
            update_date = last_update.split()[0]  # Get just the date part
            existing[lib_id] = {
                'date': update_date,
                'url': media_url
            }
            if media_url:
                existing_urls[media_url] = {
                    'date': update_date,
                    'lib_id': lib_id
                }
    
    log(f"Found {len(existing)} existing transcripts")

    # Prepare Whisper model
    model = whisper.load_model(WHISPER_MODEL)

    processed = 0
    failed = 0
    total_videos = len(video_rows)
    log(f"Starting to process all {total_videos} videos...")
    for row in video_rows:
        if processed >= BATCH_SIZE:
            break
        lib_id = row.get(COLUMNS["library_id"])
        media_url = row.get(COLUMNS["media_url"])
        if not media_url:
            log(f"No media_url for library_id={lib_id}", "warning")
            continue

        # Check both library_id and media_url for duplicates
        skip_reason = None
        if lib_id in existing:
            last_update = existing[lib_id]['date']
            if last_update == today:
                skip_reason = f"library_id={lib_id} already processed today ({today})"

        if not skip_reason and media_url in existing_urls:
            url_info = existing_urls[media_url]
            if url_info['date'] == today:
                skip_reason = f"media_url already processed today for library_id={url_info['lib_id']}"

        if skip_reason:
            log(f"Skipping: {skip_reason}")
            continue
        page = row.get(COLUMNS["Name of page"])
        page_id = row.get(COLUMNS["page_id"])
        ads_count = row.get(COLUMNS["ads_count"])
        media_url = row.get(COLUMNS["media_url"])
        
        # Only process videos if ads_count is greater than 99
        try:
            ads_count_int = int(str(ads_count).strip())
        except (ValueError, TypeError):
            log(f"Invalid ads_count for library_id={lib_id}: {ads_count}", "warning")
            continue
        
        if ads_count_int <= 10:
            log(f"Skipping video with ads_count={ads_count_int} for library_id={lib_id}", "info")
            continue
        
        if not media_url:
            log(f"No media_url for library_id={lib_id}", "warning")
            continue
        
        with tempfile.TemporaryDirectory() as tmpdir:
            video_path = download_video(media_url, tmpdir)
            if not video_path:
                failed += 1
                continue
            transcript = transcribe_video(video_path, model)
            if not transcript:
                failed += 1
                continue
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            # Make sure we get page_id using the correct column name with space
            page_id = row.get(COLUMNS["page_id"])
            # Include the current IP in the IP Address column
            new_row = [page, page_id, lib_id, ads_count, media_url, transcript, now, current_ip]
            transcript_sheet.append_row(new_row)
            log(f"Processed and updated transcript for library_id={lib_id}")
            processed += 1
            if processed % 10 == 0:  # Log progress every 10 videos
                log(f"Progress: {processed}/{total_videos} videos processed ({(processed/total_videos)*100:.1f}%)")
    log(f"All done! Successfully processed: {processed}, Failed: {failed}, Total: {total_videos}")
    log("==== BOT END ====")

if __name__ == "__main__":
    main()
