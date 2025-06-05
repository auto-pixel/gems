#!/usr/bin/env python3
# Downloader.py

import os
import re
import time
import logging
import requests
import sys
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
from google.auth.exceptions import GoogleAuthError
from urllib.parse import unquote, urlparse
import mimetypes
import uuid
import dropbox
from dropbox.exceptions import ApiError, AuthError
from io import BytesIO
from dotenv import load_dotenv

def log_message(message, level="info"):
    """Log messages to both console and log file"""
    if level.lower() == "info":
        logging.info(message)
    elif level.lower() == "warning":
        logging.warning(message)
    elif level.lower() == "error":
        logging.error(message)
    elif level.lower() == "debug":
        logging.debug(message)
    else:
        logging.info(message)

# Load environment variables from .env file
load_dotenv()

# Set up logging
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"media_downloader_{datetime.now().strftime('%Y%m%d')}.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

# Dropbox configuration
DROPBOX_ACCESS_TOKEN = os.getenv('DROPBOX_ACCESS_TOKEN')
if not DROPBOX_ACCESS_TOKEN:
    raise ValueError("DROPBOX_ACCESS_TOKEN not found in environment variables. Please set it in your .env file")

# Remove any quotes or whitespace from the token
DROPBOX_ACCESS_TOKEN = DROPBOX_ACCESS_TOKEN.strip('"\'').strip()

DROPBOX_BASE_PATH = '/Swipe file automation'  # Base path in Dropbox

# Initialize Dropbox client
try:
    log_message("Attempting to connect to Dropbox...")
    dbx = dropbox.Dropbox(DROPBOX_ACCESS_TOKEN)
    # Test the connection
    account = dbx.users_get_current_account()
    log_message(f"Successfully connected to Dropbox as {account.name.display_name}")
except Exception as e:
    log_message(f"Error connecting to Dropbox: {str(e)}", "error")
    log_message("Please check your DROPBOX_ACCESS_TOKEN in the .env file", "error")
    sys.exit(1)

def file_exists_in_dropbox(path):
    """Check if a file exists in Dropbox"""
    try:
        dbx.files_get_metadata(path)
        return True
    except dropbox.exceptions.ApiError as e:
        # If the error is a "not_found" error, the file doesn't exist
        if isinstance(e.error, dropbox.files.GetMetadataError) and e.error.is_path() and e.error.get_path().is_not_found():
            return False
        # For other API errors, log and assume file doesn't exist to be safe
        log_message(f"Error checking if file exists: {e}", "error")
        return False

def folder_exists_in_dropbox(path):
    """Check if a folder exists in Dropbox"""
    try:
        metadata = dbx.files_get_metadata(path)
        return True
    except dropbox.exceptions.ApiError as e:
        # If the error is a "not_found" error, the folder doesn't exist
        if isinstance(e.error, dropbox.files.GetMetadataError) and e.error.is_path() and e.error.get_path().is_not_found():
            return False
        # For other API errors, log and assume folder doesn't exist to be safe
        log_message(f"Error checking if folder exists: {e}", "error")
        return False

def create_folder_if_not_exists(path):
    """Create a folder in Dropbox if it doesn't exist"""
    if not folder_exists_in_dropbox(path):
        try:
            dbx.files_create_folder_v2(path)
            log_message(f"Created folder: {path}")
            return True
        except Exception as e:
            log_message(f"Error creating folder {path}: {e}", "error")
            return False
    return True

def setup_google_sheets(sheet_name="Master Auto Swipe - Test ankur", worksheet_name="Transcript", credentials_path="credentials.json"):
    """Connect to Google Sheets and return the specified worksheet"""
    try:
        # Scopes required for Google Sheets
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        
        # Authenticate using service account credentials
        credentials = Credentials.from_service_account_file(credentials_path, scopes=scope)
        client = gspread.authorize(credentials)
        
        # Open the spreadsheet and worksheet
        log_message(f"Opening spreadsheet: {sheet_name}")
        spreadsheet = client.open(sheet_name)
        
        log_message(f"Opening worksheet: {worksheet_name}")
        worksheet = spreadsheet.worksheet(worksheet_name)
        
        return worksheet, spreadsheet
    except GoogleAuthError as e:
        log_message(f"Google authentication error: {e}", "error")
        raise
    except Exception as e:
        log_message(f"Error setting up Google Sheets: {e}", "error")
        raise

def get_file_extension(url, content_type=None):
    """Determine file extension from URL or content type"""
    # Try to get extension from URL
    parsed_url = urlparse(url)
    path = unquote(parsed_url.path)
    
    # Check if there's a file extension in the URL
    ext = os.path.splitext(path)[1].lower()
    if ext and ext in [".jpg", ".jpeg", ".png", ".gif", ".mp4", ".mov", ".avi"]:
        return ext
    
    # If no extension in URL, try content type
    if content_type:
        guess_ext = mimetypes.guess_extension(content_type)
        if guess_ext:
            return guess_ext
    
    # Default based on URL pattern or type
    if is_video_url(url):
        return ".mp4"
    else:
        return ".jpg"

def is_video_url(url):
    """Determine if URL is likely a video"""
    video_patterns = ["video", ".mp4", ".mov", ".avi"]
    return any(pattern in url.lower() for pattern in video_patterns)

def get_media_type(url, media_type_value=None):
    """Determine if media is video or image based on URL and/or media_type column"""
    # If media_type_value is provided and valid, use it
    if media_type_value:
        media_type_lower = media_type_value.lower().strip()
        if media_type_lower in ["video", "image"]:
            return media_type_lower
    
    # Otherwise determine from URL
    if is_video_url(url):
        return "video"
    else:
        return "image"

def download_media(url, dropbox_path):
    """Download media from URL and upload to Dropbox"""
    try:
        # First check if file already exists in Dropbox
        if file_exists_in_dropbox(dropbox_path):
            log_message(f"File already exists in Dropbox: {dropbox_path}, skipping download")
            return True
            
        log_message(f"Uploading file to Dropbox: {dropbox_path}")
        
        # For Facebook CDN, we'll use the original URL without modification
        original_quality_url = url
        
        log_message(f"Downloading original quality from: {original_quality_url}")
        
        # Create headers with common browser user-agent and request high quality
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,video/mp4,video/*;q=0.9,*/*;q=0.8",
            "Accept-Encoding": "identity",  # No compression to get original quality
            "Connection": "keep-alive",
            "Cache-Control": "no-cache"
        }
        
        # Add specific headers for Facebook CDN to avoid 403 errors
        if 'fbcdn.net' in original_quality_url or 'facebook.com' in original_quality_url:
            headers["Referer"] = "https://www.facebook.com/"
            headers["Origin"] = "https://www.facebook.com"
            headers["Cookie"] = "presence=C%7B%22t3%22%3A%5B%5D%2C%22utc3%22%3A1653862286005%2C%22v%22%3A1%7D"
        
        # Add special headers for video content
        if is_video_url(original_quality_url):
            headers["Range"] = "bytes=0-"  # Request the full video
        
        # Download the file to memory
        session = requests.Session()
        response = session.get(original_quality_url, headers=headers, stream=True, timeout=60)
        response.raise_for_status()
        
        # Get content type for extension if needed
        content_type = response.headers.get('Content-Type')
        
        # Upload to Dropbox in chunks
        CHUNK_SIZE = 4 * 1024 * 1024  # 4MB chunks for large files
        
        if int(response.headers.get('content-length', 0)) > CHUNK_SIZE:
            # Large file, use upload session
            upload_session = dbx.files_upload_session_start(b"")
            cursor = dropbox.files.UploadSessionCursor(session_id=upload_session.session_id, offset=0)
            commit = dropbox.files.CommitInfo(path=dropbox_path, mode=dropbox.files.WriteMode('add'))
            
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                if chunk:
                    dbx.files_upload_session_append_v2(chunk, cursor)
                    cursor.offset += len(chunk)
            
            dbx.files_upload_session_finish(b"", cursor, commit)
        else:
            # Small file, direct upload
            dbx.files_upload(response.content, dropbox_path, mode=dropbox.files.WriteMode('add'))
        
        file_size = int(response.headers.get('content-length', 0)) / (1024 * 1024)  # Convert to MB
        log_message(f"Uploaded to Dropbox ({file_size:.2f} MB) at: {dropbox_path}")
        return True
        
    except requests.exceptions.RequestException as e:
        log_message(f"Error downloading media: {e}", "error")
        return False
    except dropbox.exceptions.ApiError as e:
        if isinstance(e.error, dropbox.files.UploadError) and e.error.is_path() and e.error.get_path().is_conflict():
            # This is a conflict error which means file already exists
            log_message(f"File already exists (caught during upload): {dropbox_path}")
            return True
        log_message(f"Dropbox API error: {e}", "error")
        return False
    except Exception as e:
        log_message(f"Unexpected error during download/upload: {e}", "error")
        return False

def sanitize_filename(filename):
    """Make a filename safe for all operating systems"""
    # Replace invalid characters
    invalid_chars = r'[<>:"/\\|?*]'
    sanitized = re.sub(invalid_chars, '_', filename)
    
    # Trim length if too long
    if len(sanitized) > 150:
        sanitized = sanitized[:147] + '...'
    
    return sanitized or f"unnamed_{uuid.uuid4().hex[:8]}"

def update_progress_percentage(current, total):
    """Print a progress bar for the media downloader execution."""
    if total > 0:
        progress_percentage = (current / total) * 100
        progress_bar = f"[{'=' * int(progress_percentage / 2)}{' ' * (50 - int(progress_percentage / 2))}] {progress_percentage:.1f}%"
        
        # Check if running from master script to use the appropriate format
        running_from_master = os.environ.get('RUNNING_FROM_MASTER_SCRIPT') == 'true'
        
        if running_from_master:
            # When running from master script, use GitHub Actions group format
            print(f"\n::group::PROGRESS UPDATE [Downloader]\n{progress_bar}\nProcessed: {current}/{total} media files\n::endgroup::")
        else:
            # When running standalone, use a simpler format that's still clear
            print(f"\nPROGRESS [Downloader]: {progress_percentage:.1f}% ({current}/{total} media files)\n{progress_bar}")
            
        # This ensures the progress is visible in GitHub Actions logs
        sys.stdout.flush()

def process_worksheet(worksheet_name):
    """Process a specific worksheet and download media"""
    try:
        log_message(f"Starting media download process for worksheet: {worksheet_name}")
        
        # Connect to Google Sheets
        worksheet, spreadsheet = setup_google_sheets(worksheet_name=worksheet_name)
        
        # Get all values from the worksheet
        all_data = worksheet.get_all_values()
        
        if not all_data:
            log_message(f"No data found in the {worksheet_name} worksheet", "warning")
            return 0, 0, 0
        
        # Get headers
        headers = all_data[0]
        
        # Find required column indices
        media_url_idx = None
        page_name_idx = None
        update_time_idx = None
        library_id_idx = None
        ads_count_idx = None
        media_type_idx = None
        
        for idx, header in enumerate(headers):
            header_lower = header.lower().strip()
            if header_lower == "media_url":
                media_url_idx = idx
            elif header_lower in ["page", "name of page"]:
                page_name_idx = idx
            elif header_lower == "last update time":
                update_time_idx = idx
            elif header_lower == "library_id":
                library_id_idx = idx
            elif header_lower in ["ads_count", "no. of ads"]:
                ads_count_idx = idx
            elif header_lower in ["media_type", "type"]:
                media_type_idx = idx
        
        if media_url_idx is None:
            log_message(f"'media_url' column not found in the {worksheet_name} worksheet", "error")
            return 0, 0, 0
        
        if page_name_idx is None:
            log_message(f"'Page' or 'Name of page' column not found in the {worksheet_name} worksheet", "error")
            return 0, 0, 0
        
        if update_time_idx is None:
            log_message(f"'Last Update Time' column not found in the {worksheet_name} worksheet", "error")
            return 0, 0, 0
            
        if library_id_idx is None:
            log_message(f"Warning: 'library_id' column not found in the {worksheet_name} worksheet. Using random filenames instead.", "warning")
            
        if ads_count_idx is None:
            log_message(f"Warning: 'ads_count' column not found in the {worksheet_name} worksheet. Processing all rows regardless of ad count.", "warning")
        
        # Process each row (skip header row)
        total_rows = len(all_data) - 1
        successful_downloads = 0
        processed_rows = 0
        skipped_files = 0
        
        # Initialize progress tracking
        update_progress_percentage(processed_rows, total_rows)
        
        # Track which folders we've already checked/created
        checked_folders = {}
        
        for row_idx, row in enumerate(all_data[1:], 1):
            try:
                if row_idx % 10 == 0 or row_idx == 1:
                    log_message(f"Processing row {row_idx}/{total_rows} from {worksheet_name}")
                    # Also update progress for GitHub Actions
                    update_progress_percentage(processed_rows, total_rows)
                
                # Extract required data
                media_url = row[media_url_idx].strip() if media_url_idx < len(row) else ""
                page_name = row[page_name_idx].strip() if page_name_idx < len(row) else ""
                update_time = row[update_time_idx].strip() if update_time_idx < len(row) else ""
                
                # Get library_id if available
                library_id = None
                if library_id_idx is not None and library_id_idx < len(row):
                    library_id = row[library_id_idx].strip()
                
                # Get media_type if available
                media_type_value = None
                if media_type_idx is not None and media_type_idx < len(row):
                    media_type_value = row[media_type_idx].strip()
                
                # Check ads_count
                ads_count = 0
                if ads_count_idx is not None and ads_count_idx < len(row):
                    try:
                        ads_count_str = row[ads_count_idx].strip()
                        # Extract only digits if there's text
                        digits_only = re.sub(r'[^0-9]', '', str(ads_count_str))
                        if digits_only:
                            ads_count = int(digits_only)
                    except (ValueError, TypeError):
                        pass
                
                # For Winning Creative Image worksheet, only process rows with ads_count > 10
                if worksheet_name == "Winning Creative Image" and ads_count <= 10:
                    log_message(f"Skipping row {row_idx} from {worksheet_name}: ads_count ({ads_count}) is less than or equal to 10")
                    processed_rows += 1
                    continue
                
                # Skip if any required field is missing
                if not media_url:
                    log_message(f"Missing media URL in row {row_idx} from {worksheet_name}, skipping", "warning")
                    processed_rows += 1
                    continue
                    
                if not page_name:
                    log_message(f"Missing page name in row {row_idx} from {worksheet_name}, skipping", "warning")
                    processed_rows += 1
                    continue
                    
                if not update_time:
                    log_message(f"Missing update time in row {row_idx} from {worksheet_name}, skipping", "warning")
                    processed_rows += 1
                    continue
                
                # Determine media type (video or image)
                media_type = get_media_type(media_url, media_type_value)
                
                # For Transcript worksheet, only process videos
                if worksheet_name == "Transcript" and media_type != "video":
                    log_message(f"Skipping non-video media in Transcript worksheet: {media_url}", "info")
                    processed_rows += 1
                    continue
                
                # For Winning Creative Image worksheet, only process images
                if worksheet_name == "Winning Creative Image" and media_type != "image":
                    log_message(f"Skipping non-image media in Winning Creative Image worksheet: {media_url}", "info")
                    processed_rows += 1
                    continue
                
                # Sanitize names for safe folder creation
                safe_page_name = sanitize_filename(page_name)
                
                # Extract only the date part from the update_time (ignore time)
                # Try common date formats
                date_part = update_time
                try:
                    # Try parsing different date formats
                    date_formats = [
                        '%Y-%m-%d %H:%M:%S',
                        '%m/%d/%Y %H:%M:%S',
                        '%d/%m/%Y %H:%M:%S',
                        '%Y/%m/%d %H:%M:%S',
                        '%Y-%m-%d',
                        '%m/%d/%Y',
                        '%d/%m/%Y'
                    ]
                    
                    for date_format in date_formats:
                        try:
                            # Try to parse using this format
                            parsed_date = datetime.strptime(update_time, date_format)
                            # If parsing succeeds, format as YYYY-MM-DD
                            date_part = parsed_date.strftime('%Y-%m-%d')
                            break
                        except ValueError:
                            continue
                except Exception as e:
                    log_message(f"Error parsing date from '{update_time}': {e}. Using full string.", "warning")
                
                safe_update_date = sanitize_filename(date_part)
                
                # Create Dropbox path structure: name -> date -> image/video
                file_ext = get_file_extension(media_url)
                
                # Generate filename using library_id if available, otherwise use random ID
                if library_id:
                    safe_library_id = sanitize_filename(library_id)
                    file_name = f"{safe_library_id}{file_ext}"
                else:
                    file_name = f"{uuid.uuid4().hex}{file_ext}"  # Use random UUID if no library_id
                
                # Create folder structure: base/page_name/date/media_type
                page_folder_path = f"{DROPBOX_BASE_PATH}/{safe_page_name}"
                date_folder_path = f"{page_folder_path}/{safe_update_date}"
                media_type_folder_path = f"{date_folder_path}/{media_type}"
                
                # Check and create folders if needed
                folder_key = f"{safe_page_name}:{safe_update_date}:{media_type}"
                
                if folder_key not in checked_folders:
                    # Create each folder in the path if it doesn't exist
                    if not create_folder_if_not_exists(page_folder_path):
                        log_message(f"Failed to create page folder: {page_folder_path}", "error")
                        processed_rows += 1
                        continue
                        
                    if not create_folder_if_not_exists(date_folder_path):
                        log_message(f"Failed to create date folder: {date_folder_path}", "error")
                        processed_rows += 1
                        continue
                        
                    if not create_folder_if_not_exists(media_type_folder_path):
                        log_message(f"Failed to create media type folder: {media_type_folder_path}", "error")
                        processed_rows += 1
                        continue
                        
                    checked_folders[folder_key] = True
                
                # Construct full Dropbox path
                dropbox_path = f"{media_type_folder_path}/{file_name}"
                
                # Check if file already exists
                if file_exists_in_dropbox(dropbox_path):
                    log_message(f"File already exists: {dropbox_path}, skipping")
                    skipped_files += 1
                    processed_rows += 1
                    continue
                
                # Download and upload the media to Dropbox
                if download_media(media_url, dropbox_path):
                    successful_downloads += 1
                    log_message(f"Successfully downloaded {media_type} for {page_name}")
                    
                # Add a small delay to avoid overloading servers
                time.sleep(0.5)
                
                # Update progress after each row
                processed_rows += 1
                update_progress_percentage(processed_rows, total_rows)
                
            except Exception as e:
                log_message(f"Error processing row {row_idx} from {worksheet_name}: {e}", "error")
                # Update progress even for failed items
                processed_rows += 1
                update_progress_percentage(processed_rows, total_rows)
        
        return successful_downloads, skipped_files, total_rows
        
    except Exception as e:
        log_message(f"Error processing {worksheet_name} worksheet: {e}", "error")
        return 0, 0, 0

def process_sheet_data():
    """Main function to process both worksheets and download media"""
    try:
        log_message("Starting media download process")
        
        # Process Transcript worksheet (for videos)
        log_message("Processing Transcript worksheet for videos...")
        video_downloads, video_skipped, video_total = process_worksheet("Transcript")
        
        # Process Winning Creative Image worksheet (for images)
        log_message("Processing Winning Creative Image worksheet for images...")
        image_downloads, image_skipped, image_total = process_worksheet("Winning Creative Image")
        
        # Summarize results
        total_downloads = video_downloads + image_downloads
        total_skipped = video_skipped + image_skipped
        total_processed = video_total + image_total
        
        log_message(f"Process complete. Results summary:")
        log_message(f"- Videos: Downloaded {video_downloads} out of {video_total} from Transcript worksheet")
        log_message(f"- Images: Downloaded {image_downloads} out of {image_total} from Winning Creative Image worksheet")
        log_message(f"- Total: Successfully downloaded {total_downloads} out of {total_processed} media files")
        log_message(f"- Skipped {total_skipped} files because they already existed in Dropbox")
        
    except Exception as e:
        log_message(f"Error in media download process: {e}", "error")

# Run the script
if __name__ == "__main__":
    process_sheet_data()
