import os
import re
import time
import logging
import requests
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
from google.auth.exceptions import GoogleAuthError
from urllib.parse import unquote, urlparse
import mimetypes
import uuid

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

# Base directory for saving media
BASE_DIR = "D:\\vid_img"

# Ensure base directory exists
os.makedirs(BASE_DIR, exist_ok=True)

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

def setup_google_sheets(sheet_name="Master Auto Swipe - Test ankur", worksheet_name="Ads Details", credentials_path="credentials.json"):
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
        
        return worksheet
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
    
    # Default based on URL pattern
    if "video" in url:
        return ".mp4"
    else:
        return ".jpg"

def is_video_url(url):
    """Determine if URL is likely a video"""
    video_patterns = ["video", ".mp4", ".mov", ".avi"]
    return any(pattern in url.lower() for pattern in video_patterns)

def download_media(url, save_path):
    """Download media from URL to specified path in original quality"""
    try:
        # Check if file already exists
        if os.path.exists(save_path):
            file_size = os.path.getsize(save_path) / (1024 * 1024)  # Convert to MB
            log_message(f"File already exists ({file_size:.2f} MB) at: {save_path}. Skipping download.")
            return True
        
        # For Facebook CDN, we'll use the original URL without modification
        # as modifying their URLs can trigger security measures
        original_quality_url = url
        
        # For non-Facebook URLs, we might try to improve quality if needed in the future
        # But for now, we'll use the original URL to avoid 403 errors
        
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
            # Add referer that matches Facebook domain
            headers["Referer"] = "https://www.facebook.com/"
            headers["Origin"] = "https://www.facebook.com"
            # Set cookies that might help with access
            headers["Cookie"] = "presence=C%7B%22t3%22%3A%5B%5D%2C%22utc3%22%3A1653862286005%2C%22v%22%3A1%7D"
        
        # Add special headers for video content
        if is_video_url(original_quality_url):
            headers["Range"] = "bytes=0-"  # Request the full video
        
        # Request the file with a session to handle cookies and redirects
        session = requests.Session()
        response = session.get(original_quality_url, headers=headers, stream=True, timeout=60)  # Longer timeout for videos
        response.raise_for_status()
        
        # Get content type for extension if needed
        content_type = response.headers.get('Content-Type')
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        
        # Save file
        with open(save_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        
        file_size = os.path.getsize(save_path) / (1024 * 1024)  # Convert to MB
        log_message(f"Downloaded original quality ({file_size:.2f} MB) to: {save_path}")
        return True
    except requests.exceptions.RequestException as e:
        log_message(f"Error downloading media: {e}", "error")
        return False
    except Exception as e:
        log_message(f"Unexpected error during download: {e}", "error")
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

def process_sheet_data():
    """Main function to process sheet data and download media"""
    try:
        log_message("Starting media download process")
        
        # Connect to Google Sheets
        worksheet = setup_google_sheets()
        
        # Get all values from the worksheet
        all_data = worksheet.get_all_values()
        
        if not all_data:
            log_message("No data found in the worksheet", "warning")
            return
        
        # Get headers
        headers = all_data[0]
        
        # Find required column indices
        # Note: Look for columns with exact names including trailing spaces based on memory
        media_url_idx = None
        page_name_idx = None
        update_time_idx = None
        library_id_idx = None
        ads_count_idx = None
        
        for idx, header in enumerate(headers):
            if header == "media_url":
                media_url_idx = idx
            elif header == "Page " or header.strip() == "Page":
                page_name_idx = idx
            elif header == "Last Update Time":
                update_time_idx = idx
            elif header == "Name of page":
                page_name_idx = idx
            elif header == "library_id":
                library_id_idx = idx
            # Check for ad count column
            elif header == "ads_count" or header.strip() == "ads_count":
                ads_count_idx = idx
        
        if media_url_idx is None:
            log_message("'media_url' column not found in the worksheet", "error")
            return
        
        if page_name_idx is None:
            log_message("'Page' or 'Name of page' column not found in the worksheet", "error")
            return
        
        if update_time_idx is None:
            log_message("'Last Update Time' column not found in the worksheet", "error")
            return
            
        if library_id_idx is None:
            log_message("Warning: 'library_id' column not found in the worksheet. Using random filenames instead.", "warning")
            
        if ads_count_idx is None:
            log_message("Warning: 'ads_count' column not found in the worksheet. Processing all rows regardless of ad count.", "warning")
        
        # Process each row (skip header row)
        total_rows = len(all_data) - 1
        successful_downloads = 0
        
        for row_idx, row in enumerate(all_data[1:], 1):
            try:
                if row_idx % 10 == 0 or row_idx == 1:
                    log_message(f"Processing row {row_idx}/{total_rows}")
                
                # Extract required data
                media_url = row[media_url_idx].strip() if media_url_idx < len(row) else ""
                page_name = row[page_name_idx].strip() if page_name_idx < len(row) else ""
                update_time = row[update_time_idx].strip() if update_time_idx < len(row) else ""
                
                # Get library_id if available
                library_id = None
                if library_id_idx is not None and library_id_idx < len(row):
                    library_id = row[library_id_idx].strip()
                
                # Check ads_count
                ads_count = 0
                if ads_count_idx is not None and ads_count_idx < len(row):
                    try:
                        ads_count_str = row[ads_count_idx].strip()
                        if ads_count_str and ads_count_str.replace('.', '', 1).isdigit():  # Handle decimal numbers
                            ads_count = float(ads_count_str)
                    except (ValueError, TypeError):
                        pass
                
                # Skip if ad count is less than 2
                if ads_count < 0:
                    log_message(f"Skipping row {row_idx}: ads_count ({ads_count}) is less than 2")
                    continue
                
                # Skip if any required field is missing
                if not media_url:
                    log_message(f"Missing media URL in row {row_idx}, skipping", "warning")
                    continue
                    
                if not page_name:
                    log_message(f"Missing page name in row {row_idx}, skipping", "warning")
                    continue
                    
                if not update_time:
                    log_message(f"Missing update time in row {row_idx}, skipping", "warning")
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
                
                # Create folder structure
                # Format: D:\vid_img\[Page Name]\clone\[Last Update Date]\[media file]
                folder_path = os.path.join(BASE_DIR, safe_page_name, "clone", safe_update_date)
                
                # Determine file extension based on URL
                file_ext = get_file_extension(media_url)
                
                # Generate filename using library_id if available, otherwise use random ID
                if library_id:
                    # Sanitize library_id for use as filename
                    safe_library_id = sanitize_filename(library_id)
                    file_name = f"{safe_library_id}{file_ext}"
                else:
                    file_name = f"No library id found"
                
                # Full save path
                save_path = os.path.join(folder_path, file_name)
                
                # Download the media
                if download_media(media_url, save_path):
                    successful_downloads += 1
                    log_message(f"Successfully downloaded media for {page_name}")
                    
                # Add a small delay to avoid overloading servers
                time.sleep(0.5)
                
            except Exception as e:
                log_message(f"Error processing row {row_idx}: {e}", "error")
        
        log_message(f"Process complete. Successfully downloaded {successful_downloads} out of {total_rows} media files.")
        
    except Exception as e:
        log_message(f"Error in media download process: {e}", "error")

# Run the script
if __name__ == "__main__":
    process_sheet_data()