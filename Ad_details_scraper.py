from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.common.action_chains import ActionChains
from webdriver_manager.firefox import GeckoDriverManager
import time
import re
from datetime import datetime
from urllib.parse import unquote, urlparse
import json
import gspread
from google.oauth2.service_account import Credentials
from google.auth.exceptions import GoogleAuthError
import logging
import os
import sys
import random
from datetime import datetime
import concurrent.futures
import threading
import queue

# Import anti-detection utilities
from fb_antidetect_utils import (
    ProxyManager,
    create_stealth_driver,
    perform_human_like_scroll,
    simulate_random_mouse_movements,
    add_random_delays,
    get_current_ip
)

# Set up logging system
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"fb_scraper_ads_details.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

# Thread-local storage for thread-specific data
thread_local = threading.local()

# Function to log messages (prints to console and writes to log file)
def custom_print(message, level=None, thread_id=None):
    """Log a message with timestamp to both console and log file"""
    thread_prefix = f"[Thread-{thread_id}] " if thread_id is not None else ""
    msg = f"{thread_prefix}{message}"
    
    # Default to info level if no level provided
    if level is None:
        logging.info(msg)
    elif level.lower() == "info":
        logging.info(msg)
    elif level.lower() == "warning":
        logging.warning(msg)
    elif level.lower() == "error":
        logging.error(msg)
    elif level.lower() == "debug":
        logging.debug(msg)
    else:
        logging.info(msg)

# Platform identification mapping
PLATFORM_MAPPING = {
    ("https://static.xx.fbcdn.net/rsrc.php/v4/yV/r/OLar8kmsCmm.png", "0px -1188px"): "Facebook",
    ("https://static.xx.fbcdn.net/rsrc.php/v4/yV/r/OLar8kmsCmm.png", "0px -1201px"): "Instagram",
    ("https://static.xx.fbcdn.net/rsrc.php/v4/yO/r/ZuVkzM77JQ-.png", "-68px -189px"): "Audience Network",
    ("https://static.xx.fbcdn.net/rsrc.php/v4/yO/r/ZuVkzM77JQ-.png", "-51px -189px"): "Messenger",
    ("https://static.xx.fbcdn.net/rsrc.php/v4/abc/xyz.png", "-100px -200px"): "Messenger",
    ("https://static.xx.fbcdn.net/rsrc.php/v4/y5/r/7Ia52m_bDk0.png", "-246px -280px"): "Messenger",  # Added based on new icon format
    ("https://static.xx.fbcdn.net/rsrc.php/v4/yO/r/ZuVkzM77JQ-.png", "-34px -189px"): "WhatsApp",
    ("https://static.xx.fbcdn.net/rsrc.php/v4/yO/r/ZuVkzM77JQ-.png", "-56px -206px"): "Financial products and services",
    ("https://static.xx.fbcdn.net/rsrc.php/v4/yV/r/OLar8kmsCmm.png", "0px -1214px"): "Thread"
}
CATEGORY_MAPPING = {
    ("https://static.xx.fbcdn.net/rsrc.php/v4/y5/r/7Ia52m_bDk0.png", "-189px -384px"): "Employment",
    ("https://static.xx.fbcdn.net/rsrc.php/v4/yO/r/ZuVkzM77JQ-.png", "-56px -206px"): "Financial products and services",
}

# Global shared data structures with locks
url_queue = queue.Queue()
processed_urls = set()
processed_urls_lock = threading.Lock()
all_ads_data = {}
all_ads_data_lock = threading.Lock()
progress_update_lock = threading.Lock()

# Configure proxy manager with enhanced proxy handling
proxy_file = "proxies.txt"  # Proxy file in ip:port format, one per line
proxy_manager = None
use_proxies = True  # Set to False to temporarily disable proxy usage

if os.path.exists(proxy_file) and use_proxies:
    custom_print("Initializing proxy manager with proxies from file")
    try:
        # Count the number of proxies in the file
        with open(proxy_file, 'r') as f:
            proxy_count = sum(1 for line in f if line.strip() and not line.strip().startswith('#'))
        
        if proxy_count > 0:
            custom_print(f"Found {proxy_count} proxies in {proxy_file}")
            proxy_manager = ProxyManager(proxy_file=proxy_file)
        else:
            custom_print("No usable proxies found in proxy file. Will run without proxies.", "warning")
    except Exception as e:
        custom_print(f"Error loading proxies: {e}", "error")
        custom_print("Will run without proxies due to error.", "warning")
else:
    custom_print("No proxy file found or proxies disabled. Will run without proxies.", "warning")

# Google Sheets setup
def setup_google_sheets(sheet_name="Master Auto Swipe - Test ankur", worksheet_name="Ads Details", credentials_path="credentials.json"):
    """Connect to Google Sheets and return the specified worksheet"""
    try:
        # Scopes required for Google Sheets
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        
        # Authenticate using service account credentials
        credentials = Credentials.from_service_account_file(credentials_path, scopes=scope)
        
        # Create a gspread client
        gc = gspread.authorize(credentials)
        
        # Open the spreadsheet by name
        spreadsheet = gc.open(sheet_name)
        custom_print(f"Successfully connected to spreadsheet: {sheet_name}")
        
        # Access the specified worksheet
        worksheet = spreadsheet.worksheet(worksheet_name)
        custom_print(f"Successfully accessed worksheet: {worksheet_name}")
        
        return worksheet
    except FileNotFoundError:
        custom_print(f"Error: Credentials file '{credentials_path}' not found.")
        return None
    except GoogleAuthError as e:
        custom_print(f"Authentication error: {e}")
        return None
    except Exception as e:
        custom_print(f"Error connecting to Google Sheets: {e}")
        return None

# Function to extract URLs from Milk worksheet
def extract_urls_from_milk_worksheet(worksheet):
    """Extract URLs and page names from the Milk worksheet"""
    if not worksheet:
        custom_print("Invalid worksheet provided", "error")
        return [], {}
        
    try:
        # Get all headers (first row)
        headers = worksheet.row_values(1)
        custom_print(f"Found headers in Milk worksheet: {headers}")
        
        # Find relevant column indices
        page_col_idx = None
        transperancy_col_idx = None
        
        for i, header in enumerate(headers):
            # Check for exact matches including trailing spaces (per memory)
            if header == "Page " or header == "Page":
                page_col_idx = i + 1  # gspread is 1-indexed
                custom_print(f"Found Page column at index {page_col_idx}: '{header}'")
            
            # Check for Page Transperancy with trailing space 
            if header == "Page Transperancy " or header == "Page Transperancy":
                transperancy_col_idx = i + 1  # gspread is 1-indexed
                custom_print(f"Found Page Transperancy column at index {transperancy_col_idx}: '{header}'")
            # Fallback to case-insensitive matching if needed
            elif header.lower().strip() in ["page transperancy", "page transparency"]:
                transperancy_col_idx = i + 1  # gspread is 1-indexed
                custom_print(f"Found Page Transperancy column at index {transperancy_col_idx} via fallback: '{header}'")
        
        if not page_col_idx or not transperancy_col_idx:
            custom_print("Could not find required columns in the Milk worksheet!", "warning")
            return [], {}
            
        # Get all records with transparency links
        all_values = worksheet.get_all_values()
        urls = []
        page_names = {}
        
        # Skip header row
        for row in all_values[1:]:
            # Only proceed if we have valid data in both columns
            if len(row) >= max(page_col_idx, transperancy_col_idx):
                page_name = row[page_col_idx - 1].strip()  # Convert to 0-indexed
                transparency_url = row[transperancy_col_idx - 1].strip()
                
                if transparency_url and "facebook.com" in transparency_url:
                    # Check if this is an ad library URL
                    if "ads/library" not in transparency_url:
                        # Convert transparency URL to ad library URL
                        ad_library_url = convert_to_ad_library_url(transparency_url)
                        if ad_library_url:
                            urls.append(ad_library_url)
                            # Also store the page name
                            page_names[ad_library_url] = page_name
                            custom_print(f"Extracted Ad Library URL for {page_name}: {ad_library_url}")
                    else:
                        # Already an ad library URL
                        urls.append(transparency_url)
                        page_names[transparency_url] = page_name
                        custom_print(f"Found existing Ad Library URL for {page_name}")
        
        custom_print(f"Extracted {len(urls)} Ad Library URLs from Milk worksheet")
        return urls, page_names
        
    except Exception as e:
        custom_print(f"Error extracting URLs from Milk worksheet: {e}", "error")
        return [], {}

# Function to convert transparency URL to ad library URL
def convert_to_ad_library_url(transparency_url):
    """Convert a Facebook transparency URL to an Ad Library URL"""
    try:
        # Skip URLs that are already Ad Library URLs
        if "facebook.com/ads/library" in transparency_url:
            return transparency_url
        
        # Extract page ID from URL
        if "/page_transparency/?page_id=" in transparency_url:
            page_id = transparency_url.split("/page_transparency/?page_id=")[1].split("&")[0]
        else:
            custom_print(f"Could not extract page ID from URL: {transparency_url}", "warning")
            return None
            
        # Construct Ad Library URL
        ad_lib_url = f"https://www.facebook.com/ads/library/?active_status=active&ad_type=all&country=ALL&view_all_page_id={page_id}&sort_data[direction]=desc&sort_data[mode]=relevancy_monthly_grouped"
        return ad_lib_url
    except Exception as e:
        custom_print(f"Error converting transparency URL {transparency_url}: {e}", "error")
        return None

# Helper function to parse URL parameters
def parse_url_params(url):
    """Parse URL parameters to extract page_id and other values"""
    params = {}
    try:
        if "view_all_page_id=" in url:
            page_id_match = re.search(r'view_all_page_id=(\d+)', url)
            if page_id_match:
                params['view_all_page_id'] = page_id_match.group(1)
        
        if "page_id=" in url:
            page_id_match = re.search(r'page_id=(\d+)', url)
            if page_id_match:
                params['page_id'] = page_id_match.group(1)
    except Exception:
        pass
    
    return params

# Function to update progress percentage
def update_progress_percentage():
    with progress_update_lock:
        if total_urls > 0:
            progress_percentage = (len(processed_urls) / total_urls) * 100
            progress_bar = f"[{'#' * int(progress_percentage / 2)}{'-' * (50 - int(progress_percentage / 2))}] {progress_percentage:.1f}%"
            
            # Check if running from master script to use the appropriate format
            running_from_master = os.environ.get('RUNNING_FROM_MASTER_SCRIPT') == 'true'
            
            if running_from_master:
                # When running from master script, simplified output for central display
                print(f"PROGRESS [Ad_details_scraper]: {progress_bar} ({len(processed_urls)}/{total_urls} URLs)")
                sys.stdout.flush()
            else:
                # When running standalone, use a simpler format that's still clear
                print(f"\nPROGRESS [Ad_details_scraper]: {progress_percentage:.1f}% ({len(processed_urls)}/{total_urls} URLs)\n{progress_bar}")
                
            # This ensures the progress is visible in GitHub Actions logs
            sys.stdout.flush()

# Worker function for processing URLs in parallel
def worker_process_url(thread_id):
    thread_local.thread_id = thread_id
    thread_local.request_count = 0
    
    # Create thread-specific browser instance
    try:
        thread_local.driver = create_stealth_driver(
            use_proxy=(proxy_manager is not None),
            proxy_manager=proxy_manager,
            headless=True
        )
        thread_local.wait = WebDriverWait(thread_local.driver, random.uniform(5, 8))
        custom_print(f"Worker {thread_id}: Browser initialized successfully", thread_id=thread_id)
    except Exception as e:
        custom_print(f"Worker {thread_id}: Failed to initialize browser: {e}", "error", thread_id)
        return

    while True:
        try:
            # Get next URL from queue with a timeout
            try:
                url_data = url_queue.get(timeout=5)
            except queue.Empty:
                custom_print(f"Worker {thread_id}: No more URLs to process, exiting", thread_id=thread_id)
                break
                
            url, row_index = url_data
            
            custom_print(f"Worker {thread_id}: Processing URL: {url}", thread_id=thread_id)
            
            # Process the URL (scrape ads)
            process_single_url(url, row_index, thread_id)
            
            # Mark URL as processed
            with processed_urls_lock:
                processed_urls.add(url)
            
            # Update progress
            update_progress_percentage()
            
            # Mark task as done
            url_queue.task_done()
            
        except Exception as e:
            custom_print(f"Worker {thread_id}: Error processing URL: {e}", "error", thread_id)
            # Still mark task as done to avoid blocking
            try:
                url_queue.task_done()
            except:
                pass
    
    # Close browser when done
    try:
        thread_local.driver.quit()
        custom_print(f"Worker {thread_id}: Browser closed", thread_id=thread_id)
    except:
        pass

# Function to process a single URL (to be run in worker threads)
def process_single_url(url, row_index, thread_id):
    # Use thread-local variables
    driver = thread_local.driver
    wait = thread_local.wait
    
    # Add retry mechanism with proxy rotation
    max_retries = 3
    retry_count = 0
    success = False
    
    while retry_count < max_retries and not success:
        try:
            # Try to load the page with additional safety measures
            custom_print(f"Worker {thread_id}: Loading page (attempt {retry_count + 1}/{max_retries})...", thread_id=thread_id)
            
            # Increment request count
            thread_local.request_count += 1
            
            # Rotate IP every 5-8 requests if proxies are available (faster rotation)
            if proxy_manager and thread_local.request_count >= random.randint(5, 8):
                custom_print(f"Worker {thread_id}: Rotation limit reached. Rotating IP for safety...", thread_id=thread_id)
                # Close current driver
                driver.quit()
                
                # Create new driver with different proxy
                thread_local.driver = create_stealth_driver(
                    use_proxy=True,
                    proxy_manager=proxy_manager,
                    headless=True
                )
                driver = thread_local.driver
                thread_local.wait = WebDriverWait(driver, random.uniform(5, 8))
                wait = thread_local.wait
                thread_local.request_count = 0  # Reset counter
            
            # Load the page directly
            driver.get(url)
            
            # Minimal wait for page load
            time.sleep(random.uniform(0.3, 0.6))
                
            # Check if page loaded properly
            try:
                # Wait for an element that should be present on a properly loaded page with shorter timeout
                WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'x6s0dn4')]"))
                )
                success = True
                custom_print(f"Worker {thread_id}: Page loaded successfully!", thread_id=thread_id)
            except (TimeoutException, NoSuchElementException):
                custom_print(f"Worker {thread_id}: Page didn't load properly", "warning", thread_id)
                raise Exception("Page failed to load properly")
                
        except Exception as e:
            retry_count += 1
            custom_print(f"Worker {thread_id}: Error loading page: {e}", "error", thread_id)
            
            if proxy_manager and retry_count < max_retries:
                # Rotate proxy before retrying
                custom_print(f"Worker {thread_id}: Rotating proxy and retrying...", "warning", thread_id)
                
                # Close current driver
                driver.quit()
                
                # Create new driver with different proxy
                thread_local.driver = create_stealth_driver(
                    use_proxy=True,
                    proxy_manager=proxy_manager,
                    headless=True
                )
                driver = thread_local.driver
                thread_local.wait = WebDriverWait(driver, random.uniform(5, 8))
                wait = thread_local.wait
            elif retry_count >= max_retries:
                custom_print(f"Worker {thread_id}: Maximum retry attempts reached. Continuing with direct connection.", "warning", thread_id)
                
                # Fall back to direct connection if all proxies fail
                if proxy_manager:
                    custom_print(f"Worker {thread_id}: Attempting with direct connection (no proxy)...", "warning", thread_id)
                    driver.quit()
                    thread_local.driver = create_stealth_driver(use_proxy=False, headless=True)
                    driver = thread_local.driver
                    thread_local.wait = WebDriverWait(driver, random.uniform(5, 8))
                    wait = thread_local.wait
                    
                # Try one last time with direct connection
                try:
                    driver.get(url)
                    time.sleep(random.uniform(0.8, 1.5))
                    success = True
                except Exception as e:
                    custom_print(f"Worker {thread_id}: Failed to load with direct connection: {e}", "error", thread_id)
                    custom_print(f"Worker {thread_id}: Skipping URL {url}", "error", thread_id)
                    return
    
    # If we couldn't load the page after all retries, skip this URL
    if not success:
        custom_print(f"Worker {thread_id}: Failed to load URL {url} after multiple attempts. Skipping.", "error", thread_id)
        return
        
    # Extract the ad count fast
    custom_print(f"Worker {thread_id}: Extracting ad count from the page...", thread_id=thread_id)
    ad_count = None
    ad_count_text = ""
    
    try:
        # First try fast extraction with shorter timeout
        try:
            ad_count_element = WebDriverWait(driver, 2).until(
                EC.presence_of_element_located((By.XPATH, "//div[@role='heading'][contains(text(), 'results') or contains(text(), 'result')]"))
            )
            ad_count_text = ad_count_element.text.strip()
            
            # Extract the numeric part using regex
            matches = re.search(r'~?(\d+(?:,\d+)?)', ad_count_text)
            if matches:
                # Remove commas and convert to int
                ad_count = int(matches.group(1).replace(',', ''))
        except (TimeoutException, NoSuchElementException):
            # Try JavaScript as fast alternative
            try:
                ad_count_text = driver.execute_script("""
                    const elements = document.querySelectorAll('div');
                    for (const el of elements) {
                        const text = el.textContent.trim();
                        if (text.includes('results') || text.includes('result')) {
                            return text;
                        }
                    }
                    return null;
                """)
                
                if ad_count_text:
                    matches = re.search(r'~?(\d+(?:,\d+)?)', ad_count_text)
                    if matches:
                        ad_count = int(matches.group(1).replace(',', ''))
            except Exception:
                pass
    except Exception as e:
        custom_print(f"Worker {thread_id}: Error extracting ad count: {e}", "error", thread_id)
    
    # Process URL parameters
    url_params = parse_url_params(url)
    
    # If ads present, efficiently scroll to load them
    if ad_count and ad_count > 0:
        custom_print(f"Worker {thread_id}: Found approximately {ad_count} ads, scrolling to load them...", thread_id=thread_id)
        
        # Fast scroller function optimized for speed
        def fast_scroll():
            scroll_count = 0
            last_height = driver.execute_script("return document.body.scrollHeight")
            max_scrolls = min(100, ad_count // 2 + 15)  # Optimize scroll limit based on ad count
            
            # Fast scroll loop
            while scroll_count < max_scrolls:
                # Use fast scroll method
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
                scroll_count += 1
                
                # Minimal wait between scrolls
                time.sleep(random.uniform(0.1, 0.3))
                
                # Check for end of content
                new_height = driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    # Try a bit more to see if more content loads
                    time.sleep(0.5)
                    new_height = driver.execute_script("return document.body.scrollHeight")
                    if new_height == last_height:
                        break
                
                last_height = new_height
                
                # Every few scrolls, check for "end of results" indicator
                if scroll_count % 5 == 0:
                    try:
                        end_divs = driver.find_elements(By.XPATH, 
                            "//div[contains(text(), 'End of results') or contains(text(), 'No more results')]")
                        if end_divs and len(end_divs) > 0:
                            break
                    except:
                        pass
            
            return scroll_count
        
        # Perform the fast scrolling
        scrolls_performed = fast_scroll()
        custom_print(f"Worker {thread_id}: Completed {scrolls_performed} scrolls to load ads", thread_id=thread_id)
    
    # Extract and process ads
    target_class_2 = "xrvj5dj x18m771g x1p5oq8j xbxaen2 x18d9i69 x1u72gb5 xtqikln x1na6gtj x1jr1mh3 xm39877 x7sq92a xxy4fzi"
    
    try:
        # Use direct JavaScript to get ads faster
        divs_2 = driver.find_elements(By.CSS_SELECTOR, f'div[class="{target_class_2}"]')
        custom_print(f"Worker {thread_id}: Found {len(divs_2)} ad groups", thread_id=thread_id)
        
        # Fast extraction of ads data
        ads_data = {}
        processed_count = 0
        
        # Process ads in smaller batches for speed
        for i, div in enumerate(divs_2, 1):
            try:
                # Find all child ad elements
                child_ads = div.find_elements(By.XPATH, ".//div[contains(@class, 'xh8yej3')]")
                
                if child_ads:
                    for j, ad in enumerate(child_ads, 1):
                        ad_id = f"ad_{i}_{j}"
                        ads_data[ad_id] = {
                            "ad_group": i,
                            "child_ad": j,
                            "platforms": [],
                            "url": url
                        }
                        processed_count += 1
                else:
                    # Single ad
                    ad_id = f"ad_{i}_1"
                    ads_data[ad_id] = {
                        "ad_group": i,
                        "child_ad": 1,
                        "platforms": [],
                        "url": url
                    }
                    processed_count += 1
                
                # Skip detailed ad processing for speed
            except Exception as e:
                custom_print(f"Worker {thread_id}: Error processing ad group {i}: {e}", "error", thread_id)
        
        custom_print(f"Worker {thread_id}: Processed {processed_count} ads for {url}", thread_id=thread_id)
        
        # Update global ad data with thread safety
        with all_ads_data_lock:
            all_ads_data[url] = {
                "ad_count": ad_count if ad_count else len(ads_data),
                "ads_data": ads_data,
                "url_params": url_params
            }
    
    except Exception as e:
        custom_print(f"Worker {thread_id}: Error extracting ads: {e}", "error", thread_id)
    
    # Update the Google Sheet with the results if row_index is provided
    if milk_worksheet and row_index:
        try:
            # Update with current timestamp and ad count
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            current_ip = get_current_ip()
            
            # Find column indices if not already done
            if not hasattr(thread_local, "milk_column_indices"):
                thread_local.milk_column_indices = {}
                milk_headers = milk_worksheet.row_values(1)
                
                for i, header in enumerate(milk_headers):
                    if header == "no.of ads By Ai":
                        thread_local.milk_column_indices['ads_by_ai'] = i + 1
                    if header == "Last Update Time" or header.lower().strip() == "last update time":
                        thread_local.milk_column_indices['last_update'] = i + 1
                    if header == "IP Address" or header.lower().strip() == "ip address":
                        thread_local.milk_column_indices['ip_address'] = i + 1
            
            # Update with batch update to reduce API calls
            cells_to_update = []
            
            if 'ads_by_ai' in thread_local.milk_column_indices:
                cells_to_update.append({
                    'row': row_index,
                    'col': thread_local.milk_column_indices['ads_by_ai'],
                    'value': str(ad_count if ad_count is not None else '0')
                })
            
            if 'last_update' in thread_local.milk_column_indices:
                cells_to_update.append({
                    'row': row_index,
                    'col': thread_local.milk_column_indices['last_update'],
                    'value': current_time
                })
            
            if 'ip_address' in thread_local.milk_column_indices:
                cells_to_update.append({
                    'row': row_index,
                    'col': thread_local.milk_column_indices['ip_address'],
                    'value': current_ip
                })
            
            # Perform batch update
            if cells_to_update:
                batch_data = []
                for cell in cells_to_update:
                    batch_data.append({
                        'range': f"{cell['row']}:{cell['row']}",
                        'values': [['' for _ in range(cell['col']-1)] + [cell['value']]]
                    })
                
                milk_worksheet.batch_update(batch_data)
                custom_print(f"Worker {thread_id}: Updated milk worksheet for URL at row {row_index}", thread_id=thread_id)
        except Exception as e:
            custom_print(f"Worker {thread_id}: Error updating Google Sheet: {e}", "error", thread_id)

# Main execution flow
if __name__ == "__main__":
    # Connect to Google Sheets
    custom_print("Connecting to Google Sheets...")
    sheet_name = "Master Auto Swipe - Test ankur"
    
    # Get current IP
    current_ip = get_current_ip()
    custom_print(f"Using IP: {current_ip} for scraping")
    
    # First try to connect to the Milk worksheet (for URL sources)
    custom_print("Connecting to 'Milk' worksheet for URLs...")
    milk_worksheet = setup_google_sheets(sheet_name, "Milk")
    
    # Then connect to Ads Details worksheet (for output)
    custom_print("Connecting to 'Ads Details' worksheet for output...")
    ads_worksheet = setup_google_sheets(sheet_name, "Ads Details")
    
    # Extract URLs and page names from Milk worksheet
    page_names = {}
    urls = []
    url_to_row_mapping = {}
    
    if milk_worksheet:
        # Check for required columns and add if missing
        milk_headers = milk_worksheet.row_values(1)
        milk_column_indices = {}
        
        for i, header in enumerate(milk_headers):
            milk_column_indices[header.lower().strip()] = i + 1
        
        # Ensure required columns exist
        required_columns = {
            'no.of ads by ai': "no.of ads By Ai",
            'last update time': "Last Update Time",
            'ip address': "IP Address"
        }
        
        for key, header in required_columns.items():
            if not any(k.lower().strip() == key for k in milk_column_indices.keys()):
                next_col = len(milk_headers) + 1
                milk_worksheet.update_cell(1, next_col, header)
                milk_column_indices[key] = next_col
                custom_print(f"Added '{header}' column at index {next_col}")
                milk_headers.append(header)
        
        # Extract URLs
        custom_print("Extracting URLs and page names from Milk worksheet...")
        urls, page_names = extract_urls_from_milk_worksheet(milk_worksheet)
        
        # Get current date (without time) for comparing last update time
        current_date = datetime.now()
        current_date_only = current_date.date()
        custom_print(f"Current date: {current_date_only}")
        
        # Check which URLs have already been processed today
        previously_processed_urls = set()
        all_rows = milk_worksheet.get_all_values()
        
        # Find important column indices
        url_col = None
        last_update_col = None
        page_id_col = None
        
        for i, header in enumerate(milk_headers):
            header_lower = header.lower().strip()
            if header_lower in ['ad url', 'url', 'ad library url']:
                url_col = i + 1
            elif header_lower in ['last update time']:
                last_update_col = i + 1
            elif header_lower in ['page transperancy', 'page transparency']:
                page_id_col = i + 1
        
        # Skip header row
        for row_idx, row in enumerate(all_rows[1:], 2):
            if url_col and url_col-1 < len(row) and row[url_col-1].strip():
                url_value = row[url_col-1].strip()
                url_to_row_mapping[url_value] = row_idx
                
                # Check if this URL was processed today
                if last_update_col and last_update_col-1 < len(row) and row[last_update_col-1].strip():
                    try:
                        last_update_date = datetime.strptime(row[last_update_col-1].strip(), "%Y-%m-%d %H:%M:%S")
                        last_update_date_only = last_update_date.date()
                        
                        if last_update_date_only == current_date_only:
                            previously_processed_urls.add(url_value)
                    except:
                        pass
            
            # Match by page_id if URL is missing
            elif page_id_col and page_id_col-1 < len(row) and row[page_id_col-1].strip():
                page_trans_url = row[page_id_col-1].strip()
                page_id_match = re.search(r'page_id=(\d+)', page_trans_url)
                
                if page_id_match:
                    page_id = page_id_match.group(1)
                    
                    # Look for matching URL in our extracted URLs
                    for url in urls:
                        if f"view_all_page_id={page_id}" in url:
                            url_to_row_mapping[url] = row_idx
                            break
    else:
        custom_print("Could not connect to Milk worksheet. Using a default URL...")
        # Set default URL as fallback - Using the user's specific URL
        urls = ["https://www.facebook.com/ads/library/?active_status=active&ad_type=all&country=US&is_targeted_country=false&media_type=all&search_type=page&view_all_page_id=456998304158696"]
    
    # Filter out URLs that were already processed today
    urls_to_process = [url for url in urls if url not in previously_processed_urls]
    custom_print(f"Will process {len(urls_to_process)} URLs (skipping {len(previously_processed_urls)} already processed today)")
    
    # If no URLs to process, exit
    if not urls_to_process:
        custom_print("All URLs have already been processed today. Nothing new to process.")
        sys.exit(0)
    
    # Set up for parallel processing
    total_urls = len(urls_to_process)
    
    # Determine number of worker threads (browsers)
    # Use 2-3 browsers as requested for faster parallel processing
    num_workers = 3  # Can be adjusted between 2-3 as requested
    custom_print(f"Using {num_workers} parallel browser instances for faster scraping")
    
    # Fill the queue with URLs to process
    for url in urls_to_process:
        row_index = url_to_row_mapping.get(url, None)
        url_queue.put((url, row_index))
    
    # Create and start worker threads
    workers = []
    for i in range(num_workers):
        thread = threading.Thread(target=worker_process_url, args=(i+1,))
        thread.daemon = True
        thread.start()
        workers.append(thread)
        # Small delay between starting threads to avoid simultaneous browser launches
        time.sleep(1)
    
    # Wait for all tasks to be processed
    url_queue.join()
    
    # Signal threads to exit
    for _ in range(num_workers):
        url_queue.put((None, None))
    
    # Wait for all worker threads to finish
    for thread in workers:
        thread.join()
    
    custom_print("All URLs have been processed successfully!")
