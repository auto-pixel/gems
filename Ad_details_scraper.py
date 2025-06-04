from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException
from selenium.webdriver.common.action_chains import ActionChains
from webdriver_manager.firefox import GeckoDriverManager
import time
import re
from datetime import datetime, timedelta
from urllib.parse import unquote, urlparse, parse_qs
import json
import gspread
from google.oauth2.service_account import Credentials
from google.auth.exceptions import GoogleAuthError
import logging
import os
import sys
import random
import concurrent.futures
from queue import Queue
from threading import Lock
import requests
from fake_useragent import UserAgent
import backoff
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# Import anti-detection utilities
from fb_antidetect_utils import (
    ProxyManager,
    create_stealth_driver,
    perform_human_like_scroll,
    simulate_random_mouse_movements,
    add_random_delays,
    get_current_ip
)

# Global variables
MAX_WORKERS = 3  # Number of concurrent scrapers
REQUEST_TIMEOUT = 30  # seconds
MAX_RETRIES = 3
PROXY_TIMEOUT = 10  # seconds
LOCK = Lock()

class ProxyManagerEnhanced(ProxyManager):
    def __init__(self, proxy_file=None, proxies=None):
        super().__init__(proxy_file=proxy_file, proxies=proxies)
        self.proxy_failures = {}
        self.proxy_blacklist = set()
        self.max_failures = 3
        self.lock = Lock()
        self.session = self._create_session()
    
    def _create_session(self):
        """Create a requests session with retry logic"""
        session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session
    
    def get_proxy(self):
        """Get a random working proxy"""
        with self.lock:
            if not self.proxies:
                return None
                
            # Remove blacklisted proxies
            self.proxies = [p for p in self.proxies if p not in self.proxy_blacklist]
            
            if not self.proxies:
                return None
                
            # Sort proxies by failure count (ascending)
            self.proxies.sort(key=lambda x: self.proxy_failures.get(x, 0))
            
            # Return the least failed proxy
            return self.proxies[0]
    
    def mark_failed(self, proxy):
        """Mark a proxy as failed"""
        with self.lock:
            if proxy not in self.proxy_failures:
                self.proxy_failures[proxy] = 0
            self.proxy_failures[proxy] += 1
            
            if self.proxy_failures[proxy] >= self.max_failures:
                self.proxy_blacklist.add(proxy)
                custom_print(f"Proxy {proxy} blacklisted after {self.max_failures} failures", "warning")
    
    def is_proxy_working(self, proxy):
        """Check if a proxy is working"""
        if not proxy:
            return False
            
        proxies = {
            'http': f'http://{proxy}',
            'https': f'http://{proxy}'
        }
        
        try:
            # Test with a small, fast request
            response = self.session.get(
                'http://httpbin.org/ip',
                proxies=proxies,
                timeout=PROXY_TIMEOUT
            )
            return response.status_code == 200
        except Exception as e:
            custom_print(f"Proxy {proxy} test failed: {str(e)}", "debug")
            return False
    
    def get_working_proxy(self):
        """Get a working proxy with verification"""
        max_attempts = len(self.proxies) * 2  # Try each proxy up to 2 times
        attempts = 0
        
        while attempts < max_attempts:
            proxy = self.get_proxy()
            if not proxy:
                break
                
            if self.is_proxy_working(proxy):
                return proxy
                
            self.mark_failed(proxy)
            attempts += 1
            
        return None

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

# Function to log messages (prints to console and writes to log file)
def custom_print(message, level=None):
    """Log a message with timestamp to both console and log file"""
    # Default to info level if no level provided
    if level is None:
        logging.info(message)
    elif level.lower() == "info":
        logging.info(message)
    elif level.lower() == "warning":
        logging.warning(message)
    elif level.lower() == "error":
        logging.error(message)
    elif level.lower() == "debug":
        logging.debug(message)
    else:
        logging.info(message)

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

# Configure enhanced proxy manager
proxy_file = "proxies.txt"  # Proxy file in ip:port format, one per line
proxy_manager = None
use_proxies = True  # Set to False to temporarily disable proxy usage

if os.path.exists(proxy_file) and use_proxies:
    custom_print("Initializing enhanced proxy manager with proxies from file")
    try:
        # Load and validate proxies
        with open(proxy_file, 'r') as f:
            proxies = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
        
        if proxies:
            custom_print(f"Found {len(proxies)} proxies in {proxy_file}")
            proxy_manager = ProxyManagerEnhanced(proxies=proxies)
            
            # Test proxies in parallel
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                future_to_proxy = {executor.submit(proxy_manager.is_proxy_working, proxy): proxy for proxy in proxies}
                for future in concurrent.futures.as_completed(future_to_proxy):
                    proxy = future_to_proxy[future]
                    try:
                        if future.result():
                            custom_print(f"Proxy {proxy} is working")
                        else:
                            custom_print(f"Proxy {proxy} is not working", "warning")
                    except Exception as e:
                        custom_print(f"Error testing proxy {proxy}: {e}", "error")
            
            working_proxies = [p for p in proxies if p not in proxy_manager.proxy_blacklist]
            custom_print(f"Found {len(working_proxies)} working proxies out of {len(proxies)}")
            
            if not working_proxies:
                custom_print("No working proxies found. Will run without proxies.", "warning")
                proxy_manager = None
        else:
            custom_print("No valid proxies found in proxy file. Will run without proxies.", "warning")
    except Exception as e:
        custom_print(f"Error initializing proxy manager: {e}", "error")
        custom_print("Will run without proxies due to error.", "warning")
else:
    custom_print("No proxy file found or proxies disabled. Will run without proxies.", "warning")

def create_driver(proxy=None):
    """Create a new WebDriver instance with the given proxy"""
    options = Options()
    options.headless = True
    
    # Set proxy if provided
    if proxy:
        options.set_preference('network.proxy.type', 1)
        options.set_preference('network.proxy.http', proxy.split(':')[0])
        options.set_preference('network.proxy.http_port', int(proxy.split(':')[1]))
        options.set_preference('network.proxy.ssl', proxy.split(':')[0])
        options.set_preference('network.proxy.ssl_port', int(proxy.split(':')[1]))
    
    # Additional stealth options
    options.set_preference('dom.webdriver.enabled', False)
    options.set_preference('useAutomationExtension', False)
    options.set_preference('permissions.default.image', 2)  # Disable images
    
    # Randomize user agent
    ua = UserAgent()
    user_agent = ua.random
    options.set_preference('general.useragent.override', user_agent)
    
    # Initialize WebDriver with error handling
    max_retries = 3
    for attempt in range(max_retries):
        try:
            driver = webdriver.Firefox(options=options)
            # Set page load timeout
            driver.set_page_load_timeout(REQUEST_TIMEOUT)
            return driver
        except Exception as e:
            if attempt == max_retries - 1:
                custom_print(f"Failed to create WebDriver after {max_retries} attempts: {e}", "error")
                raise
            time.sleep(2 ** attempt)  # Exponential backoff

# Create a driver pool
DRIVER_POOL = Queue()

def get_driver():
    """Get a WebDriver instance from the pool or create a new one"""
    if not DRIVER_POOL.empty():
        return DRIVER_POOL.get()
    
    proxy = None
    if proxy_manager:
        proxy = proxy_manager.get_working_proxy()
        if not proxy:
            custom_print("No working proxies available. Will try without proxy.", "warning")
    
    try:
        driver = create_driver(proxy)
        return driver
    except Exception as e:
        if proxy:
            proxy_manager.mark_failed(proxy)
        custom_print(f"Error creating WebDriver: {e}", "error")
        raise

def release_driver(driver):
    """Release a WebDriver instance back to the pool"""
    try:
        # Clear cookies and cache
        driver.delete_all_cookies()
        driver.execute_script("window.localStorage.clear();")
        driver.execute_script("window.sessionStorage.clear();")
        
        # Reset the page to clear any state
        driver.get("about:blank")
        
        # Add back to pool if not too many drivers
        if DRIVER_POOL.qsize() < MAX_WORKERS * 2:  # Limit pool size
            DRIVER_POOL.put(driver)
        else:
            driver.quit()
    except Exception as e:
        custom_print(f"Error releasing driver: {e}", "error")
        try:
            driver.quit()
        except:
            pass

# Initialize driver pool
for _ in range(MAX_WORKERS):
    try:
        driver = get_driver()
        DRIVER_POOL.put(driver)
    except Exception as e:
        custom_print(f"Failed to initialize driver pool: {e}", "error")

# Configure dynamic wait times with jitter
def get_wait_time(base=5, jitter=3):
    """Get a random wait time with jitter"""
    return base + random.uniform(-jitter, jitter)

# Default WebDriverWait with dynamic timeout
def get_webdriver_wait(driver, timeout=None):
    """Get a WebDriverWait instance with dynamic timeout"""
    if timeout is None:
        timeout = get_wait_time(10, 5)  # Base 10s ±5s
    return WebDriverWait(driver, timeout)

# Variable to store the result string for notification
result_string = ""

# Initialize variable to store column indices across all URLs
column_indices = {}

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

# Connect to Google Sheets
custom_print("Connecting to Google Sheets...")
sheet_name = "Master Auto Swipe - Test ankur"

# Get utility functions from the anti-detection utils module
from fb_antidetect_utils import ProxyManager, get_current_ip

# Initialize the ProxyManager (optional, for traditional proxies)
proxy_manager = ProxyManager()

# Get and log the current IP
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
if milk_worksheet:
    custom_print("Extracting URLs and page names from Milk worksheet...")
    
    # Check if IP Address column exists in Milk worksheet
    milk_headers = milk_worksheet.row_values(1)
    ip_col_exists = False
    for i, header in enumerate(milk_headers):
        if header == "IP Address" or header.lower().strip() == "ip address":
            ip_col_exists = True
            # Update the IP address in all rows
            ip_col_idx = i + 1  # Convert to 1-indexed
            custom_print(f"Found IP Address column in Milk worksheet at index {ip_col_idx}")
            break
            
    if not ip_col_exists:
        # Add IP Address column to Milk worksheet
        next_col = len(milk_headers) + 1
        milk_worksheet.update_cell(1, next_col, "IP Address")
        ip_col_idx = next_col
        custom_print(f"Added IP Address column to Milk worksheet at index {ip_col_idx}")
        
    # Don't update IP addresses for all rows at once - we'll update them individually later
    # Get all data rows (excluding header) - keep this for other purposes
    all_milk_rows = milk_worksheet.get_all_values()[1:]
    
    # Also check if we need to add Last Update Time column to the Ads Details worksheet
    if ads_worksheet:
        ads_headers = ads_worksheet.row_values(1)
        
        # Check for Last Update Time column
        last_update_col_exists = False
        for i, header in enumerate(ads_headers):
            if header == "Last Update Time" or header.lower().strip() == "last update time":
                last_update_col_exists = True
                last_update_col_idx = i + 1  # Convert to 1-indexed
                custom_print(f"Found Last Update Time column in Ads Details worksheet at index {last_update_col_idx}")
                break
                
        if not last_update_col_exists:
            # Add Last Update Time column to Ads Details worksheet
            next_col = len(ads_headers) + 1
            ads_worksheet.update_cell(1, next_col, "Last Update Time")
            last_update_col_idx = next_col
            custom_print(f"Added Last Update Time column to Ads Details worksheet at index {last_update_col_idx}")
        
        # Check for IP Address column
        ip_addr_col_exists = False
        for i, header in enumerate(ads_headers):
            if header == "IP Address" or header.lower().strip() == "ip address":
                ip_addr_col_exists = True
                ads_ip_col_idx = i + 1  # Convert to 1-indexed
                custom_print(f"Found IP Address column in Ads Details worksheet at index {ads_ip_col_idx}")
                break
                
        if not ip_addr_col_exists:
            # Add IP Address column to Ads Details worksheet
            next_col = len(ads_headers) + 1
            ads_worksheet.update_cell(1, next_col, "IP Address")
            ads_ip_col_idx = next_col
            custom_print(f"Added IP Address column to Ads Details worksheet at index {ads_ip_col_idx}")
    
    urls, page_names = extract_urls_from_milk_worksheet(milk_worksheet)
    
    if not urls:
        custom_print("No URLs found in the Milk worksheet. Using a default URL...")
        # Set default URL as fallback
        urls = ["https://www.facebook.com/ads/library/?active_status=active&ad_type=all&country=ALL&is_targeted_country=false&media_type=all&search_type=page&source=page-transparency-widget&view_all_page_id=109760815561056"]
else:
    custom_print("Could not connect to Milk worksheet. Using a default URL...")
    # Set default URL as fallback - Using the user's specific URL
    urls = ["https://www.facebook.com/ads/library/?active_status=active&ad_type=all&country=US&is_targeted_country=false&media_type=all&search_type=page&view_all_page_id=456998304158696"]

# Initialize a dictionary to collect all ads data
all_ads_data = {}

# Implement URL randomization to avoid detection patterns
# Use a copy of the URLs for randomization but track which ones were processed
custom_print("Setting up randomized URL processing order to avoid detection patterns...")
urls_to_process = urls.copy()  # Keep original list intact
processed_urls = set()  # Track which URLs have been processed
custom_print(f"Will process {len(urls_to_process)} URLs in randomized order")

# Initialize progress tracking
total_urls = len(urls)
def update_progress_percentage():
    if total_urls > 0:
        progress_percentage = (len(processed_urls) / total_urls) * 100
        # Format with GitHub Actions-friendly output that's easy to spot in logs
        # Use simple ASCII characters for progress bar to avoid encoding issues
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

def process_url(url_data):
    """Process a single URL with retry logic and ensure Milk sheet is updated. Optimized for speed and robustness."""
    url, url_index, total_urls, page_name = url_data
    driver = None
    max_retries = 3
    result = {
        'success': False,
        'url': url,
        'page_name': page_name,
        'ad_count': 0,
        'error': [],  # Store all errors for debugging
    }

    for attempt in range(max_retries):
        try:
            driver = None
            try:
                driver = get_driver()
            except Exception as e:
                result['error'].append(f"get_driver() failed: {str(e)}")
                custom_print(f"[Attempt {attempt+1}] get_driver() failed: {e}", "error")
                time.sleep(2 ** attempt)
                continue

            wait = get_webdriver_wait(driver, timeout=15)  # Use shorter, smarter wait
            custom_print(f"[{page_name}] Processing URL {url_index}/{total_urls}: {url}")

            # Navigate to the URL with 'eager' page load strategy for speed
            try:
                driver.execute_cdp_cmd('Page.setLifecycleEventsEnabled', {'enabled': True})
            except Exception:
                pass  # Not all drivers support this
            try:
                driver.get(url)
            except Exception as e:
                result['error'].append(f"driver.get() failed: {str(e)}")
                custom_print(f"[Attempt {attempt+1}] driver.get() failed: {e}", "error")
                raise

            # Wait for the main ad container or fallback to body
            try:
                wait.until(EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'ad') or contains(text(), 'results') or @role='main']")))
            except Exception:
                wait.until(EC.presence_of_element_located((By.TAG_NAME, 'body')))

            # Extract ad count robustly
            ad_count = 0
            try:
                # Try multiple selectors for robustness
                selectors = [
                    "//div[contains(text(), 'results')]",
                    "//span[contains(text(), 'results')]",
                    "//div[contains(@class, 'ad-count')]",
                ]
                ad_count_text = None
                for selector in selectors:
                    try:
                        elem = driver.find_element(By.XPATH, selector)
                        ad_count_text = elem.text.strip()
                        if ad_count_text:
                            break
                    except Exception:
                        continue
                if ad_count_text:
                    ad_count = int(''.join(filter(str.isdigit, ad_count_text)))
                    custom_print(f"[{page_name}] Found {ad_count} ads.")
                else:
                    custom_print(f"[{page_name}] Could not find ad count element.", "warning")
            except Exception as e:
                custom_print(f"[{page_name}] Error extracting ad count: {e}", "warning")
            result['ad_count'] = ad_count

            # Update Milk sheet with the ad count
            try:
                update_milk_sheet(url, page_name, ad_count)
            except Exception as e:
                custom_print(f"[{page_name}] Error updating Milk sheet: {e}", "error")
                result['error'].append(f"update_milk_sheet error: {str(e)}")

            # Mark as successful
            result['success'] = True
            return result

        except Exception as e:
            error_msg = str(e)
            custom_print(f"[Attempt {attempt + 1}] Failed for URL {url}: {error_msg}", "error")
            result['error'].append(error_msg)

            # Robust proxy handling
            if proxy_manager and hasattr(driver, 'proxy'):
                try:
                    proxy_manager.mark_failed(getattr(driver, 'proxy', None))
                except Exception as proxy_err:
                    custom_print(f"Proxy marking failed: {proxy_err}", "debug")

            if attempt == max_retries - 1:
                custom_print(f"[{page_name}] Failed to process URL after {max_retries} attempts", "error")
                try:
                    update_milk_sheet(url, page_name, 0, error='; '.join(result['error']))
                except Exception as e2:
                    custom_print(f"[{page_name}] Error updating Milk sheet on final failure: {e2}", "error")
                return result
            time.sleep(2 ** attempt)
        finally:
            if driver:
                try:
                    release_driver(driver)
                except Exception as e:
                    custom_print(f"Error releasing driver: {e}", "warning")

    return result


def update_milk_sheet(url, page_name, ad_count, error=None):
    """Update the Milk sheet with the ad count and status"""
    if not milk_worksheet:
        custom_print("Milk worksheet not available for update", "warning")
        return False
    
    try:
        # Get all rows to find the matching page
        all_rows = milk_worksheet.get_all_values()
        headers = all_rows[0] if all_rows else []
        
        # Find the row index for this page
        row_index = None
        page_col_idx = None
        trans_col_idx = None
        
        # Find column indices
        for idx, header in enumerate(headers):
            header_lower = header.lower().strip()
            if header_lower in ['page', 'page name', 'name']:
                page_col_idx = idx
            elif header_lower in ['page transparency', 'transparency url']:
                trans_col_idx = idx
        
        if page_col_idx is None or trans_col_idx is None:
            custom_print("Could not find required columns in Milk sheet", "error")
            return False
        
        # Find the matching row
        for i, row in enumerate(all_rows[1:], start=2):  # Skip header row
            if len(row) > max(page_col_idx, trans_col_idx):
                page_name_match = row[page_col_idx].strip().lower() == page_name.lower()
                url_match = url in (row[trans_col_idx].strip(), '')
                if page_name_match or url_match:
                    row_index = i
                    break
        
        if row_index:
            # Prepare update data
            update_data = {}
            
            # Find or create columns for our data
            columns_to_find = [
                'Ad Count', 'Last Checked', 'Status', 'Error'
            ]
            
            for col_name in columns_to_find:
                if col_name not in headers:
                    # Add new column
                    col_idx = len(headers)
                    milk_worksheet.update_cell(1, col_idx + 1, col_name)
                    headers.append(col_name)
            
            # Update the row
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Get column indices
            col_indices = {col: headers.index(col) for col in headers if col in columns_to_find}
            
            # Prepare row data
            row_data = [''] * len(headers)
            
            # Copy existing data
            if row_index - 1 < len(all_rows):
                existing_row = all_rows[row_index - 1]
                for i, val in enumerate(existing_row):
                    if i < len(row_data):
                        row_data[i] = val
            
            # Update with new data
            if 'Ad Count' in col_indices:
                row_data[col_indices['Ad Count']] = str(ad_count)
            if 'Last Checked' in col_indices:
                row_data[col_indices['Last Checked']] = current_time
            if 'Status' in col_indices:
                row_data[col_indices['Status']] = 'Success' if error is None else 'Failed'
            if 'Error' in col_indices and error:
                row_data[col_indices['Error']] = error[:100]  # Truncate long errors
            
            # Update the row
            milk_worksheet.update(f"A{row_index}", [row_data])
            custom_print(f"Updated Milk sheet for {page_name} with {ad_count} ads")
            return True
        else:
            custom_print(f"Could not find row for page: {page_name}", "warning")
            return False
            
    except Exception as e:
        custom_print(f"Error updating Milk sheet: {e}", "error")
        return False

def process_urls_in_parallel(urls, page_names):
    """Process all URLs in parallel and return results"""
    # Prepare URL data for processing with page names
    urls_to_process = []
    for i, (url, page_name) in enumerate(zip(urls, [page_names.get(url, f"Page {i+1}") for i, url in enumerate(urls)])):
        urls_to_process.append((url, i+1, len(urls), page_name))

    # Use ThreadPoolExecutor for parallel processing
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all URLs for processing
        future_to_url = {executor.submit(process_url, url_data): url_data for url_data in urls_to_process}
        
        # Process results as they complete
        results = []
        for future in concurrent.futures.as_completed(future_to_url):
            url_data = future_to_url[future]
            try:
                result = future.result()
                results.append(result)
                if result['success']:
                    custom_print(f"Successfully processed {result['page_name']}: {result['ad_count']} ads")
                else:
                    custom_print(f"Failed to process {result['page_name']}: {result.get('error', 'Unknown error')}", "error")
            except Exception as e:
                error_msg = str(e)
                custom_print(f"Error processing URL {url_data[0]}: {error_msg}", "error")
                results.append({
                    'url': url_data[0],
                    'page_name': url_data[3] if len(url_data) > 3 else 'Unknown',
                    'success': False,
                    'error': error_msg,
                    'ad_count': 0
                })
        
        # Generate summary report
        success_count = sum(1 for r in results if r.get('success', False))
        total_ads = sum(r.get('ad_count', 0) for r in results)
        
        custom_print("\n" + "="*50)
        custom_print("PROCESSING COMPLETE")
        custom_print("="*50)
        custom_print(f"Total URLs processed: {len(results)}")
        custom_print(f"Successfully processed: {success_count}")
        custom_print(f"Failed: {len(results) - success_count}")
        custom_print(f"Total ads found: {total_ads}")
        custom_print("="*50)
        
        # Print failed URLs for reference
        failed_urls = [r for r in results if not r.get('success', False)]
        if failed_urls:
            custom_print("\nFailed URLs:")
            for item in failed_urls:
                custom_print(f"- {item.get('page_name', 'Unknown')}: {item.get('url')} - {item.get('error', 'Unknown error')}")
        
        return results

# Process URLs in parallel with thread pool
custom_print(f"Starting to process {len(urls)} URLs with {MAX_WORKERS} workers")
# Process all URLs and get results
results = process_urls_in_parallel(urls, page_names)

# First, check which URLs have already been processed in the milk sheet recently
previously_processed_urls = set()
reprocessing_needed = []  # URLs that need to be reprocessed (processed > 24 hours ago)

# Get current date (without time) for comparing last update time
current_date = datetime.now()
current_date_only = current_date.date()
custom_print(f"Current date: {current_date_only}")

if milk_worksheet:
    try:
        # Get the current headers to find column indices
        milk_headers = milk_worksheet.row_values(1)
        milk_column_indices = {}
        
        for i, header in enumerate(milk_headers):
            milk_column_indices[header.lower().strip()] = i + 1  # Convert to 1-indexed
        
        # Find the Last Update Time and URL columns
        last_update_col = None
        url_col = None
        page_id_col = None
        
        for header, idx in milk_column_indices.items():
            if header in ['last update time']:
                last_update_col = idx
            elif header in ['ad url', 'url', 'ad library url']:
                url_col = idx
            elif header in ['page transperancy', 'page transparency']:
                page_id_col = idx
        
        if last_update_col and url_col:
            # Get all rows from milk sheet
            all_rows = milk_worksheet.get_all_values()
            
            custom_print(f"Checking {len(all_rows)-1} rows in milk sheet for previously processed URLs")
            
            # Skip header row
            for row_idx, row in enumerate(all_rows[1:], 2):  # Start from row 2 (1-indexed)
                # Check if URL has a Last Update Time (meaning it was processed)
                if url_col-1 < len(row) and last_update_col-1 < len(row):
                    url_value = row[url_col-1].strip() if row[url_col-1] else ""
                    last_update_value = row[last_update_col-1].strip() if last_update_col-1 < len(row) else ""
                    
                    # Check if this is a valid URL to process
                    if url_value:
                        # Parse the page_id from the URL for better matching
                        url_params = parse_url_params(url_value)
                        page_id = url_params.get('page_id') or url_params.get('view_all_page_id')
                        
                        if last_update_value:
                            try:
                                # Parse the last update timestamp
                                last_update_date = datetime.strptime(last_update_value, "%Y-%m-%d %H:%M:%S")
                                
                                # Extract just the date part (without time)
                                last_update_date_only = last_update_date.date()
                                
                                # Compare only dates, not times
                                if last_update_date_only == current_date_only:
                                    # URL was processed today, skip it
                                    previously_processed_urls.add(url_value)
                                    custom_print(f"URL already processed today ({last_update_date_only}): {url_value}")
                                else:
                                    # URL was not processed today, needs reprocessing
                                    custom_print(f"URL needs reprocessing (last updated on {last_update_date_only}, current date is {current_date_only}): {url_value}")
                                    reprocessing_needed.append((url_value, row_idx, page_id))
                            except Exception as e:
                                custom_print(f"Error parsing last update time '{last_update_value}': {e}", "warning")
                                # If we can't parse the timestamp, treat as needing reprocessing
                                reprocessing_needed.append((url_value, row_idx, page_id))
                        else:
                            # No last update time, it needs processing
                            custom_print(f"URL never processed: {url_value}")
                            reprocessing_needed.append((url_value, row_idx, page_id))
                    elif page_id_col and page_id_col-1 < len(row) and row[page_id_col-1].strip():
                        # If URL is empty but page_id exists, this row needs a URL
                        custom_print(f"Row {row_idx} has page_id but no URL, it may need attention")
    except Exception as e:
        custom_print(f"Error checking previously processed URLs: {e}", "error")

custom_print(f"Found {len(previously_processed_urls)} URLs already processed today that will be skipped")
custom_print(f"Found {len(reprocessing_needed)} URLs that need to be processed (not updated today)")

# Create a comprehensive list of all URLs to process
urls_to_process = []

# First add URLs that need reprocessing (processed > 24 hours ago or never processed)
for url_info in reprocessing_needed:
    url = url_info[0]  # Extract just the URL from the tuple
    if url not in urls_to_process:
        urls_to_process.append(url)

# Then add any URLs from the original list that aren't in either category yet
for url in urls:
    if url not in previously_processed_urls and url not in urls_to_process:
        urls_to_process.append(url)
        custom_print(f"Added new URL to processing list: {url}")

custom_print(f"Will process {len(urls_to_process)} URLs total (skipping {len(previously_processed_urls)} URLs already processed today)")

# If no URLs to process, exit gracefully
if not urls_to_process:
    custom_print("All URLs have already been processed today. Nothing new to process.")
    # Clean up and exit
    driver.quit()
    sys.exit(0)

# Reset total URLs to the final list we need to process
total_urls = len(urls_to_process)
urls = urls_to_process

# Create a mapping of URLs to their milk sheet row numbers from our earlier processing
# This will help us update the correct row later
url_to_row_mapping = {}
for url_info in reprocessing_needed:
    url, row_idx, page_id = url_info
    url_to_row_mapping[url] = row_idx

while len(processed_urls) < len(urls):
    # Randomly select a URL that hasn't been processed yet
    remaining_urls = [u for u in urls if u not in processed_urls]
    url = random.choice(remaining_urls)
    url_index += 1
    
    # Track the milk sheet row for this URL if we know it
    current_milk_row = url_to_row_mapping.get(url, None)
    if current_milk_row:
        custom_print(f"This URL corresponds to row {current_milk_row} in the milk sheet")
    
    custom_print(f"\n===== Processing URL {url_index}/{len(urls)} ({len(processed_urls)+1} of {len(urls)} total) =====")
    custom_print(f"Opening URL: {url}")
    
    # Update progress percentage for GitHub Actions console
    if total_urls > 0:
        progress_percentage = (len(processed_urls) / total_urls) * 100
        # Format with GitHub Actions-friendly output that's easy to spot in logs
        # Use simple ASCII characters for progress bar to avoid encoding issues
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
    
    # Implement session cooling - add random delays between processing URLs
    if url_index > 1:
        # Ultra-minimal cooling period between URLs for fastest scraping
        cooling_time = random.uniform(0.5, 3.5)  # 0.5-3.5 seconds between URLs - ultra fast
        custom_print(f"Adding ultra-minimal cooling period of {cooling_time:.1f} seconds before next URL...")
        time.sleep(cooling_time)
    
    # Add retry mechanism with proxy rotation
    max_retries = 3
    retry_count = 0
    success = False
    
    while retry_count < max_retries and not success:
        try:
            # Try to load the page with additional safety measures
            custom_print(f"Loading page (attempt {retry_count + 1}/{max_retries})...")
            
            # Add IP rotation after certain number of requests (if proxies available)
            global request_count
            if not 'request_count' in globals():
                request_count = 0
            request_count += 1
            
            # Rotate IP every 8-12 requests if proxies are available
            if proxy_manager and request_count >= random.randint(8, 12):
                custom_print("Rotation limit reached. Rotating IP for safety...")
                # Close current driver
                driver.quit()
                
                # Create new driver with different proxy
                driver = create_stealth_driver(
                    use_proxy=True,
                    proxy_manager=proxy_manager,
                    headless=True
                )
                wait = WebDriverWait(driver, random.uniform(8, 12))
                request_count = 0  # Reset counter
            
            # Load the page directly
            driver.get(url)
            custom_print(f"Navigating to URL directly")
            
            # Ultra-minimal wait times for fastest scraping
            if random.random() < 0.05:  # 5% chance of slightly longer wait
                custom_print("Using minimal extended waiting pattern...")
                wait_time = random.uniform(0.5, 0.8)
                add_random_delays(wait_time, wait_time + 0.2)
            else:
                add_random_delays(0.1, 0.3)  # Extremely short delays
                
            # Ultra-minimal mouse behavior - very rarely
            if random.random() < 0.1:  # 10% chance to do any movement
                num_movements = random.randint(1, 2)  # Minimal movements
                simulate_random_mouse_movements(driver, num_movements=num_movements)
            
            # Ultra-minimal wait for initial content to load
            wait_time = random.uniform(0.3, 0.8)  # Ultra-short wait
            custom_print(f"Waiting {wait_time:.1f} seconds for content to load...")
            time.sleep(wait_time)
            
            # Check if page loaded properly (look for a known element)
            try:
                # Wait for an element that should be present on a properly loaded page with shorter timeout
                WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'x6s0dn4')]"))
                )
                success = True
                custom_print("Page loaded successfully!")
            except (TimeoutException, NoSuchElementException):
                custom_print("Page didn't load properly", "warning")
                raise Exception("Page failed to load properly")
                
        except Exception as e:
            retry_count += 1
            custom_print(f"Error loading page: {e}", "error")
            
            if proxy_manager and retry_count < max_retries:
                # Rotate proxy before retrying
                custom_print("Rotating proxy and retrying...", "warning")
                
                # Close current driver
                driver.quit()
                
                # Create new driver with different proxy
                driver = create_stealth_driver(
                    use_proxy=True,
                    proxy_manager=proxy_manager,
                    headless=True
                )
                wait = WebDriverWait(driver, random.uniform(8, 12))
            elif retry_count >= max_retries:
                custom_print("Maximum retry attempts reached. Continuing with direct connection.", "warning")
                
                # Fall back to direct connection if all proxies fail
                if proxy_manager:
                    custom_print("Attempting with direct connection (no proxy)...", "warning")
                    driver.quit()
                    driver = create_stealth_driver(use_proxy=False, headless=True)
                    wait = WebDriverWait(driver, random.uniform(8, 12))
                    
                # Try one last time with direct connection
                try:
                    driver.get(url)
                    time.sleep(random.uniform(1.5, 3))
                    success = True
                except Exception as e:
                    custom_print(f"Failed to load with direct connection: {e}", "error")
                    custom_print(f"Skipping URL {url}", "error")
                    continue
    
    # If we couldn't load the page after all retries, skip this URL
    if not success:
        custom_print(f"Failed to load URL {url} after multiple attempts. Skipping.", "error")
        continue
        
    # Ultra-minimal wait for initial ad content to load
    custom_print("Waiting for initial ad content to load...")
    time.sleep(0.2)  # Ultra-reduced initial wait
    
    # Extract the ad count (like "~5 results") from the page immediately after loading
    custom_print("Extracting ad count from the page...")
    ad_count = None
    ad_count_text = ""
    
    try:
        # First try to find the element with role="heading" containing "results" with shorter timeout
        try:
            ad_count_element = WebDriverWait(driver, 3).until(
                EC.presence_of_element_located((By.XPATH, "//div[@role='heading'][contains(text(), 'results') or contains(text(), 'result')]"))
            )
        except TimeoutException:
            # Fall back to direct find without waiting
            ad_count_element = driver.find_element(By.XPATH, "//div[@role='heading'][contains(text(), 'results') or contains(text(), 'result')]")
        ad_count_text = ad_count_element.text.strip()
        custom_print(f"Found ad count text: {ad_count_text}")
        
        # Extract the numeric part using regex
        matches = re.search(r'~?(\d+(?:,\d+)?)', ad_count_text)
        if matches:
            # Remove commas and convert to int
            ad_count = int(matches.group(1).replace(',', ''))
            custom_print(f"Extracted ad count: {ad_count}")
        else:
            custom_print(f"Could not extract numeric ad count from: {ad_count_text}", "warning")
    except (NoSuchElementException, TimeoutException):
        # If the first method fails, try a more general approach
        try:
            ad_count_element = driver.find_element(By.XPATH, "//div[contains(text(), 'results') or contains(text(), 'result')]") 
            ad_count_text = ad_count_element.text.strip()
            custom_print(f"Found ad count text (alternate method): {ad_count_text}")
            
            # Extract the numeric part using regex
            matches = re.search(r'~?(\d+(?:,\d+)?)', ad_count_text)
            if matches:
                # Remove commas and convert to int
                ad_count = int(matches.group(1).replace(',', ''))
                custom_print(f"Extracted ad count: {ad_count}")
            else:
                custom_print(f"Could not extract numeric ad count from: {ad_count_text}", "warning")
        except NoSuchElementException:
            # Try JavaScript as a last resort
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
                    custom_print(f"Found ad count text (JavaScript method): {ad_count_text}")
                    matches = re.search(r'~?(\d+(?:,\d+)?)', ad_count_text)
                    if matches:
                        ad_count = int(matches.group(1).replace(',', ''))
                        custom_print(f"JavaScript-extracted ad count: {ad_count}")
            except Exception as js_error:
                custom_print(f"JavaScript ad count extraction failed: {str(js_error)}", "warning")
    except Exception as e:
        custom_print(f"Error extracting ad count: {e}", "error")
    
    # Process URL parameters to get the page_id for tracking in the Milk sheet
    url_params = {}
    try:
        # Extract page_id from URL
        if "view_all_page_id=" in url:
            page_id_match = re.search(r'view_all_page_id=(\d+)', url)
            if page_id_match:
                url_params['page_id'] = page_id_match.group(1)
                custom_print(f"Extracted page_id from URL: {url_params['page_id']}")
    except Exception as e:
        custom_print(f"Error extracting URL parameters: {e}", "error")
    
    # Initialize should_skip_url flag
    should_skip_url = False
    
    # If ad_count is 0, update the Milk sheet and skip to the next URL without scrolling
    if ad_count == 0 or ad_count is None:
        custom_print("No ads found (count is 0). Updating Milk sheet and skipping to next URL...")
        
        # Update the Milk worksheet with the ad count, timestamp, and IP address
        if milk_worksheet:
            try:
                # Get the current headers to find column indices
                milk_headers = milk_worksheet.row_values(1)
                milk_column_indices = {}
                
                # Find the necessary column indices - account for trailing spaces in column names
                for i, header in enumerate(milk_headers):
                    # Check for 'no.of ads By Ai' column (exact match with spaces)
                    if header == "no.of ads By Ai":
                        milk_column_indices['ads_by_ai'] = i + 1  # 1-indexed
                        custom_print(f"Found 'no.of ads By Ai' column at index {milk_column_indices['ads_by_ai']}")
                    
                    # Check for Last Update Time column
                    if header == "Last Update Time" or header.lower().strip() == "last update time":
                        milk_column_indices['last_update'] = i + 1  # 1-indexed
                        custom_print(f"Found 'Last Update Time' column at index {milk_column_indices['last_update']}")
                    
                    # Check for IP Address column
                    if header == "IP Address" or header.lower().strip() == "ip address":
                        milk_column_indices['ip_address'] = i + 1  # 1-indexed
                        custom_print(f"Found 'IP Address' column at index {milk_column_indices['ip_address']}")
                    
                    # Find Page Transperancy column for matching - handle spaces in column name
                    if header == "Page Transperancy " or header == "Page Transperancy":
                        milk_column_indices['page_transperancy'] = i + 1  # 1-indexed
                        custom_print(f"Found 'Page Transperancy' column at index {milk_column_indices['page_transperancy']}")
                    elif header.lower().strip() in ["page transperancy", "page transparency"]:
                        milk_column_indices['page_transperancy'] = i + 1  # 1-indexed
                        custom_print(f"Found 'Page Transperancy' column via fallback at index {milk_column_indices['page_transperancy']}")
                    
                # Also find URL column to check for existing URLs
                url_col_index = None
                for i, header in enumerate(milk_headers):
                    if header.lower().strip() in ["ad url", "url", "ad library url"]:
                        url_col_index = i + 1  # 1-indexed
                        custom_print(f"Found URL column at index {url_col_index}")
                        break
                
                # Check if this URL has already been processed (has Last Update Time)
                if url_col_index and 'last_update' in milk_column_indices:
                    # Get all URLs
                    url_values = milk_worksheet.col_values(url_col_index)
                    
                    # Get all Last Update Time values
                    last_update_values = milk_worksheet.col_values(milk_column_indices['last_update'])
                    
                    # Look for a row that contains this exact URL and has a Last Update Time
                    for i, cell_url in enumerate(url_values):
                        if i > 0 and i < len(last_update_values) and cell_url.strip() == url.strip():
                            if last_update_values[i].strip():
                                custom_print(f"This URL was already processed on {last_update_values[i]}")
                                custom_print("Skipping this URL as it has already been processed...")
                                
                                # Flag that this URL should be skipped
                                should_skip_url = True
                                # Add to processed URLs for progress tracking
                                processed_urls.add(url)
                                update_progress_percentage()
                                break
                
                # Add any missing columns to Milk worksheet
                if 'ads_by_ai' not in milk_column_indices:
                    next_col = len(milk_headers) + 1
                    milk_worksheet.update_cell(1, next_col, "no.of ads By Ai")
                    milk_column_indices['ads_by_ai'] = next_col
                    custom_print(f"Added 'no.of ads By Ai' column at index {next_col}")
                
                if 'last_update' not in milk_column_indices:
                    next_col = len(milk_headers) + 1
                    milk_worksheet.update_cell(1, next_col, "Last Update Time")
                    milk_column_indices['last_update'] = next_col
                    custom_print(f"Added 'Last Update Time' column at index {next_col}")
                
                if 'ip_address' not in milk_column_indices:
                    next_col = len(milk_headers) + 1
                    milk_worksheet.update_cell(1, next_col, "IP Address")
                    milk_column_indices['ip_address'] = next_col
                    custom_print(f"Added 'IP Address' column at index {next_col}")
                
                # Find the matching row in Milk worksheet for this URL
                row_index = None
                
                # First, check if we already know which row this URL corresponds to
                if current_milk_row:
                    row_index = current_milk_row
                    custom_print(f"Using known row {row_index} for this URL from earlier processing")
                else:
                    # Try to find URL column and match directly by URL
                    url_col_index = None
                    for i, header in enumerate(milk_headers):
                        if header.lower().strip() in ["ad url", "url", "ad library url"]:
                            url_col_index = i + 1  # 1-indexed
                            custom_print(f"Found URL column at index {url_col_index}")
                            break
                    
                    if url_col_index:
                        # Get all values in the URL column
                        url_values = milk_worksheet.col_values(url_col_index)
                        
                        # Look for a row that contains this exact URL
                        for i, cell_url in enumerate(url_values):
                            if cell_url.strip() == url.strip():
                                row_index = i + 1  # 1-indexed
                                custom_print(f"Found matching URL in Milk worksheet at row {row_index}")
                                break
                    
                    # If no match by URL, try matching by page_id as a fallback
                    if not row_index and 'page_transperancy' in milk_column_indices and 'page_id' in url_params:
                        # Get all values in the Page Transperancy column
                        page_trans_values = milk_worksheet.col_values(milk_column_indices['page_transperancy'])
                        
                        # Look for a row that contains this page_id
                        for i, cell_value in enumerate(page_trans_values):
                            if url_params['page_id'] in cell_value:
                                row_index = i + 1  # 1-indexed
                                custom_print(f"Found matching page_id in Milk worksheet at row {row_index}")
                                
                                # If we found it by page_id but not URL, update the URL column
                                if url_col_index:
                                    milk_worksheet.update_cell(row_index, url_col_index, url)
                                    custom_print(f"Updated URL column at row {row_index} with current URL")
                                break
                
                if row_index:
                    # Get current timestamp
                    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    
                    # Update the no.of ads By Ai column - extract only the number
                    if 'ads_by_ai' in milk_column_indices:
                        # If ad_count is found, use it, otherwise use 0
                        value_to_update = str(ad_count) if ad_count is not None else '0'
                        milk_worksheet.update_cell(row_index, milk_column_indices['ads_by_ai'], value_to_update)
                        custom_print(f"Updated 'no.of ads By Ai' column with value: {value_to_update}")
                    
                    # Update the Last Update Time column
                    if 'last_update' in milk_column_indices:
                        milk_worksheet.update_cell(row_index, milk_column_indices['last_update'], current_time)
                        custom_print(f"Updated 'Last Update Time' column with value: {current_time}")
                    
                    # Update the IP Address column
                    if 'ip_address' in milk_column_indices:
                        milk_worksheet.update_cell(row_index, milk_column_indices['ip_address'], current_ip)
                        custom_print(f"Updated 'IP Address' column with value: {current_ip}")
                    
                    # Update the same columns in the Ads Details worksheet if needed
                    if ads_details_worksheet:
                        try:
                            # Get the headers from Ads Details worksheet
                            ads_headers = ads_details_worksheet.row_values(1)
                            ads_column_indices = {}
                            
                            # Find the necessary column indices
                            for i, header in enumerate(ads_headers):
                                # Check for Last Update Time column
                                if header == "Last Update Time" or header.lower().strip() == "last update time":
                                    ads_column_indices['last_update'] = i + 1  # 1-indexed
                                    custom_print(f"Found 'Last Update Time' column in Ads Details at index {ads_column_indices['last_update']}")
                                
                                # Check for IP Address column
                                if header == "IP Address" or header.lower().strip() == "ip address":
                                    ads_column_indices['ip_address'] = i + 1  # 1-indexed
                                    custom_print(f"Found 'IP Address' column in Ads Details at index {ads_column_indices['ip_address']}")
                            
                            # Find all rows in Ads Details worksheet that match this page_id
                            if 'page_id' in url_params:
                                # Update all matching rows in Ads Details worksheet
                                try:
                                    # Find column with Page ID or similar in Ads Details
                                    page_id_col_index = None
                                    for i, header in enumerate(ads_headers):
                                        if "page id" in header.lower() or "pageid" in header.lower().replace(" ", ""):
                                            page_id_col_index = i + 1  # 1-indexed
                                            custom_print(f"Found Page ID column in Ads Details at index {page_id_col_index}")
                                            break
                                    
                                    if page_id_col_index:
                                        # Get all Page ID values
                                        page_id_values = ads_details_worksheet.col_values(page_id_col_index)
                                        
                                        # Find rows with matching page_id
                                        matching_rows = []
                                        for i, cell_value in enumerate(page_id_values):
                                            if str(url_params['page_id']) in str(cell_value):
                                                matching_rows.append(i + 1)  # 1-indexed
                                        
                                        custom_print(f"Found {len(matching_rows)} matching rows in Ads Details worksheet")
                                        
                                        # Update Last Update Time and IP Address for all matching rows
                                        for row_idx in matching_rows:
                                            if 'last_update' in ads_column_indices:
                                                ads_details_worksheet.update_cell(row_idx, ads_column_indices['last_update'], current_time)
                                            
                                            if 'ip_address' in ads_column_indices:
                                                ads_details_worksheet.update_cell(row_idx, ads_column_indices['ip_address'], current_ip)
                                    
                                except Exception as e:
                                    custom_print(f"Error updating Ads Details worksheet for zero ads case: {e}", "error")
                        except Exception as e:
                            custom_print(f"Error working with Ads Details worksheet for zero ads case: {e}", "error")
                else:
                    custom_print(f"Could not find matching row in Milk worksheet for page_id: {url_params.get('page_id', 'unknown')}", "warning")
            except Exception as e:
                custom_print(f"Error updating Milk worksheet for zero ads case: {e}", "error")
        
        # Add this URL to processed_urls and continue to the next URL
        processed_urls.add(url)
        # Update progress after adding to processed_urls
        update_progress_percentage()
        continue
        
    # Check if this URL should be skipped (was already processed)
    if should_skip_url:
        custom_print(f"Skipping URL {url} as it was already processed")
        continue
    
    # Define variable for storing ad data from this URL
    ads_data = {}
    
    # Start scrolling to load content with human-like behavior since ads were found
    custom_print(f"Found {ad_count} ads. Starting human-like scrolling to load content...")
    
    # Initialize vars needed for our special end-of-results detection
    element_found = False
    
    # Only occasionally simulate minimal mouse movements before scrolling
    if random.random() < 0.15:  # 15% chance of brief pause
        custom_print("Performing minimal random mouse movements...")
        simulate_random_mouse_movements(driver, num_movements=random.randint(1, 3))
    else:
        custom_print("Skipping initial mouse movements to save time...")
    
    # Add a minimal random delay before starting to scroll
    delay = add_random_delays(0.2, 0.6)
    custom_print(f"Waiting {delay:.2f} seconds before starting to scroll...")
    
    # Perform ultra-fast scrolling with minimal pauses
    scroll_count = perform_human_like_scroll(
        driver, 
        scroll_pause_base=random.uniform(0.1, 0.5),  # Ultra-short pause time
        max_scroll_attempts=4                       # Minimal attempts at bottom
    )
    
    custom_print(f"Completed {scroll_count} human-like scrolls")
    
    # Check if we've reached the end of results using various possible end-of-results messages
    try:
        # Look for "End of results" text
        end_divs = driver.find_elements(By.XPATH, "//div[contains(text(), 'End of results')]")
        element_found = len(end_divs) > 0
        
        if not element_found:
            # Also check for "We couldn't find any more results"
            alt_end_divs = driver.find_elements(By.XPATH, "//div[contains(text(), 'We couldn')]/span[contains(text(), 'find any more results')]")
            element_found = len(alt_end_divs) > 0
            
            if not element_found:
                # Try another possible end message format
                try:
                    alt_end_divs_2 = driver.find_elements(By.XPATH, "//div[contains(@class, 'xu06os2')]/span[contains(text(), 'End of results')]")
                    element_found = len(alt_end_divs_2) > 0
                except (NoSuchElementException, TimeoutException):
                    pass
                    
    except Exception as e:
        custom_print(f"Error checking for end-of-results: {e}", "error")
        
    if element_found:
        custom_print(f"✅ End-of-list element found after {scroll_count} scrolls. Stopping scroll.")
        
    # Safety limit check (separate from human-like scrolling function)
    if scroll_count > 500: # Adjust limit as needed
        custom_print("⚠️ Reached maximum scroll limit (500). Stopping scroll.")
    
    try:
        # First try to find the element with role="heading" containing "results" with shorter timeout
        try:
            ad_count_element = WebDriverWait(driver, 3).until(
                EC.presence_of_element_located((By.XPATH, "//div[@role='heading'][contains(text(), 'results') or contains(text(), 'result')]"))
            )
        except TimeoutException:
            # Fall back to direct find without waiting
            ad_count_element = driver.find_element(By.XPATH, "//div[@role='heading'][contains(text(), 'results') or contains(text(), 'result')]")
        ad_count_text = ad_count_element.text.strip()
        custom_print(f"Found ad count text: {ad_count_text}")
        
        # Extract the numeric part using regex
        matches = re.search(r'~?(\d+(?:,\d+)?)', ad_count_text)
        if matches:
            # Remove commas and convert to int
            ad_count = int(matches.group(1).replace(',', ''))
            custom_print(f"Extracted ad count: {ad_count}")
        else:
            custom_print(f"Could not extract numeric ad count from: {ad_count_text}", "warning")
    except (NoSuchElementException, TimeoutException):
        # If the first method fails, try a more general approach
        try:
            ad_count_element = driver.find_element(By.XPATH, "//div[contains(text(), 'results') or contains(text(), 'result')]") 
            ad_count_text = ad_count_element.text.strip()
            custom_print(f"Found ad count text (alternate method): {ad_count_text}")
            
            # Extract the numeric part using regex
            matches = re.search(r'~?(\d+(?:,\d+)?)', ad_count_text)
            if matches:
                # Remove commas and convert to int
                ad_count = int(matches.group(1).replace(',', ''))
                custom_print(f"Extracted ad count: {ad_count}")
            else:
                custom_print(f"Could not extract numeric ad count from: {ad_count_text}", "warning")
        except NoSuchElementException:
            # Try JavaScript as a last resort
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
                    custom_print(f"Found ad count text (JavaScript method): {ad_count_text}")
                    matches = re.search(r'~?(\d+(?:,\d+)?)', ad_count_text)
                    if matches:
                        ad_count = int(matches.group(1).replace(',', ''))
                        custom_print(f"JavaScript-extracted ad count: {ad_count}")
            except Exception as js_error:
                custom_print(f"JavaScript ad count extraction failed: {str(js_error)}", "warning")
    except Exception as e:
        custom_print(f"Error extracting ad count: {e}", "error")
    
    # Process URL parameters to get the page_id for tracking in the Milk sheet
    url_params = {}
    try:
        # Extract page_id from URL
        if "view_all_page_id=" in url:
            page_id_match = re.search(r'view_all_page_id=(\d+)', url)
            if page_id_match:
                url_params['page_id'] = page_id_match.group(1)
                custom_print(f"Extracted page_id from URL: {url_params['page_id']}")
    except Exception as e:
        custom_print(f"Error extracting URL parameters: {e}", "error")
    
    # Update the Milk worksheet with the ad count, timestamp, and IP address
    if milk_worksheet:
        try:
            # Get the current headers to find column indices
            milk_headers = milk_worksheet.row_values(1)
            milk_column_indices = {}
            
            # Find the necessary column indices
            for i, header in enumerate(milk_headers):
                # Check for 'no.of ads By Ai' column (exact match with spaces)
                if header == "no.of ads By Ai":
                    milk_column_indices['ads_by_ai'] = i + 1  # 1-indexed
                    custom_print(f"Found 'no.of ads By Ai' column at index {milk_column_indices['ads_by_ai']}")
                
                # Check for Last Update Time column
                if header == "Last Update Time" or header.lower().strip() == "last update time":
                    milk_column_indices['last_update'] = i + 1  # 1-indexed
                    custom_print(f"Found 'Last Update Time' column at index {milk_column_indices['last_update']}")
                
                # Check for IP Address column
                if header == "IP Address" or header.lower().strip() == "ip address":
                    milk_column_indices['ip_address'] = i + 1  # 1-indexed
                    custom_print(f"Found 'IP Address' column at index {milk_column_indices['ip_address']}")
                
                # Find Page Transperancy column for matching
                if header == "Page Transperancy " or header == "Page Transperancy":
                    milk_column_indices['page_transperancy'] = i + 1  # 1-indexed
                    custom_print(f"Found 'Page Transperancy' column at index {milk_column_indices['page_transperancy']}")
                elif header.lower().strip() in ["page transperancy", "page transparency"]:
                    milk_column_indices['page_transperancy'] = i + 1  # 1-indexed
                    custom_print(f"Found 'Page Transperancy' column via fallback at index {milk_column_indices['page_transperancy']}")
            
            # Add any missing columns to Milk worksheet
            if 'ads_by_ai' not in milk_column_indices:
                next_col = len(milk_headers) + 1
                milk_worksheet.update_cell(1, next_col, "no.of ads By Ai")
                milk_column_indices['ads_by_ai'] = next_col
                custom_print(f"Added 'no.of ads By Ai' column at index {next_col}")
            
            if 'last_update' not in milk_column_indices:
                next_col = len(milk_headers) + 1
                milk_worksheet.update_cell(1, next_col, "Last Update Time")
                milk_column_indices['last_update'] = next_col
                custom_print(f"Added 'Last Update Time' column at index {next_col}")
            
            if 'ip_address' not in milk_column_indices:
                next_col = len(milk_headers) + 1
                milk_worksheet.update_cell(1, next_col, "IP Address")
                milk_column_indices['ip_address'] = next_col
                custom_print(f"Added 'IP Address' column at index {next_col}")
            
            # Find the matching row in Milk worksheet for this URL's page_id
            row_index = None
            
            if 'page_transperancy' in milk_column_indices and 'page_id' in url_params:
                # Get all values in the Page Transperancy column
                page_trans_values = milk_worksheet.col_values(milk_column_indices['page_transperancy'])
                
                # Look for a row that contains this page_id
                for i, cell_value in enumerate(page_trans_values):
                    if url_params['page_id'] in cell_value:
                        row_index = i + 1  # 1-indexed
                        custom_print(f"Found matching page_id in Milk worksheet at row {row_index}")
                        break
            
            if row_index:
                # Get current timestamp
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                # Update the no.of ads By Ai column - extract only the number
                if 'ads_by_ai' in milk_column_indices:
                    # If ad_count is found, use it, otherwise use 0
                    value_to_update = str(ad_count) if ad_count is not None else '0'
                    milk_worksheet.update_cell(row_index, milk_column_indices['ads_by_ai'], value_to_update)
                    custom_print(f"Updated 'no.of ads By Ai' column with value: {value_to_update}")
                
                # Update the Last Update Time column
                if 'last_update' in milk_column_indices:
                    milk_worksheet.update_cell(row_index, milk_column_indices['last_update'], current_time)
                    custom_print(f"Updated 'Last Update Time' column with value: {current_time}")
                
                # Update the IP Address column
                if 'ip_address' in milk_column_indices:
                    milk_worksheet.update_cell(row_index, milk_column_indices['ip_address'], current_ip)
                    custom_print(f"Updated 'IP Address' column with value: {current_ip}")
            else:
                custom_print(f"Could not find a matching row in the Milk worksheet for this URL", "warning")
        except Exception as e:
            custom_print(f"Error updating Milk worksheet: {e}", "error")
    
    # Add random delay after scrolling completes (more human-like)
    custom_print("Adding random delay after scrolling to appear more natural...")
    add_random_delays(2.0, 5.0)

    # Safety catch in case end-of-results text wasn't found
    if not element_found:
        custom_print("🏁 Reached bottom of page or scroll limit.")

    # Count divs with the first class (unchanged selector logic)
    target_class_1 = "x6s0dn4 x78zum5 xdt5ytf xl56j7k x1n2onr6 x1ja2u2z x19gl646 xbumo9q"
    try:
        divs_1 = driver.find_elements(By.CSS_SELECTOR, f'div[class="{target_class_1}"]')
        print(f"Total <div> elements with target class 1: {len(divs_1)}")
    except Exception as e:
        print(f"Error finding elements with target class 1: {e}")
        divs_1 = []

    # Count divs with the second class (unchanged selector logic)
    target_class_2 = "xrvj5dj x18m771g x1p5oq8j xbxaen2 x18d9i69 x1u72gb5 xtqikln x1na6gtj x1jr1mh3 xm39877 x7sq92a xxy4fzi"
    try:
        divs_2 = driver.find_elements(By.CSS_SELECTOR, f'div[class="{target_class_2}"]')
        print(f"Total <div> elements (ad groups) with target class 2: {len(divs_2)}")
    except Exception as e:
        print(f"Error finding elements with target class 2: {e}")
        divs_2 = []

    # Dictionary to store all ads data (unchanged)
    ads_data = {}

    # For each target_class_2 div, count xh8yej3 children and process them (unchanged logic, potential speedup from faster page load/scrolling)
    print("\nProcessing ads...")
    total_processed = 0
    total_child_ads_found = 0

    # --- Enhanced processing with human-like behavior ---
    custom_print("Adding random delay before processing ads to prevent detection...")
    add_random_delays(1.0, 2.0)  # Reduced delay
    
    for i, div in enumerate(divs_2, 1):
        # Randomize processing pattern (sometimes add delay between ads to look more human)
        if random.random() < 0.1:  # 10% chance
            delay = random.uniform(0.2, 0.5)  # Reduced delay
            custom_print(f"Taking a very short {delay:.1f}s break to maintain minimal human-like behavior...")
            time.sleep(delay)
            
            # Occasionally perform minimal mouse movements
            if random.random() < 0.2:  # 20% chance
                simulate_random_mouse_movements(driver, num_movements=1)  # Reduced movements
        
        try:
            # Detect potential anti-scraping challenges
            captcha_elements = driver.find_elements(By.XPATH, "//div[contains(text(), 'Security Check') or contains(text(), 'captcha') or contains(text(), 'Checkpoint')]")
            if captcha_elements:
                custom_print("⚠️ SECURITY CHECK DETECTED! Taking evasive action...", "warning")
                
                # Take screenshot of the security check for debugging
                try:
                    driver.save_screenshot(f"security_check_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
                    custom_print("Screenshot saved of security check")
                except Exception as e:
                    custom_print(f"Failed to save screenshot: {e}", "error")
                
                # If proxy is available, try to switch
                if proxy_manager:
                    custom_print("Attempting to switch proxies and retry...", "warning")
                    
                    # Close and restart driver with new proxy
                    driver.quit()
                    driver = create_stealth_driver(
                        use_proxy=True,
                        proxy_manager=proxy_manager,
                        headless=True
                    )
                    wait = WebDriverWait(driver, random.uniform(8, 12))
                    
                    # Try to reload current URL
                    driver.get(url)
                    time.sleep(random.uniform(5, 8))
                    
                    # Skip current ad group and continue with next URL
                    break
                else:
                    custom_print("No proxy manager available. Taking a long pause before continuing...", "warning")
                    time.sleep(random.uniform(30, 60))  # Long pause to avoid being blocked
            
            # Process ad normally if no security check detected
            custom_print(f"Processing ad group {i}...")
            child_divs = div.find_elements(By.XPATH, './div[contains(@class, "xh8yej3")]')
            num_children = len(child_divs)
            total_child_ads_found += num_children

            # Process each xh8yej3 child with more human-like behavior
            for j, child_div in enumerate(child_divs, 1):
                current_ad_id_for_logging = f"Group {i}, Ad {j}"
                library_id = None # Initialize library_id for potential error logging
                
                # Randomized tiny delays and interactions to look like a human investigating ads
                if random.random() < 0.15:  # 15% chance of brief pause
                    time.sleep(random.uniform(0.1, 0.8))
                
                # Occasionally 'inspect' the ad more carefully with mouse hover
                if random.random() < 0.2:  # 20% chance
                    try:
                        # Try to hover over the ad to look more human-like
                        actions = ActionChains(driver)
                        actions.move_to_element(child_div).perform()
                        time.sleep(random.uniform(0.2, 0.6))
                    except Exception:
                        # Don't worry if the hover fails, just continue
                        pass
                try:
                    main_container = child_div.find_element(By.XPATH, './/div[contains(@class, "x78zum5 xdt5ytf x2lwn1j xeuugli")]')

                    # Extract Library ID
                    library_id_element = main_container.find_element(By.XPATH, './/div[contains(@class, "x1rg5ohu x67bb7w")]/span[contains(text(), "Library ID:")]')
                    library_id = library_id_element.text.replace("Library ID: ", "").strip()
                    current_ad_id_for_logging = library_id # Update logging ID once found

                    # if library_id in ads_data:
                    #     # print(f"Skipping duplicate Library ID: {library_id}")
                    #     continue

                    # Initialize ad data with library_id
                    ad_data = {"library_id": library_id}
                    
                    # Check if we have a page name from the Milk worksheet for this URL
                    if url in page_names and page_names[url]:
                        # Use the page name from Milk worksheet
                        page_name = page_names[url]
                        ad_data["page_name"] = page_name
                        print(f"Using page name from Milk worksheet: {page_name}")
                    else:
                        # Fall back to extraction methods if no page name in worksheet
                        try:
                            # Find element with the specific class combination from screenshot
                            page_name_element = main_container.find_element(By.XPATH, 
                                './/div[contains(@class, "x8t9es0") and contains(@class, "x1ldc4aq") and contains(@class, "x1xlr1w8") and contains(@class, "x1cgboj8") and contains(@class, "x4hq6eo") and contains(@class, "xq9mrsl") and contains(@class, "x1yc453h") and contains(@class, "x1h4wwuj") and contains(@class, "xeuugli") and @role="heading"]')
                            
                            if page_name_element and page_name_element.text.strip():
                                page_name = page_name_element.text.strip()
                                ad_data["page_name"] = page_name
                                print(f"Found page name from heading: {page_name}")
                        except Exception as e:
                            print(f"Page name extraction method 1 failed: {str(e)}")
                            # Try alternative method: look for any heading element
                            try:
                                # Look for any heading role element
                                heading_element = main_container.find_element(By.XPATH, './/*[@role="heading"]')
                                if heading_element and heading_element.text.strip():
                                    page_name = heading_element.text.strip()
                                    ad_data["page_name"] = page_name
                                    print(f"Found page name from any heading: {page_name}")
                            except Exception as e2:
                                print(f"Page name extraction method 2 failed: {str(e2)}")
                                # Try extracting from the title
                                try:
                                    title = driver.title
                                    if title:
                                        # Common patterns include "Page Name - Ad Library" or "Ad Library - Page Name"
                                        if " - " in title:
                                            parts = title.split(" - ")
                                            # Usually the page name is the first part
                                            page_name = parts[0].strip()
                                            ad_data["page_name"] = page_name
                                            print(f"Found page name from title: {page_name}")
                                        else:
                                            # If no dash, use the whole title
                                            page_name = title.strip()
                                            ad_data["page_name"] = page_name
                                            print(f"Using whole title as page name: {page_name}")
                                except Exception as e3:
                                    print(f"Page name extraction method 3 failed: {str(e3)}")
                                    # Default if all methods fail
                                    ad_data["page_name"] = "Facebook Ad Page"

                    # Extract page ID from URL regardless of whether we have a page name
                    try:
                        # Try pattern 1: PAGEID parameter (case insensitive)
                        pageid_match = re.search(r'(?i)PAGEID=([0-9]+)', url)
                        if pageid_match:
                            page_id = pageid_match.group(1)
                            ad_data["page_id"] = page_id
                            print(f"Extracted page ID from PAGEID parameter: {page_id}")
                        else:
                            # Try pattern 2: view_all_page_id parameter
                            url_match = re.search(r'view_all_page_id=([0-9]+)', url)
                            if url_match:
                                page_id = url_match.group(1)
                                ad_data["page_id"] = page_id
                                print(f"Extracted page ID from view_all_page_id: {page_id}")
                            else:
                                # Try pattern 3: id parameter
                                id_match = re.search(r'[?&]id=([0-9]+)', url)
                                if id_match:
                                    page_id = id_match.group(1)
                                    ad_data["page_id"] = page_id
                                    print(f"Extracted page ID from id parameter: {page_id}")
                                else:
                                    # Try pattern 4: page_id parameter
                                    page_id_match = re.search(r'page_id=([0-9]+)', url)
                                    if page_id_match:
                                        page_id = page_id_match.group(1)
                                        ad_data["page_id"] = page_id
                                        print(f"Extracted page ID from page_id parameter: {page_id}")
                                    else:
                                        # Try pattern 5: any numeric sequence that could be an ID
                                        nums_match = re.search(r'/([0-9]{6,})/', url)
                                        if nums_match:
                                            page_id = nums_match.group(1)
                                            ad_data["page_id"] = page_id
                                            print(f"Extracted page ID from URL path: {page_id}")
                                        else:
                                            # If all the pattern matching fails, use the domain as a fallback
                                            domain_match = re.search(r'https?://(?:www\.)?([^/]+)', url)
                                            if domain_match:
                                                domain = domain_match.group(1)
                                                ad_data["page_id"] = domain
                                                print(f"Using domain as page ID: {domain}")
                                            else:
                                                ad_data["page_id"] = "unknown"
                                                print("Could not extract any page ID")
                    except Exception as e:
                        print(f"Could not extract page ID: {e}")
                        ad_data["page_id"] = "unknown"

                    # Extract Started running date
                    try:
                        started_running_element = main_container.find_element(By.XPATH, './/span[contains(text(), "Started running on")]')
                        full_text = started_running_element.text.strip()
                        
                        # Extract the started running date
                        started_running_match = re.search(r'Started running on (.*?)(?:·|$)', full_text)
                        if started_running_match:
                            started_running_text = started_running_match.group(1).strip()
                            # Try parsing with comma first, then without if that fails
                            try:
                                started_running_date = datetime.strptime(started_running_text, "%b %d, %Y").strftime("%Y-%m-%d")
                            except ValueError:
                                started_running_date = datetime.strptime(started_running_text, "%d %b %Y").strftime("%Y-%m-%d")
                            ad_data["started_running"] = started_running_date
                        else:
                            ad_data["started_running"] = None
                        
                        # Extract the total active time if present
                        active_time_match = re.search(r'Total active time\s+(.+?)(?:$|\s*·)', full_text)
                        if active_time_match:
                            active_time = active_time_match.group(1).strip()
                            ad_data["total_active_time"] = active_time
                        else:
                            ad_data["total_active_time"] = None
                        
                    except NoSuchElementException:
                        # print(f"Started running date not found for ad {current_ad_id_for_logging}")
                        ad_data["started_running"] = None
                        ad_data["total_active_time"] = None
                    except Exception as e:
                         print(f"⚠️ Error parsing started running date for ad {current_ad_id_for_logging}: {str(e)}")
                         ad_data["started_running"] = None
                         ad_data["total_active_time"] = None

                    # Extract Platforms icons
                    platforms_data = []
                    try:
                        platforms_div = main_container.find_element(By.XPATH, './/span[contains(text(), "Platforms")]/following-sibling::div[1]') # Use [1] for immediate sibling
                        platform_icons = platforms_div.find_elements(By.XPATH, './/div[contains(@class, "xtwfq29")]')

                        for icon in platform_icons:
                            try:
                                style = icon.get_attribute("style")
                                if not style: continue # Skip if no style attribute
                                mask_image_match = re.search(r'mask-image: url\("([^"]+)"\)', style)
                                mask_pos_match = re.search(r'mask-position: ([^;]+)', style)
                                mask_image = mask_image_match.group(1) if mask_image_match else None
                                mask_position = mask_pos_match.group(1).strip() if mask_pos_match else None # Added strip()

                                # Identify platform name
                                platform_name = PLATFORM_MAPPING.get((mask_image, mask_position)) # More direct lookup

                                platforms_data.append({
                                    # "style": style, # Usually not needed in final data
                                    "mask_image": mask_image,
                                    "mask_position": mask_position,
                                    "platform_name": platform_name if platform_name else "Unknown"
                                })
                            except Exception as e:
                                # print(f"Could not process a platform icon for ad {current_ad_id_for_logging}: {str(e)}")
                                continue
                    except NoSuchElementException:
                        # print(f"Platforms section not found for ad {current_ad_id_for_logging}")
                        pass # okay if this section is missing
                    except Exception as e:
                         print(f"Error extracting platforms for ad {current_ad_id_for_logging}: {str(e)}")

                    ad_data["platforms"] = platforms_data

                    # Extract Categories icon
                    category_data = []
                    try:
                        # Find the Categories span first
                        categories_span = main_container.find_element(By.XPATH, './/span[contains(text(), "Categories")]')
                        
                        # Find all sibling divs with class x1rg5ohu x67bb7w that come after the Categories span
                        category_divs = categories_span.find_elements(By.XPATH, './following-sibling::div[contains(@class, "x1rg5ohu") and contains(@class, "x67bb7w")]')
                        
                        for category_div in category_divs:
                            try:
                                # Try two different approaches to find category text
                                try:
                                    # First approach: look for text directly in the div
                                    category_text = category_div.text.strip()
                                    if category_text and category_text != "":
                                        category_data.append({
                                            "category_name": category_text
                                        })
                                        print(f"Found category text directly: {category_text}")
                                        continue  # Skip icon check if we found text
                                except Exception:
                                    pass  # Continue to icon approach if text approach fails
                                
                                # Second approach: try to map from icon
                                icon_div = category_div.find_element(By.XPATH, './/div[contains(@class, "xtwfq29")]')
                                style = icon_div.get_attribute("style")
                                
                                if style:
                                    mask_image_match = re.search(r'mask-image: url\("([^"]+)"\)', style)
                                    mask_pos_match = re.search(r'mask-position: ([^;]+)', style)
                                    mask_image = mask_image_match.group(1) if mask_image_match else None
                                    mask_position = mask_pos_match.group(1).strip() if mask_pos_match else None
                                    
                                    # Identify category name from mapping
                                    category_name = CATEGORY_MAPPING.get((mask_image, mask_position), "Unknown")
                                    
                                    # Only add if we got a real category name or there's a mask image
                                    if category_name != "Unknown" or mask_image:
                                        category_data.append({
                                            "mask_image": mask_image,
                                            "mask_position": mask_position,
                                            "category_name": category_name
                                        })
                                        print(f"Found category from icon: {category_name}")
                            except Exception as e:
                                print(f"Could not process a category div: {str(e)}")
                                continue
                                
                        # If no categories found, try another approach using the text
                        if not category_data:
                            try:
                                category_text = categories_span.find_element(By.XPATH, './following-sibling::span').text.strip()
                                if category_text:
                                    category_data.append({
                                        "category_name": category_text
                                    })
                                    print(f"Found category from sibling span: {category_text}")
                            except Exception:
                                pass
                                
                    except NoSuchElementException:
                        pass  # No categories section found
                    except Exception as e:
                        print(f"Error extracting categories: {str(e)}")

                    ad_data["categories"] = category_data
                    # Extract Ads count
                    try:
                        # Adjusted XPath to be more specific to the 'N ads use this creative and text.' structure
                        ads_count_element = main_container.find_element(By.XPATH, './/div[contains(@class, "x6s0dn4 x78zum5 xsag5q8")]//strong')
                        ads_count = ads_count_element.text.strip() # Should just be the number
                        number_match = re.search(r'(\d+)', ads_count)
                        if number_match:
                            ads_count = number_match.group(1)  # This will be just "4"
                        else:
                            ads_count = None
                        
                        ad_data["ads_count"] = ads_count
                    except NoSuchElementException:
                         # print(f"Ads count not found for ad {current_ad_id_for_logging}")
                         ad_data["ads_count"] = None
                    except Exception as e:
                        print(f"Error extracting ads count for ad {current_ad_id_for_logging}: {str(e)}")
                        ad_data["ads_count"] = None

                    # Extract Ad Text Content
                    try:
                        # Find the parent div containing the text first, more reliable
                        ad_text_container = child_div.find_element(By.XPATH, './/div[@data-ad-preview="message" or contains(@style, "white-space: pre-wrap")]')
                        # Get all text within, handles cases with multiple spans or line breaks better
                        ad_data["ad_text"] = ad_text_container.text.strip()
                    except NoSuchElementException:
                        # print(f"Ad text not found for ad {current_ad_id_for_logging}")
                        ad_data["ad_text"] = None
                    except Exception as e:
                         print(f"Error extracting ad text for ad {current_ad_id_for_logging}: {str(e)}")
                         ad_data["ad_text"] = None

                    try:
                        # First find the xh8yej3 div inside child_div if we're not already looking at it
                        xh8yej3_div = child_div
                        if "xh8yej3" not in child_div.get_attribute("class"):
                            xh8yej3_div = child_div.find_element(By.XPATH, './/div[contains(@class, "xh8yej3")]')
                        
                        # Try to find the link container first as it often contains both media and CTA
                        link_container = xh8yej3_div.find_element(By.XPATH, './/a[contains(@class, "x1hl2dhg") and contains(@class, "x1lku1pv")]')
                        
                        # Extract and store the link URL
                        link_url = link_container.get_attribute('href')
                        decoded_url = unquote(link_url)
                        
                        # Parse the URL to get the 'u' parameter value
                        parsed_url = urlparse(decoded_url)
                        query_params = parsed_url.query
                        if 'u=' in query_params:
                            # Get the full URL from the u parameter (properly decoded)
                            actual_url = unquote(query_params.split('u=')[1].split('&')[0])
                        else:
                            # Try another method if u= isn't in the query params
                            actual_url = unquote(decoded_url.split('u=')[1].split('&')[0]) if 'u=' in decoded_url else decoded_url
                        
                        # Store the full URL, not just the domain
                        ad_data["destination_url"] = actual_url


                        # Extract media from this link container
                        ad_data["media_type"] = None
                        ad_data["media_url"] = None
                        ad_data["thumbnail_url"] = None
                        
                        # Check for video within the link container
                        try:
                            video_element = child_div.find_element(By.XPATH, './/video')
                            media_url = video_element.get_attribute('src')
                            if media_url: # Ensure src is not empty
                               ad_data["media_type"] = "video"
                               ad_data["media_url"] = media_url
                               poster_url = video_element.get_attribute('poster')
                               if poster_url:
                                   ad_data["thumbnail_url"] = poster_url
                        except NoSuchElementException:
                            # If no video, try image with more specific targeting
                            try:
                                img_element = link_container.find_element(By.XPATH, './/img[contains(@class, "x168nmei") or contains(@class, "_8nqq")]')
                                media_url = img_element.get_attribute('src')
                                if media_url:
                                    ad_data["media_type"] = "image"
                                    ad_data["media_url"] = media_url
                            except NoSuchElementException:
                                # Fallback to any image within the link container
                                try:
                                    img_element = link_container.find_element(By.XPATH, './/img')
                                    media_url = img_element.get_attribute('src')
                                    if media_url:
                                        ad_data["media_type"] = "image"
                                        ad_data["media_url"] = media_url
                                except NoSuchElementException:
                                    pass  # No media found
                        
                        # Extract CTA Button text - look within the same link container first
                        try:
                            # Look for the CTA text within the link container
                            cta_text_element = link_container.find_element(By.XPATH, './/div[contains(@class, "x8t9es0") and contains(@class, "x1fvot60") and contains(@class, "xxio538")]')
                            cta_text = cta_text_element.text.strip()
                            
                            if cta_text:
                                ad_data["cta_button_text"] = cta_text
                            else:
                                # Fallback to older method if the text is empty
                                raise NoSuchElementException("Empty CTA text")
                                
                        except NoSuchElementException:
                            # Fallback to the original method if we can't find CTA in the link container
                            try:
                                cta_container = xh8yej3_div.find_element(By.XPATH, './/div[contains(@class, "x6s0dn4") and contains(@class, "x2izyaf")]')
                                cta_div = cta_container.find_element(By.XPATH, './/div[contains(@class, "x2lah0s")]')
                                cta_text_element = cta_div.find_element(By.XPATH, './/div[contains(@class, "x8t9es0") and contains(@class, "x1fvot60")]')
                                cta_text = cta_text_element.text.strip()
                                ad_data["cta_button_text"] = cta_text
                            except NoSuchElementException:
                                ad_data["cta_button_text"] = None
                        
                    except Exception as e:
                        print(f"Error extracting media or CTA for ad {current_ad_id_for_logging}: {str(e)}")
                        # Initialize with None if not already set
                        if "media_type" not in ad_data:
                            ad_data["media_type"] = None
                        if "media_url" not in ad_data:
                            ad_data["media_url"] = None
                        if "thumbnail_url" not in ad_data:
                            ad_data["thumbnail_url"] = None
                        if "cta_button_text" not in ad_data:
                            ad_data["cta_button_text"] = None
                    except Exception as e:
                         print(f"Error extracting media for ad {current_ad_id_for_logging}: {str(e)}")

                    # Extract CTA Button text
                    try:
                        # Find the div with the specific class that contains the CTA button
                        cta_container = child_div.find_element(By.XPATH, './/div[contains(@class, "x6s0dn4 x2izyaf x78zum5 x1qughib x168nmei x13lgxp2 x30kzoy x9jhf4c xexx8yu x1sxyh0 xwib8y2 xurb0ha")]')
                        
                        # Look for the button text within the second div (with class x2lah0s)
                        cta_div = cta_container.find_element(By.XPATH, './/div[contains(@class, "x2lah0s")]')
                        
                        # Find the text content within the button element
                        # This targets the text that's inside the button's visible content area
                        cta_text_element = cta_div.find_element(By.XPATH, './/div[contains(@class, "x8t9es0 x1fvot60 xxio538 x1heor9g xuxw1ft x6ikm8r x10wlt62 xlyipyv x1h4wwuj x1pd3egz xeuugli")]')
                        cta_text = cta_text_element.text.strip()
                        
                        ad_data["cta_button_text"] = cta_text
                    except NoSuchElementException:
                        # print(f"CTA button not found for ad {current_ad_id_for_logging}")
                        ad_data["cta_button_text"] = None
                    except Exception as e:
                        print(f"Error extracting CTA button text for ad {current_ad_id_for_logging}: {str(e)}")
                        ad_data["cta_button_text"] = None

                    # Add to main dictionary with library_id as key
                    ads_data[library_id] = ad_data
                    total_processed += 1
                    # Reduce console noise: print progress periodically instead of every ad
                    if total_processed % 50 == 0:
                        custom_print(f"Processed {total_processed}/{total_child_ads_found} ads...")

                except NoSuchElementException as e:
                    # This might happen if the structure is unexpected, often failure to find library ID
                    custom_print(f"WARNING: Critical element missing for ad {current_ad_id_for_logging}, skipping. Error: {e.msg}")
                    continue # Skip this child_div entirely if critical info (like ID) is missing
                except Exception as e:
                    custom_print(f"ERROR: Unexpected error processing ad {current_ad_id_for_logging}: {str(e)}")
                    continue # Skip this child_div on unexpected errors

        except Exception as e:
            custom_print(f"ERROR: Error finding or processing xh8yej3 children for div group {i}: {str(e)}")
            continue

    # End of scrolling, now extract the data
    custom_print(f"\nExtraction completed for URL {url_index}/{len(urls)}: Found {len(ads_data)} ads.")
    custom_print("Beginning data processing for Google Sheets update...")
    
    # Store data in memory (no need to create individual files)
    # This makes the process cleaner while still processing one URL at a time

    # Update the Ads Details worksheet with just this URL's data before moving to next URL
    if ads_worksheet and ads_data:
        custom_print(f"Updating Ads Details worksheet with {len(ads_data)} ads from URL {url_index}/{len(urls)}...")
        
        # Get column indices if we don't have them already
        # Always get the headers to ensure they're defined
        headers = ads_worksheet.row_values(1)  # Assuming headers are in the first row
        if url_index == 1:  # Only need to do this once
            column_indices = {}
            
            # Preserve exact column names with trailing spaces
            preserve_exact_columns = {
                "Page ",
                "Page Transperancy ", 
                "No. of ads", 
                "no.of ads By Ai",
                "IP Address"  # Add IP Address to preserved columns
            }
            
            # Print all headers for debugging
            custom_print(f"Found headers in sheet: {headers}")
            
            # Map column names to indices (1-indexed) with special handling for columns with trailing spaces
            for col_idx, header in enumerate(headers, 1):
                # Check if this is a header we want to preserve exactly
                if header in preserve_exact_columns:
                    column_indices[header] = col_idx
                    custom_print(f"Mapped exact column '{header}' to index {col_idx}")
                else:
                    # Also map the normalized version (lowercase, no spaces) for easier matching
                    normalized_header = header.lower().strip()
                    column_indices[normalized_header] = col_idx
                    # Keep the original mapping too
                    column_indices[header] = col_idx
                    custom_print(f"Mapped column '{header}' to index {col_idx}")
            
            custom_print(f"Found {len(column_indices)} column mappings in the sheet")
        
        # Check if IP Address column exists, if not add it
        ip_address_col_exists = False
        for header in headers:
            if header == "IP Address" or header.lower().strip() == "ip address":
                ip_address_col_exists = True
                break

        if not ip_address_col_exists:
            # Add IP Address column
            next_col = len(headers) + 1
            ads_worksheet.update_cell(1, next_col, "IP Address")
            column_indices["IP Address"] = next_col
            custom_print(f"Added 'IP Address' column at index {next_col}")
        
        # Prepare data for the sheet from this URL only
        rows_to_update = []
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        for library_id, ad_data in ads_data.items():
            row = []
            
            # Extract platform and category names for easier access
            # Convert platforms data to comma-separated string for platform_names column
            platform_names = []
            for platform in ad_data.get('platforms', []):
                if 'platform_name' in platform and platform['platform_name']:
                    platform_names.append(platform['platform_name'])
            ad_data['platform_names'] = ', '.join(platform_names) if platform_names else ''
            
            # Extract category names for categories column
            category_names = []
            for category in ad_data.get('categories', []):
                if 'category_name' in category and category['category_name'] != 'Unknown':
                    category_names.append(category['category_name'])
            ad_data['category_names'] = ', '.join(category_names) if category_names else ''
            
            # If no category names were found, but the URL contains keywords that suggest a category
            if not ad_data['category_names']:
                url = ad_data.get('destination_url', '').lower()
                page_name = ad_data.get('page_name', '').lower()
                
                # Look for category keywords in URL and page name
                finance_keywords = ['finance', 'money', 'loan', 'bank', 'insurance', 'invest', 'financial']
                employment_keywords = ['job', 'career', 'employment', 'hire', 'hiring', 'work', 'hustle']
                retail_keywords = ['shop', 'store', 'product', 'buy', 'retail', 'purchase']
                
                found_categories = []
                
                for keyword in finance_keywords:
                    if keyword in url or keyword in page_name:
                        found_categories.append('Financial products and services')
                        break
                        
                for keyword in employment_keywords:
                    if keyword in url or keyword in page_name:
                        found_categories.append('Employment')
                        break
                        
                for keyword in retail_keywords:
                    if keyword in url or keyword in page_name:
                        found_categories.append('Retail')
                        break
                
                # Update category_names if we inferred any
                if found_categories:
                    ad_data['category_names'] = ', '.join(found_categories)
                    custom_print(f"Inferred categories from URL/page name: {ad_data['category_names']}")
            
            # Debug info for critical columns
            custom_print(f"Data for library_id {library_id}:")
            custom_print(f"  - page_name: {ad_data.get('page_name', 'Facebook Ad Page')}")
            custom_print(f"  - page_id: {ad_data.get('page_id', 'unknown')}")
            custom_print(f"  - ads_count: {ad_data.get('ads_count', 1)}")
            custom_print(f"  - platform_names: {ad_data.get('platform_names', '')}")
            custom_print(f"  - category_names: {ad_data.get('category_names', '')}")
            
            # Create a row with cells for each column in the sheet
            row = [''] * len(headers)  # Initialize with empty strings for all columns
            
            # Identify and map the critical columns by checking various name patterns
            for col_idx, header in enumerate(headers, 0):  # 0-indexed for list
                # Page name columns
                if header in ["Name of page", "Page", "Page "] or header.lower().strip() in ["name of page", "page"]:
                    row[col_idx] = ad_data.get('page_name', 'Facebook Ad Page')
                    custom_print(f"Set column '{header}' to {row[col_idx]}")
                
                # Page ID / Transparency columns
                elif header in ["page id", "Page Transperancy", "Page Transperancy "] or header.lower().strip() in ["page id", "page transperancy"]:
                    row[col_idx] = ad_data.get('page_id', 'unknown')
                    custom_print(f"Set column '{header}' to {row[col_idx]}")
                
                # Ads count columns
                elif header in ["No. of ads", "no. of ads", "ads_count"] or header.lower().strip() in ["no. of ads", "ads count", "ads_count"]:
                    row[col_idx] = ad_data.get('ads_count', 1)  # Default to 1 if not specified
                    custom_print(f"Set column '{header}' to {row[col_idx]}")
                
                # no.of ads By Ai - special case with casing preserved
                elif header == "no.of ads By Ai":
                    row[col_idx] = ''
                
                # Last Update Time column
                elif header == "Last Update Time" or header.lower().strip() == "last update time":
                    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    row[col_idx] = current_time
                    custom_print(f"Set column '{header}' to {row[col_idx]}")
                
                # IP Address column
                elif header == "IP Address" or header.lower().strip() == "ip address":
                    row[col_idx] = current_ip
                    custom_print(f"Set column '{header}' to {row[col_idx]}")
                
                # Library ID column
                elif header.lower().strip() == "library_id":
                    row[col_idx] = library_id
                
                # Landing page column - format as HTML link
                elif header.lower().strip() in ["destination_url", "landing_page", "destination url", "landing page"]:
                    url = ad_data.get('destination_url', '')
                    if url:
                        # For Google Sheets, we can use hyperlink formula
                        # First make sure the URL has a scheme
                        if not url.startswith('http'):
                            url = 'https://' + url
                        # Just use the URL itself - Google Sheets will render this as a clickable link
                        row[col_idx] = url
                    else:
                        row[col_idx] = ''
                
                # Start time column
                elif header.lower().strip() in ["start_time", "start time"]:
                    row[col_idx] = ad_data.get('started_running', '')
                
                # Active time column
                elif header.lower().strip() in ["total_active_time", "total active time"]:
                    row[col_idx] = ad_data.get('total_active_time', '')
                
                # Ad text column
                elif header.lower().strip() in ["ad_text", "ad text"]:
                    row[col_idx] = ad_data.get('ad_text', '')
                
                # CTA button text column
                elif header.lower().strip() in ["cta_button_text", "cta button text"]:
                    row[col_idx] = ad_data.get('cta_button_text', '')
                
                # Media type column
                elif header.lower().strip() in ["media_type", "media type"]:
                    row[col_idx] = ad_data.get('media_type', '')
                
                # Platform names column
                elif header.lower().strip() in ["platform_names", "platform names", "platforms", "platform"]:
                    row[col_idx] = ad_data.get('platform_names', '')
                    custom_print(f"Set column '{header}' to {row[col_idx]}")
                
                # Media URL column
                elif header.lower().strip() in ["media_url", "media url"]:
                    row[col_idx] = ad_data.get('media_url', '')
                
                # Thumbnail URL column
                elif header.lower().strip() in ["thumbnail_url", "thumbnail url"]:
                    row[col_idx] = ad_data.get('thumbnail_url', '')
                
                # Source URL column - format as a link
                elif header.lower().strip() in ["source_url", "source url"]:
                    source_url = f"https://www.facebook.com/ads/library/?id={library_id}"
                    row[col_idx] = source_url  # Sheets will render this as a clickable link
                
                # Categories column
                elif header.lower().strip() in ["categories", "category"]:
                    row[col_idx] = ad_data.get('category_names', '')
                    custom_print(f"Set column '{header}' to {row[col_idx]}")
                
                # IP Address column
                elif header == "IP Address" or header.lower().strip() == "ip address":
                    row[col_idx] = current_ip
                
                # Last update columns
                elif header in ["Last Update Date", "Last Update Time"] or header.lower().strip() in ["last update date", "last update time"]:
                    row[col_idx] = timestamp
            
            rows_to_update.append(row)
        
        # Check for duplicates and update the sheet with this URL's data
        duplicate_checking_done = False
        if rows_to_update:
            try:
                # Get the current date for comparison (just the date part, not time)
                today_date = datetime.now().strftime("%Y-%m-%d")
                custom_print(f"Today's date for comparison: {today_date}")
                
                # Find library_id column in the sheet
                library_id_col_idx = None
                last_update_time_col_idx = None
                
                for col_idx, header in enumerate(headers, 1):
                    if header.lower().strip() == "library_id":
                        library_id_col_idx = col_idx
                        custom_print(f"Found library_id column at index {library_id_col_idx}")
                    elif header == "Last Update Time" or header.lower().strip() == "last update time":
                        last_update_time_col_idx = col_idx
                        custom_print(f"Found Last Update Time column at index {last_update_time_col_idx}")
                
                # Get existing library IDs and dates from the sheet
                existing_data = {}
                if library_id_col_idx and last_update_time_col_idx:
                    # Get all data from these columns (skipping the header row)
                    all_library_ids = ads_worksheet.col_values(library_id_col_idx)[1:]
                    all_update_times = ads_worksheet.col_values(last_update_time_col_idx)[1:]
                    
                    # Create a mapping of library_id to date (just the date part)
                    for i, lib_id in enumerate(all_library_ids):
                        if i < len(all_update_times):
                            # Extract just the date part from the timestamp (assuming format like "2025-05-09 16:30:45")
                            try:
                                date_part = all_update_times[i].split()[0] if ' ' in all_update_times[i] else all_update_times[i]
                                existing_data[lib_id] = date_part
                            except Exception:
                                # If we can't parse the date, just use the raw value
                                existing_data[lib_id] = all_update_times[i]
                
                # Filter rows_to_update to exclude rows with duplicate library_id and same date
                filtered_rows = []
                for row in rows_to_update:
                    # Find the library_id in this row (it's at the index that matches the column number - 1 since rows are 0-indexed)
                    lib_id_col = headers.index("library_id") if "library_id" in headers else None
                    if lib_id_col is None:
                        # Try case-insensitive search
                        for i, header in enumerate(headers):
                            if header.lower().strip() == "library_id":
                                lib_id_col = i
                                break
                    
                    if lib_id_col is not None and lib_id_col < len(row):
                        lib_id = row[lib_id_col]
                        
                        # Check if this library_id exists and has the same date
                        if lib_id in existing_data and existing_data[lib_id] == today_date:
                            custom_print(f"Skipping duplicate: Library ID {lib_id} already exists with today's date")
                        else:
                            custom_print(f"Adding new entry with Library ID: {lib_id}")
                            filtered_rows.append(row)
                    else:
                        # If we can't find the library_id in the row, include it anyway
                        filtered_rows.append(row)
                
                # Update count of rows after filtering
                skipped_count = len(rows_to_update) - len(filtered_rows)
                custom_print(f"Filtered out {skipped_count} duplicate entries")
                if skipped_count > 0:
                    custom_print(f"Skipped {skipped_count} ads that already exist with today's date")
                rows_to_update = filtered_rows
                
                if not rows_to_update:
                    custom_print("No new data to add after filtering duplicates")
                    # Skip the rest of this block since there's nothing to update
                    continue
                
                # Find the first empty row
                first_col_values = ads_worksheet.col_values(1)
                start_row = len(first_col_values) + 1
                custom_print(f"Appending data starting at row {start_row}")
                custom_print(f"Preparing to add {len(rows_to_update)} new rows to Google Sheet")
                
                # Append the rows
                ads_worksheet.append_rows(rows_to_update)
                custom_print(f"Successfully updated Google Sheet with {len(rows_to_update)} new ads from URL {url_index}/{len(urls)}.")
            except Exception as e:
                custom_print(f"Error updating Google Sheet: {e}")
                # Fallback to cell-by-cell update if batch update fails
                custom_print("Trying alternative cell-by-cell update method...")
                try:
                    for i, row_data in enumerate(rows_to_update):
                        row_idx = start_row + i
                        for j, value in enumerate(row_data):
                            if j < len(row_data):  # Ensure we don't go out of bounds
                                ads_worksheet.update_cell(row_idx, j+1, value)  # gspread is 1-indexed
                    custom_print("Successfully updated sheet using cell-by-cell method.")
                except Exception as e2:
                    custom_print(f"Error with cell-by-cell update: {e2}")
        else:
            custom_print(f"No data to update for URL {url_index}/{len(urls)}")
            
    # Add this URL to the set of processed URLs
    processed_urls.add(url)
    
    # Update progress after completing a URL
    if total_urls > 0:
        progress_percentage = (len(processed_urls) / total_urls) * 100
        # Format with GitHub Actions-friendly output that's easy to spot in logs
        # Use simple ASCII characters for progress bar to avoid encoding issues
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
        
    custom_print(f"Completed processing URL {url_index} ({len(processed_urls)} of {len(urls)} total)")
    custom_print("-------------------------------------------")

# Process is complete - all data has been added to the Google Sheet
custom_print("\nAll URLs have been processed successfully")

# Clean up
driver.quit()

total_time = time.time()
print(f"\nTotal script execution time: {total_time - time.time():.2f} seconds.")
