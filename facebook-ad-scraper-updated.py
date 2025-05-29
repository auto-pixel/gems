import os
import sys
import time
import logging
import re
import random
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
from selenium.webdriver.common.action_chains import ActionChains
from webdriver_manager.firefox import GeckoDriverManager
import gspread
from google.oauth2.service_account import Credentials
from google.auth.exceptions import GoogleAuthError

# Set up logging first so logger is available
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("fb_ad_scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Import anti-detection utilities
try:
    from fb_antidetect_utils import (
        ProxyManager,
        create_stealth_driver,
        perform_human_like_scroll,
        simulate_random_mouse_movements,
        add_random_delays,
        get_current_ip
    )
    ANTI_DETECT_UTILS_AVAILABLE = True
    logger.info("Successfully imported fb_antidetect_utils module")
except ImportError:
    logger.warning("fb_antidetect_utils module not found, using built-in fallbacks")
    ANTI_DETECT_UTILS_AVAILABLE = False
    # We'll define the functions below

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("fb_ad_scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Define fallback anti-detection utilities if needed
if not ANTI_DETECT_UTILS_AVAILABLE:
    logger.info("fb_antidetect_utils module not found, using built-in fallbacks")
    
    # Modern, realistic Firefox user agents
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:125.0) Gecko/20100101 Firefox/125.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:124.0) Gecko/20100101 Firefox/124.0",
        "Mozilla/5.0 (X11; Linux x86_64; rv:124.0) Gecko/20100101 Firefox/124.0"
    ]
    
    class ProxyManager:
        def __init__(self, proxy_file=None, proxies=None):
            self.proxies = []
            self.failed_proxies = set()
            self.current_index = 0
            
            # Load proxies from file if provided
            if proxy_file and os.path.exists(proxy_file):
                try:
                    with open(proxy_file, 'r') as f:
                        self.proxies = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
                except Exception as e:
                    logger.error(f"Error loading proxies from file: {e}")
            
            # Add direct proxies if provided
            if proxies and isinstance(proxies, list):
                self.proxies.extend(proxies)
                
            # Remove duplicates
            self.proxies = list(set(self.proxies))
            
            logger.info(f"ProxyManager initialized with {len(self.proxies)} proxies")
        
        def get_next_proxy(self):
            if not self.proxies:
                return None
                
            attempts = 0
            while attempts < len(self.proxies):
                self.current_index = (self.current_index + 1) % len(self.proxies)
                proxy = self.proxies[self.current_index]
                
                if proxy in self.failed_proxies:
                    attempts += 1
                    continue
                    
                return proxy
                    
            return None
        
        def mark_proxy_failed(self, proxy):
            if proxy in self.proxies:
                self.failed_proxies.add(proxy)
    
    def create_stealth_driver(use_proxy=False, proxy_manager=None, headless=True):
        options = Options()
        
        # Select random user agent
        user_agent = random.choice(USER_AGENTS)
        options.set_preference("general.useragent.override", user_agent)
        
        # Firefox-specific privacy settings
        options.set_preference("dom.webdriver.enabled", False)
        options.set_preference("privacy.trackingprotection.enabled", False)
        options.set_preference("browser.cache.disk.enable", True)
        options.set_preference("browser.cache.memory.enable", True)
        
        # Add headless mode if requested
        if headless:
            options.add_argument("--headless")
        
        # Common performance settings
        options.set_preference("permissions.default.image", 2)  # Block images for faster loading
        options.set_preference("dom.ipc.plugins.enabled.libflashplayer.so", False)  # Disable Flash
        options.set_preference("media.volume_scale", "0.0")  # Mute sounds
        options.add_argument("--width=1920")
        options.add_argument("--height=1080")
        
        # Add proxy if requested and available
        if use_proxy and proxy_manager:
            proxy = proxy_manager.get_next_proxy()
            if proxy:
                proxy_parts = proxy.split(':')
                if len(proxy_parts) == 2:
                    host, port = proxy_parts
                    options.set_preference("network.proxy.type", 1)
                    options.set_preference("network.proxy.http", host)
                    options.set_preference("network.proxy.http_port", int(port))
                    options.set_preference("network.proxy.ssl", host)
                    options.set_preference("network.proxy.ssl_port", int(port))
                    logger.info(f"Using proxy: {proxy}")
        
        # Create Firefox driver
        driver = webdriver.Firefox(service=Service(GeckoDriverManager().install()), options=options)
        
        # Inject JavaScript to modify navigator properties using executeScript
        driver.execute_script("""
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        """)
        
        return driver
    
    def simulate_random_mouse_movements(driver, num_movements=5):
        if not driver:
            return
        
        try:
            # Try to find the body element
            body_element = driver.find_element(By.TAG_NAME, "body")
            
            # Move to the body element
            actions = ActionChains(driver)
            actions.move_to_element(body_element)
            actions.perform()
            
            # Small random movements
            for _ in range(num_movements):
                offset_x = random.randint(-50, 50)
                offset_y = random.randint(-50, 50)
                
                try:
                    actions = ActionChains(driver)
                    actions.move_by_offset(offset_x, offset_y)
                    actions.perform()
                    time.sleep(random.uniform(0.1, 0.5))
                except Exception as e:
                    logger.warning(f"Mouse movement failed: {e}")
                    break
        except Exception as e:
            logger.warning(f"Could not perform mouse movements: {e}")
    
    def perform_human_like_scroll(driver, scroll_pause_base=3.0, max_scroll_attempts=3):
        scroll_count = 0
        last_height = driver.execute_script("return document.body.scrollHeight")
        attempts_at_bottom = 0
        
        while attempts_at_bottom < max_scroll_attempts:
            # Smooth scrolling
            current_position = driver.execute_script("return window.pageYOffset")
            viewport_height = driver.execute_script("return window.innerHeight")
            scroll_distance = random.uniform(0.7, 1.0) * viewport_height
            target_position = min(current_position + scroll_distance, last_height)
            
            steps = random.randint(5, 15)
            for step in range(1, steps + 1):
                next_pos = current_position + (target_position - current_position) * (step / steps)
                driver.execute_script(f"window.scrollTo(0, {next_pos})")
                time.sleep(random.uniform(0.01, 0.05))
            
            scroll_count += 1
            
            # Random pause between scrolls
            scroll_pause = random.uniform(scroll_pause_base * 0.7, scroll_pause_base * 1.5)
            time.sleep(scroll_pause)
            
            # Check if we've reached the bottom
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                attempts_at_bottom += 1
            else:
                attempts_at_bottom = 0
            
            last_height = new_height
            
            # Safety break
            if scroll_count >= 20:
                break
                
        return scroll_count
    
    def add_random_delays(min_delay=0.5, max_delay=2.0):
        delay = random.uniform(min_delay, max_delay)
        time.sleep(delay)
        return delay

class FacebookAdScraper:
    def __init__(self, sheet_name, worksheet_name, credentials_path="credentials.json", proxy_manager=None):
        """Initialize the Facebook Ad Scraper.
        
        Args:
            sheet_name (str): Name of the Google Sheets document
            worksheet_name (str): Name of the worksheet/tab within the document
            credentials_path (str): Path to Google API credentials
            proxy_manager (ProxyManager): Instance of ProxyManager for ScrapeOps proxy
        """
        self.sheet_name = sheet_name
        self.worksheet_name = worksheet_name
        self.credentials_path = credentials_path
        self.proxy_manager = proxy_manager  # Store the proxy_manager instance
        self.worksheet = None
        self.driver = None
        self.total_processed = 0
        self.successful_processed = 0
        self.failed_processed = 0
        
    def setup_google_sheets(self):
        """Set up connection to Google Sheets."""
        try:
            # Define the scope
            scope = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            
            # Authenticate with Google
            creds = Credentials.from_service_account_file(
                self.credentials_path, scopes=scope
            )
            client = gspread.authorize(creds)
            
            # Open the spreadsheet by name and the specific worksheet
            spreadsheet = client.open(self.sheet_name)
            self.worksheet = spreadsheet.worksheet(self.worksheet_name)
            
            logger.info(f"Successfully connected to Google Sheet: {self.sheet_name}, worksheet: {self.worksheet_name}")
            return True
            
        except GoogleAuthError as e:
            logger.error(f"Authentication error: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Error setting up Google Sheets: {str(e)}")
            return False
    
    def setup_selenium(self):
        """Set up Selenium WebDriver with advanced anti-detection measures."""
        try:
            # Check if we have a proxy_manager from the constructor
            if self.proxy_manager:
                use_proxy = True
            else:
                # Try to load proxies from file
                use_proxy = False
                proxy_file = "proxies.txt"  # Proxy file in ip:port format, one per line
                if os.path.exists(proxy_file):
                    logger.info("Initializing proxy manager with proxies from file")
                    try:
                        # Count the number of proxies in the file
                        with open(proxy_file, 'r') as f:
                            proxy_count = sum(1 for line in f if line.strip() and not line.strip().startswith('#'))
                        
                        if proxy_count > 0:
                            logger.info(f"Found {proxy_count} proxies in {proxy_file}")
                            self.proxy_manager = ProxyManager(proxy_file=proxy_file)
                            use_proxy = True
                        else:
                            logger.warning("No usable proxies found in proxy file. Using direct connection.")
                    except Exception as e:
                        logger.warning(f"Error loading proxies: {e}. Using direct connection.")
                else:
                    logger.info("No proxy file found. Using direct connection.")
            
            # Create the stealth driver with anti-detection measures
            logger.info("Creating stealth browser with anti-detection measures")
            self.driver = create_stealth_driver(
                use_proxy=use_proxy,
                proxy_manager=self.proxy_manager,
                headless=True,  # Set to False to see the browser in action
                target_url="https://www.facebook.com"  # Default target URL for setup
            )
            
            # Configure dynamic wait times (variable to appear more human-like)
            wait_time = random.uniform(8, 12)  # Random wait between 8-12 seconds
            self.wait = WebDriverWait(self.driver, wait_time)
            
            # Initialize request counter for IP rotation
            self.request_count = 0
            self.max_requests_per_ip = random.randint(8, 12)
            
            logger.info("Anti-detection Selenium WebDriver initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error setting up Selenium: {str(e)}")
            return False
    
    def get_page_id_from_page(self, page_url, page_name):
        """Navigate to the Facebook page, go to About section, and extract Page ID from transparency.
        
        Args:
            page_url (str): URL of the Facebook page
            page_name (str): Name of the Facebook page
            
        Returns:
            tuple: (page_id, transparency_url) or (None, None) if extraction fails
        """
        if not page_url or not isinstance(page_url, str):
            logger.warning(f"Invalid page URL for '{page_name}': {page_url}")
            return None, None
        
        if not page_url.startswith("http"):
            page_url = f"https://{page_url}" if not page_url.startswith("www.") else f"https://{page_url}"
        
        try:
            logger.info(f"Navigating to page URL for '{page_name}': {page_url}")
        
            # Check if we're using ScrapeOps proxy
            if hasattr(self, 'proxy_manager') and hasattr(self.proxy_manager, 'get_proxy_url'):
                scrapeops_url = self.proxy_manager.get_proxy_url(page_url)
                logger.info(f"Using ScrapeOps.io proxy API for page navigation")
                self.driver.get(scrapeops_url)
            else:
                # Fallback to direct navigation
                self.driver.get(page_url)
                time.sleep(5)  # Allow page to fully load

            # If already on the about_profile_transparency page, skip all clicking
            if '/about_profile_transparency' not in page_url:
                # Click on About tab
                try:
                    about_tab = WebDriverWait(self.driver, 15).until(
                        EC.element_to_be_clickable((By.XPATH, "//a[contains(text(), 'About')]"))
                    )
                    self.driver.execute_script("arguments[0].click();", about_tab)  # JavaScript click for better reliability
                    logger.info(f"Clicked on About tab for '{page_name}'")
                    time.sleep(3)  # Wait for About page to load
                except Exception as e:
                    logger.warning(f"Could not click on About tab for '{page_name}': {str(e)}")
                    # Try direct navigation to about page
                    about_url = f"{page_url.rstrip('/')}/about"
                    logger.info(f"Trying direct navigation to About page: {about_url}")
                    
                    # Check if we're using ScrapeOps proxy - no fallback to direct navigation
                    if hasattr(self, 'proxy_manager') and hasattr(self.proxy_manager, 'get_proxy_url'):
                        scrapeops_url = self.proxy_manager.get_proxy_url(about_url)
                        logger.info(f"Using ScrapeOps.io proxy API for about page navigation")
                        self.driver.get(scrapeops_url)
                        time.sleep(3)
                    else:
                        # No fallback - terminate if ScrapeOps isn't available
                        logger.error("ERROR: ScrapeOps proxy unavailable - terminating to protect your IP")
                        sys.exit(1)

                # Click on Page transparency section
                try:
                    transparency_section = WebDriverWait(self.driver, 15).until(
                        EC.element_to_be_clickable((By.XPATH, "//a[contains(text(), 'Page transparency') or contains(text(), 'Page Transparency')]"))
                    )
                    self.driver.execute_script("arguments[0].click();", transparency_section)  # JavaScript click
                    logger.info(f"Clicked on Page transparency section for '{page_name}'")
                    time.sleep(3)  # Wait for transparency info to load
                except Exception as e:
                    logger.warning(f"Could not click on Page transparency section for '{page_name}': {str(e)}")
                    return None, None

            # Extract Page ID using multiple methods for robustness
            try:
                # Method 1: Using direct XPath - looking for a span near "Page ID" text
                try:
                    # First try to find "Page ID" label
                    page_id_label = WebDriverWait(self.driver, 5).until(
                        EC.presence_of_element_located((By.XPATH, "//div[contains(text(), 'Page ID')]"))
                    )
                    # Then find the actual ID (following sibling or nearby element)
                    page_id_element = WebDriverWait(self.driver, 5).until(
                        EC.presence_of_element_located((By.XPATH, "//div[contains(text(), 'Page ID')]/following-sibling::div"))
                    )
                    page_id = page_id_element.text.strip()
                    logger.info(f"Method 1: Extracted Page ID for '{page_name}': {page_id}")
                except (TimeoutException, NoSuchElementException, StaleElementReferenceException):
                    page_id = None
                
                # Method 2: JavaScript method to extract content including pseudo-elements
                if not page_id or not page_id.isdigit():
                    try:
                        # Look for any span that might contain the ID (numeric-only text)
                        page_id = self.driver.execute_script("""
                            // Try different selectors that might contain the page ID
                            const selectors = [
                                'span[class*="193iq5w"]',
                                'span[dir="auto"]',
                                'div[class*="xzsf02u"]',
                                'div'  // As a last resort, check all divs for numeric content
                            ];
                            
                            for (const selector of selectors) {
                                const elements = document.querySelectorAll(selector);
                                for (const el of elements) {
                                    const text = el.textContent.trim();
                                    // Looking for a numeric-only string that's likely to be an ID
                                    if (/^\\d{12,}$/.test(text)) {
                                        return text;
                                    }
                                }
                            }
                            return null;
                        """)
                        if page_id:
                            logger.info(f"Method 2: Extracted Page ID for '{page_name}': {page_id}")
                    except Exception as js_error:
                        logger.warning(f"JavaScript extraction failed: {str(js_error)}")
                
                # Method 3: Take a screenshot and log HTML if previous methods failed
                if not page_id or not page_id.isdigit():
                    # Save screenshot for debugging
                    screenshot_path = f"debug_{page_name.replace(' ', '_')}.png"
                    self.driver.save_screenshot(screenshot_path)
                    logger.warning(f"Could not extract Page ID using standard methods. Screenshot saved to {screenshot_path}")
                    
                    # Get and log page HTML for manual inspection
                    html = self.driver.page_source
                    with open(f"debug_{page_name.replace(' ', '_')}.html", "w", encoding="utf-8") as f:
                        f.write(html)
                    
                    # Final attempt: look for any numeric string that looks like an ID in the HTML
                    id_match = re.search(r'>\s*(\d{12,})\s*<', html)
                    if id_match:
                        page_id = id_match.group(1)
                        logger.info(f"Method 3: Extracted Page ID from HTML for '{page_name}': {page_id}")
                
                if page_id and page_id.isdigit():
                    # Construct the transparency URL with the page ID
                    transparency_url = f"https://www.facebook.com/ads/library/?active_status=active&ad_type=all&country=ALL&is_targeted_country=false&media_type=all&search_type=page&source=page-transparency-widget&view_all_page_id={page_id}"
                    logger.info(f"Constructed transparency URL for '{page_name}': {transparency_url}")
                    return page_id, transparency_url
                else:
                    logger.warning(f"Could not extract valid Page ID for '{page_name}'")
                    return None, None
                    
            except Exception as e:
                logger.warning(f"Could not extract Page ID for '{page_name}': {str(e)}")
                return None, None
                
        except Exception as e:
            logger.error(f"Error navigating to page URL for '{page_name}': {str(e)}")
            return None, None
    
    def extract_ad_count(self, url, page_name):
        """Extract ad count from a Facebook Page Transparency URL.
        
        Args:
            url (str): Facebook Page Transparency URL
            page_name (str): Name of the Facebook page
            
        Returns:
            int or None: Number of ads or None if extraction fails
        """
        if not url or not isinstance(url, str):
            logger.warning(f"Invalid URL for page '{page_name}': {url}")
            return None
            
        try:
            logger.info(f"Processing page: '{page_name}'")
            logger.info(f"Opening URL: {url}")
            
            # Navigate to the URL directly
            self.driver.get(url)
            time.sleep(5)  # Give more time for content to load
            
            # Wait for content to load (the ad count element)
            wait = WebDriverWait(self.driver, 15)
            
            try:
                # Try to find the ad count element first
                ad_count_element = wait.until(
                    EC.presence_of_element_located((By.XPATH, "//div[contains(text(), 'results') or contains(text(), 'result')]"))
                )
                ad_count_text = ad_count_element.text
                logger.info(f"Found ad count text: {ad_count_text}")
                
                # Extract the numeric part using regex
                matches = re.search(r'~?(\d+(?:,\d+)?)', ad_count_text)
                if matches:
                    # Remove commas and convert to int
                    ad_count = int(matches.group(1).replace(',', ''))
                    logger.info(f"Extracted ad count for '{page_name}': {ad_count}")
                    return ad_count
                else:
                    logger.warning(f"Could not extract numeric ad count from: {ad_count_text}")
                    return 0
            except TimeoutException:
                # Check if "No ads" message is present
                try:
                    no_ads_element = self.driver.find_element(By.XPATH, "//div[contains(text(), 'No ads')]")
                    logger.info(f"Page '{page_name}' has no ads")
                    return 0
                except NoSuchElementException:
                    # Try JavaScript as a last resort
                    try:
                        ad_count_text = self.driver.execute_script("""
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
                                logger.info(f"JavaScript-extracted ad count for '{page_name}': {ad_count}")
                                return ad_count
                    except Exception as js_error:
                        logger.warning(f"JavaScript ad count extraction failed: {str(js_error)}")
                
                # If we get here, we couldn't find the ad count
                logger.warning(f"Could not find ad count or 'No ads' message for '{page_name}'")
                return None
                
            # Handle navigation to about page if needed
            except Exception as e:
                logger.warning(f"Error processing page: {str(e)}")
                about_url = f"{url.rstrip('/')}/about"
                logger.info(f"Trying navigation to About page: {about_url}")
                
                # Navigate directly to about page
                self.driver.get(about_url)
                time.sleep(5)  # Give more time for content to load
                
                # Click on Page transparency section
                try:
                    transparency_section = WebDriverWait(self.driver, 15).until(
                        EC.element_to_be_clickable((By.XPATH, "//a[contains(text(), 'Page transparency') or contains(text(), 'Page Transparency')]"))
                    )
                    self.driver.execute_script("arguments[0].click();", transparency_section)  # JavaScript click
                    logger.info(f"Clicked on Page transparency section for '{page_name}'")
                    time.sleep(3)  # Wait for transparency info to load
                    
                    # Return and parse ad count
                    # This section would need to be implemented based on how the page structure looks
                    return None
                except Exception as e:
                    logger.warning(f"Could not access transparency info: {str(e)}")
                    return None
                    
        except Exception as e:
            logger.error(f"Unexpected error in extract_ad_count: {str(e)}")
            return None

    def process_sheet(self):
        """Process all rows in the Google Sheet with advanced anti-detection measures."""
        try:
            # Set up connections to Google Sheets and Selenium WebDriver
            if not self.setup_google_sheets():
                logger.error("Failed to set up Google Sheets. Exiting.")
                return
                
            if not self.setup_selenium():
                logger.error("Failed to set up Selenium. Exiting.")
                return
                
            # Initialize counters
            self.total_processed = 0
            self.successful_processed = 0
            self.failed_processed = 0
            
            # Get all values from the worksheet
            all_values = self.worksheet.get_all_values()
            
            if not all_values:
                logger.error("Worksheet is empty. Exiting.")
                return
                
            # Get headers and find required columns
            headers = all_values[0]
            logger.info(f"Found headers: {headers}")
            
            # Handle column names carefully, with proper handling of trailing spaces
            # Create a case-insensitive mapping with original column names preserved
            column_mapping = {}
            for idx, header in enumerate(headers):
                # Store with the original name for reference
                column_mapping[header] = idx  
                # Also store lowercase version for case-insensitive matching
                column_mapping[header.lower().strip()] = idx
            
            logger.info(f"Column mapping: {column_mapping}")
            
            # Find the Page link column directly or with case-insensitive search
            page_link_col_idx = None
            if 'Page link' in column_mapping:
                page_link_col_idx = column_mapping['Page link']
                logger.info(f"Found 'Page link' column at index {page_link_col_idx}")
            else:
                # Try case-insensitive
                if 'page link' in column_mapping:
                    page_link_col_idx = column_mapping['page link']
                    logger.info(f"Found 'Page link' column (case-insensitive) at index {page_link_col_idx}")
            
            # Find the Page Transparency column (with or without trailing space)
            url_col_idx = None
            transparency_variants = ['Page Transperancy', 'Page Transperancy ', 'Page Transparency', 'Page Transparency ']
            for variant in transparency_variants:
                if variant in column_mapping:
                    url_col_idx = column_mapping[variant]
                    logger.info(f"Found '{variant}' column at index {url_col_idx}")
                    break
                    
            if url_col_idx is None:
                # Try case-insensitive
                lowercase_variants = ['page transperancy', 'page transparency']
                for variant in lowercase_variants:
                    if variant in column_mapping:
                        url_col_idx = column_mapping[variant]
                        logger.info(f"Found transparency column (case-insensitive) at index {url_col_idx}")
                        break
            
            # If still not found, try last resort methods
            if page_link_col_idx is None or url_col_idx is None:
                # Last resort: try to find columns by position or partial match
                if page_link_col_idx is None:
                    for idx, header in enumerate(headers):
                        if "page link" in header.lower() or "page url" in header.lower():
                            page_link_col_idx = idx
                            logger.info(f"Found Page link column using partial match at index {idx}: '{header}'")
                            break

                if url_col_idx is None:
                    for idx, header in enumerate(headers):
                        if "transperancy" in header.lower() or "transparency" in header.lower():
                            url_col_idx = idx
                            logger.info(f"Found transparency column using partial match at index {idx}: '{header}'")
                            break

            # Check if we found the required columns
            if page_link_col_idx is None or url_col_idx is None:
                logger.error("Required columns not found. Please check your sheet headers.")
                logger.error(f"Available columns are: {headers}")
                return
                
            # Find or create the ad count column
            ads_col_idx = None
            for idx, header in enumerate(headers):
                if "no.of ads by ai" in header.lower():
                    ads_col_idx = idx
                    logger.info(f"Found 'no.of ads By Ai' column at index {idx}: '{header}'")
                    break
                    
            # If ad count column not found, create it
            if ads_col_idx is None:
                ads_col_idx = len(headers)
                self.worksheet.update_cell(1, ads_col_idx + 1, "no.of ads By Ai") # Sheets are 1-indexed
                logger.info(f"Created 'no.of ads By Ai' column at index {ads_col_idx}")
                
            # Find or create the timestamp column
            time_col_idx = None
            for idx, header in enumerate(headers):
                if "last update time" in header.lower():
                    time_col_idx = idx
                    logger.info(f"Found 'Last Update Time' column at index {idx}: '{header}'")
                    break
                    
            # If timestamp column not found, create it
            if time_col_idx is None:
                time_col_idx = len(headers) if ads_col_idx < len(headers) else ads_col_idx + 1
                self.worksheet.update_cell(1, time_col_idx + 1, "Last Update Time")
                logger.info(f"Created 'Last Update Time' column at index {time_col_idx}")
            
            # Collect all valid rows to process
            rows_to_process = []
            for row_idx, row in enumerate(all_values[1:], start=2): # Sheets are 1-indexed, skip header
                # Skip rows that don't have enough columns
                max_needed_col = max(page_link_col_idx, url_col_idx)
                if len(row) <= max_needed_col:
                    logger.warning(f"Row {row_idx} has insufficient columns. Skipping.")
                    continue
                    
                page_link = row[page_link_col_idx] if page_link_col_idx < len(row) else ""
                transparency_url = row[url_col_idx] if url_col_idx < len(row) else ""
                
                # Rule 2: If both are blank, skip
                if not page_link.strip() and not transparency_url.strip():
                    logger.warning(f"Both Page link and Page Transperancy are blank at row {row_idx}. Skipping.")
                    continue
                    
                rows_to_process.append((row_idx, page_link, transparency_url))
                
            # Randomize processing order to avoid detection patterns
            logger.info(f"Found {len(rows_to_process)} rows to process. Randomizing order for anti-detection.")
            random.shuffle(rows_to_process)
            
            # Process each row with anti-detection measures
            for i, (row_idx, page_link, transparency_url) in enumerate(rows_to_process):
                try:
                    # Add cooling period between requests (more random than original 2 seconds)
                    if i > 0:
                        cooling_time = random.uniform(10, 20) # 10-20 seconds
                        logger.info(f"Adding cooling period of {cooling_time:.1f} seconds...")
                        time.sleep(cooling_time)
                    
                    logger.info(f"Processing row {row_idx} ({i+1}/{len(rows_to_process)})")
                    
                    # Apply IP rotation after certain number of requests
                    self.request_count = getattr(self, 'request_count', 0) + 1
                    max_requests = getattr(self, 'max_requests_per_ip', random.randint(8, 12))
                    
                    if self.request_count >= max_requests and hasattr(self, 'rotate_ip'):
                        logger.info("Rotation limit reached. Rotating IP for safety...")
                        self.rotate_ip()
                        self.request_count = 0
                    
                    # Rule 1: If Page Transperancy is blank and Page link is not blank
                    if not transparency_url.strip() and page_link.strip():
                        about_transparency_url = f"{page_link.rstrip('/')}/about_profile_transparency"
                        logger.info(f"No transparency URL for row {row_idx}, using {about_transparency_url}")
                        
                        # Get page ID from the page
                        page_id, constructed_url = self.get_page_id_from_page(about_transparency_url, page_link)
                        if page_id and constructed_url:
                            logger.info(f"Extracted Page ID for row {row_idx}: {page_id}")
                            logger.info(f"Constructed transparency URL for row {row_idx}: {constructed_url}")
                            self.worksheet.update_cell(row_idx, url_col_idx + 1, constructed_url)
                            logger.info(f"Updated Page Transperancy for row {row_idx} with {constructed_url}")
                            transparency_url = constructed_url  # Use this for ad count extraction
                        else:
                            logger.warning(f"Could not extract page id for row {row_idx} from {about_transparency_url}")
                            self.failed_processed += 1
                            continue
                    
                    # Extract ad count
                    ad_count = self.extract_ad_count(transparency_url, page_link)
                    
                    # Update the sheet if we got an ad count
                    if ad_count is not None:
                        # Add realistic variability to timing
                        if ANTI_DETECT_UTILS_AVAILABLE:
                            add_random_delays(0.5, 1.5)
                        else:
                            time.sleep(random.uniform(0.5, 1.5))
                            
                        # Update ad count
                        self.worksheet.update_cell(row_idx, ads_col_idx + 1, ad_count)
                        
                        # Update timestamp
                        current_time = time.strftime("%Y-%m-%d %H:%M:%S")
                        self.worksheet.update_cell(row_idx, time_col_idx + 1, current_time)
                        
                        logger.info(f"Updated ad count for row {row_idx}: {ad_count} and timestamp: {current_time}")
                        self.successful_processed += 1
                    else:
                        logger.warning(f"Failed to extract ad count for row {row_idx}")
                        self.failed_processed += 1
                except Exception as e:
                    logger.error(f"Error processing row {row_idx}: {str(e)}")
                    self.failed_processed += 1
                
                self.total_processed += 1
            
            logger.info(f"Processing complete. Processed {self.total_processed} URLs "
                      f"({self.successful_processed} successful, {self.failed_processed} failed).")
        
        except Exception as e:
            logger.error(f"Error processing worksheet: {str(e)}")
            time_col_idx = None
            for idx, header in enumerate(headers):
                if "last update time" in header.lower():
                    time_col_idx = idx
                    break
                    
            if time_col_idx is None:
                time_col_idx = len(headers) + (1 if ads_col_idx == len(headers) else 0)
                self.worksheet.update_cell(1, time_col_idx + 1, "Last Update Time")
                logger.info("Added 'Last Update Time' column")

            # Process each row starting from the second row (index 1)
            for row_idx, row in enumerate(all_values[1:], start=2):  # Start from 2 because sheets are 1-indexed and we skip headers
                try:
                    # Skip rows that don't have enough columns
                    if len(row) <= max(page_link_col_idx, url_col_idx):
                        logger.warning(f"Row {row_idx} has insufficient columns. Skipping.")
                        self.failed_processed += 1
                        continue

                    # Get page link from 'Page link' column only
                    page_link = row[page_link_col_idx] if page_link_col_idx < len(row) else ""
                    transparency_url = row[url_col_idx] if url_col_idx < len(row) else ""

                    # Rule 1: If Page Transperancy is blank and Page link is not blank
                    if not transparency_url.strip() and page_link.strip():
                        about_transparency_url = f"{page_link.rstrip('/')}/about_profile_transparency"
                        logger.info(f"Row {row_idx}: Page Transperancy is blank, visiting {about_transparency_url} to extract page id.")
                        page_id, constructed_url = self.get_page_id_from_page(about_transparency_url, page_link)
                        if page_id and constructed_url:
                            logger.info(f"Extracted Page ID for row {row_idx}: {page_id}")
                            logger.info(f"Constructed transparency URL for row {row_idx}: {constructed_url}")
                            self.worksheet.update_cell(row_idx, url_col_idx + 1, constructed_url)
                            logger.info(f"Updated Page Transperancy for row {row_idx} with {constructed_url}")
                            transparency_url = constructed_url  # Use this for ad count extraction
                        else:
                            logger.warning(f"Could not extract page id for row {row_idx} from {about_transparency_url}")
                            self.failed_processed += 1
                            continue
                    # Rule 2: If both are blank, skip
                    elif not page_link.strip() and not transparency_url.strip():
                        logger.warning(f"Both Page link and Page Transperancy are blank at row {row_idx}. Skipping.")
                        self.failed_processed += 1
                        continue
                    # Rule 3: If Page Transperancy is not blank, use it directly
                    # (no action needed, just proceed)

                    # Extract ad count using the (possibly updated) transparency_url
                    ad_count = self.extract_ad_count(transparency_url, page_link or f"Row {row_idx}")

                    # Update the sheet
                    if ad_count is not None:
                        # Write to the ads column
                        self.worksheet.update_cell(row_idx, ads_col_idx + 1, ad_count)
                        # Update the Last Update Time column with current timestamp
                        current_time = time.strftime("%Y-%m-%d %H:%M:%S")
                        self.worksheet.update_cell(row_idx, time_col_idx + 1, current_time)
                        logger.info(f"Updated ad count for row {row_idx}: {ad_count} and timestamp: {current_time}")
                        self.successful_processed += 1
                    else:
                        logger.warning(f"Failed to extract ad count for row {row_idx}")
                        self.failed_processed += 1
                    # Add delay to avoid rate limiting
                    time.sleep(2)
                except Exception as e:
                    logger.error(f"Error processing row {row_idx}: {str(e)}")
                    self.failed_processed += 1
                self.total_processed += 1

            logger.info(f"Processing complete. Processed {self.total_processed} URLs "
                        f"({self.successful_processed} successful, {self.failed_processed} failed).")

        except Exception as e:
            logger.error(f"Error processing worksheet: {str(e)}")
        finally:
            if self.driver:
                self.driver.quit()
                logger.info("WebDriver closed")

def main():
    """Main function to run the scraper."""
    import argparse
    import sys
    
    try:
        # Import required modules
        from fb_antidetect_utils import ProxyManager, get_current_ip
        
        # Initialize the ProxyManager (optional, for traditional proxies)
        proxy_manager = ProxyManager()
        
        # Get and log the current IP
        current_ip = get_current_ip()
        logger.info(f"Using IP: {current_ip} for scraping")
        
        parser = argparse.ArgumentParser(description='Facebook Ad Scraper')
        parser.add_argument('--sheet_name', default='Master Auto Swipe - Test ankur',
                            help='Name of the Google Sheets document (default: Master Auto Swipe - Test ankur)')
        parser.add_argument('--worksheet_name', default='Milk',
                            help='Name of the worksheet/tab within the document (default: Milk)')
        parser.add_argument('--credentials', default='credentials.json', 
                            help='Path to Google API credentials file (default: credentials.json)')
        
        args = parser.parse_args()
        
        logger.info("Starting Facebook Ad Scraper")
        
        # Create FacebookAdScraper with the proxy manager
        scraper = FacebookAdScraper(args.sheet_name, args.worksheet_name, args.credentials, proxy_manager=proxy_manager)
        
        scraper.process_sheet()
        logger.info("Facebook Ad Scraper completed")
    except Exception as e:
        logger.error(f"Error in Facebook Ad Scraper: {e}")
        logger.error("Terminating script to protect your IP address")
        sys.exit(1)

if __name__ == "__main__":
    main()