import undetected_chromedriver as uc
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException, ElementNotInteractableException
from selenium.webdriver.common.action_chains import ActionChains
from webdriver_manager.chrome import ChromeDriverManager
from fake_useragent import UserAgent
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
from copy import deepcopy

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

# Function to handle popups, particularly the ad blocker popup
def handle_popups(driver, wait=None):
    """
    Detect and handle Facebook popups, including ad blocker warnings
    
    Args:
        driver: The Selenium WebDriver instance
        wait: Optional WebDriverWait instance (will create one if not provided)
        
    Returns:
        bool: True if a popup was detected and handled, False otherwise
    """
    if wait is None:
        wait = WebDriverWait(driver, 5)  # Short timeout for popup detection
    
    popup_handled = False
    
    try:
        # Check for ad blocker popup (as shown in the image)
        ad_blocker_elements = driver.find_elements(By.XPATH, "//div[contains(text(), 'Turn off ad blocker')]")
        
        if ad_blocker_elements:
            custom_print("Detected ad blocker warning popup", "info")
            
            # Try to find and click the OK button
            try:
                # Look for the OK button within the popup - multiple patterns
                ok_buttons = driver.find_elements(By.XPATH, "//div[contains(@role, 'dialog')]//div[text()='OK' or text()='Ok' or text()='ok' or text()='Continue' or text()='Okay']")
                if ok_buttons:
                    custom_print("Found OK button, clicking it", "info")
                    ok_buttons[0].click()
                    
                    # Add a small delay after clicking
                    time.sleep(random.uniform(0.5, 1.5))
                    popup_handled = True
                else:
                    # If no OK button, look for close buttons
                    close_buttons = driver.find_elements(By.XPATH, "//div[contains(@role, 'dialog')]//div[@aria-label='Close' or @aria-label='close' or @aria-label='Close dialog']")
                    if close_buttons:
                        custom_print("Found Close button, clicking it", "info")
                        close_buttons[0].click()
                        time.sleep(random.uniform(0.5, 1.5))
                        popup_handled = True
                    else:
                        # Try clicking any X icons
                        x_buttons = driver.find_elements(By.XPATH, "//div[contains(@class, 'x92rtbv')]/*[local-name()='svg' or local-name()='div']")
                        if x_buttons:
                            custom_print("Found X icon, clicking it", "info")
                            x_buttons[0].click()
                            time.sleep(random.uniform(0.5, 1.5))
                            popup_handled = True
                
            except (NoSuchElementException, ElementNotInteractableException) as e:
                custom_print(f"Could not find or click OK button: {str(e)}", "warning")
                
                # Try an alternative approach - look for any button element
                try:
                    buttons = driver.find_elements(By.XPATH, "//div[contains(@role, 'dialog')]//div[@role='button']")
                    if buttons:
                        custom_print(f"Found {len(buttons)} buttons in popup, clicking the last one", "info")
                        # Attempt to click the button up to 3 times before giving up for this URL
                        max_click_attempts = 3
                        for attempt in range(1, max_click_attempts + 1):
                            try:
                                custom_print(f"Clicking last button in popup (attempt {attempt}/{max_click_attempts})", "info")
                                buttons[-1].click()
                                time.sleep(random.uniform(0.5, 1.0))
                                popup_handled = True
                                break  # success
                            except Exception as click_err:
                                custom_print(f"Popup click attempt {attempt} failed: {click_err}", "warning")
                                if attempt == max_click_attempts:
                                    custom_print("Max popup click attempts reached; will ignore this popup for current link.", "warning")
                                    popup_handled = True
                                    break  # stop checking this button list
                                else:
                                    time.sleep(0.5)  # tiny pause before retry
                        if popup_handled:
                            pass
                        else:
                            pass
                    if popup_handled:
                        pass
                    else:
                        pass
                except Exception as e2:
                    custom_print(f"Alternative button click failed: {str(e2)}", "error")
        
        # Check for other common Facebook popups even if no ad blocker popup was found
        if not popup_handled:
            # Check for any dialog that might be present
            dialogs = driver.find_elements(By.XPATH, "//div[@role='dialog']")
            if dialogs:
                custom_print("Found dialog element, looking for buttons", "info")
                # Try to find common button text patterns in the dialog
                try:
                    dialog_buttons = driver.find_elements(By.XPATH, "//div[@role='dialog']//div[text()='OK' or text()='Ok' or text()='Continue' or text()='Accept' or text()='Close' or text()='Cancel' or text()='Got it']")
                    if dialog_buttons:
                        custom_print(f"Found dialog button with text: {dialog_buttons[0].text}", "info")
                        dialog_buttons[0].click()
                        time.sleep(random.uniform(0.5, 1.0))
                        popup_handled = True
                    else:
                        # If no text buttons found, try attribute-based buttons
                        dialog_close_buttons = driver.find_elements(By.XPATH, "//div[@role='dialog']//div[@aria-label='Close' or @aria-label='close' or @aria-label='Close dialog']")
                        if dialog_close_buttons:
                            custom_print("Found dialog close button", "info")
                            dialog_close_buttons[0].click()
                            time.sleep(random.uniform(0.5, 1.0))
                            popup_handled = True
                except Exception as e:
                    custom_print(f"Error handling dialog buttons: {str(e)}", "warning")
            
            # Check for generic dialogs with close buttons
            if not popup_handled:
                close_buttons = driver.find_elements(By.XPATH, "//div[@aria-label='Close' or @aria-label='Close dialog']")
                if close_buttons:
                    custom_print("Found generic close button for popup", "info")
                    close_buttons[0].click()
                    time.sleep(random.uniform(0.5, 1.0))
                    popup_handled = True
            
            # Check for "x" close icons
            if not popup_handled:
                close_x_buttons = driver.find_elements(By.XPATH, "//div[contains(@class, 'x92rtbv')]/*[local-name()='svg' or local-name()='div']")
                if close_x_buttons:
                    custom_print("Found 'x' close icon for popup", "info")
                    close_x_buttons[0].click()
                    time.sleep(random.uniform(0.5, 1.0))
                    popup_handled = True
            
            # Check for any element with role="button" inside any potential popup container
            if not popup_handled:
                popup_containers = driver.find_elements(By.XPATH, "//div[contains(@class, 'x1ey2m1c') or contains(@class, 'xds687c')]")
                if popup_containers:
                    for container in popup_containers:
                        try:
                            buttons = container.find_elements(By.XPATH, ".//div[@role='button']")
                            if buttons:
                                custom_print("Found buttons in popup container", "info")
                                # Try to find OK, Continue, or Close button
                                for button in buttons:
                                    if button.text.lower() in ['ok', 'okay', 'continue', 'close', 'got it', 'accept']:
                                        custom_print(f"Clicking button with text: {button.text}", "info")
                                        # Attempt to click the button up to 3 times before giving up for this URL
                                        max_click_attempts = 3
                                        for attempt in range(1, max_click_attempts + 1):
                                            try:
                                                custom_print(f"Clicking button in popup container (attempt {attempt}/{max_click_attempts})", "info")
                                                button.click()
                                                time.sleep(random.uniform(0.5, 1.0))
                                                popup_handled = True
                                                break  # success
                                            except Exception as click_err:
                                                custom_print(f"Popup click attempt {attempt} failed: {click_err}", "warning")
                                                if attempt == max_click_attempts:
                                                    custom_print("Max popup click attempts reached; will ignore this popup for current link.", "warning")
                                                    popup_handled = True
                                                    break  # stop checking this button list
                                                else:
                                                    time.sleep(0.5)  # tiny pause before retry
                                        if popup_handled:
                                            pass
                                        else:
                                            pass
                                    if popup_handled:
                                        pass
                                    else:
                                        pass
                                # If no text match, click the last button (often OK/Continue)
                                if not popup_handled and buttons:
                                    custom_print("Clicking last button in container", "info")
                                    # Attempt to click the button up to 3 times before giving up for this URL
                                    max_click_attempts = 3
                                    for attempt in range(1, max_click_attempts + 1):
                                        try:
                                            custom_print(f"Clicking last button in popup container (attempt {attempt}/{max_click_attempts})", "info")
                                            buttons[-1].click()
                                            time.sleep(random.uniform(0.5, 1.0))
                                            popup_handled = True
                                            break  # success
                                        except Exception as click_err:
                                            custom_print(f"Popup click attempt {attempt} failed: {click_err}", "warning")
                                            if attempt == max_click_attempts:
                                                custom_print("Max popup click attempts reached; will ignore this popup for current link.", "warning")
                                                popup_handled = True
                                                break  # stop checking this button list
                                            else:
                                                time.sleep(0.5)  # tiny pause before retry
                                    if popup_handled:
                                        pass
                                    else:
                                        pass
                                if popup_handled:
                                    pass
                                else:
                                    pass
                        except Exception as e:
                            custom_print(f"Error with popup container buttons: {str(e)}", "warning")
        
        return popup_handled
        
    except Exception as e:
        custom_print(f"Error handling popups: {str(e)}", "error")
        return False

# Platform identification mapping
PLATFORM_MAPPING = {
    ("https://static.xx.fbcdn.net/rsrc.php/v4/yV/r/OLar8kmsCmm.png", "0px -1188px"): "Facebook",
    ("https://static.xx.fbcdn.net/rsrc.php/v4/yV/r/OLar8kmsCmm.png", "0px -1201px"): "Instagram",
    ("https://static.xx.fbcdn.net/rsrc.php/v4/yO/r/ZuVkzM77JQ-.png", "-68px -189px"): "Audience Network",
    ("https://static.xx.fbcdn.net/rsrc.php/v4/yO/r/ZuVkzM77JQ-.png", "-51px -189px"): "Messenger",
    ("https://static.xx.fbcdn.net/rsrc.php/v4/abc/xyz.png", "-100px -200px"): "Messenger",
    ("https://static.xx.fbcdn.net/rsrc.php/v4/y5/r/7Ia52m_bDk0.png", "-246px -280px"): "Messenger",
    ("https://static.xx.fbcdn.net/rsrc.php/v4/yO/r/ZuVkzM77JQ-.png", "-34px -189px"): "WhatsApp",
    ("https://static.xx.fbcdn.net/rsrc.php/v4/yO/r/ZuVkzM77JQ-.png", "-56px -206px"): "Financial products and services",
    ("https://static.xx.fbcdn.net/rsrc.php/v4/yV/r/OLar8kmsCmm.png", "0px -1214px"): "Thread"
}
CATEGORY_MAPPING = {
    # Business and Employment
    ("https://static.xx.fbcdn.net/rsrc.php/v4/y5/r/7Ia52m_bDk0.png", "-189px -384px"): "Employment",
    ("https://static.xx.fbcdn.net/rsrc.php/v4/y5/r/7Ia52m_bDk0.png", "-204px -384px"): "Job Search",
    ("https://static.xx.fbcdn.net/rsrc.php/v4/y5/r/7Ia52m_bDk0.png", "-68px -384px"): "Business Services",
    
    # Financial
    ("https://static.xx.fbcdn.net/rsrc.php/v4/yO/r/ZuVkzM77JQ-.png", "-56px -206px"): "Financial Products and Services",
    ("https://static.xx.fbcdn.net/rsrc.php/v4/yO/r/ZuVkzM77JQ-.png", "-34px -206px"): "Insurance",
    ("https://static.xx.fbcdn.net/rsrc.php/v4/yO/r/ZuVkzM77JQ-.png", "-102px -206px"): "Investing",
    ("https://static.xx.fbcdn.net/rsrc.php/v4/yO/r/ZuVkzM77JQ-.png", "-85px -206px"): "Loans",
    
    # E-commerce & Retail
    ("https://static.xx.fbcdn.net/rsrc.php/v4/yy/r/7K0VH3FkE1a.png", "-51px -153px"): "Shopping & Retail",
    ("https://static.xx.fbcdn.net/rsrc.php/v4/yy/r/7K0VH3FkE1a.png", "-68px -153px"): "Fashion & Apparel",
    ("https://static.xx.fbcdn.net/rsrc.php/v4/yy/r/7K0VH3FkE1a.png", "-85px -153px"): "Electronics",
    ("https://static.xx.fbcdn.net/rsrc.php/v4/yy/r/7K0VH3FkE1a.png", "-102px -153px"): "Home & Garden",
    
    # Technology
    ("https://static.xx.fbcdn.net/rsrc.php/v4/y5/r/7Ia52m_bDk0.png", "-102px -153px"): "Software",
    ("https://static.xx.fbcdn.net/rsrc.php/v4/y5/r/7Ia52m_bDk0.png", "-85px -153px"): "Mobile Apps",
    ("https://static.xx.fbcdn.net/rsrc.php/v4/y5/r/7Ia52m_bDk0.png", "-68px -153px"): "Gaming",
    
    # Health & Beauty
    ("https://static.xx.fbcdn.net/rsrc.php/v4/yO/r/ZuVkzM77JQ-.png", "-17px -206px"): "Health & Wellness",
    ("https://static.xx.fbcdn.net/rsrc.php/v4/yO/r/ZuVkzM77JQ-.png", "0px -206px"): "Beauty & Personal Care",
    ("https://static.xx.fbcdn.net/rsrc.php/v4/yO/r/ZuVkzM77JQ-.png", "-17px -189px"): "Fitness",
    
    # Education
    ("https://static.xx.fbcdn.net/rsrc.php/v4/y5/r/7Ia52m_bDk0.png", "-34px -153px"): "Education",
    ("https://static.xx.fbcdn.net/rsrc.php/v4/y5/r/7Ia52m_bDk0.png", "-17px -153px"): "Online Courses",
    ("https://static.xx.fbcdn.net/rsrc.php/v4/y5/r/7Ia52m_bDk0.png", "0px -153px"): "Tutoring",
    
    # Real Estate
    ("https://static.xx.fbcdn.net/rsrc.php/v4/yO/r/ZuVkzM77JQ-.png", "-119px -206px"): "Real Estate",
    ("https://static.xx.fbcdn.net/rsrc.php/v4/yO/r/ZuVkzM77JQ-.png", "-136px -206px"): "Property Management",
    
    # Travel & Hospitality
    ("https://static.xx.fbcdn.net/rsrc.php/v4/yy/r/7K0VH3FkE1a.png", "-119px -153px"): "Travel",
    ("https://static.xx.fbcdn.net/rsrc.php/v4/yy/r/7K0VH3FkE1a.png", "-136px -153px"): "Hotels & Lodging",
    
    # Food & Beverage
    ("https://static.xx.fbcdn.net/rsrc.php/v4/yy/r/7K0VH3FkE1a.png", "-17px -153px"): "Food & Beverage",
    ("https://static.xx.fbcdn.net/rsrc.php/v4/yy/r/7K0VH3FkE1a.png", "-34px -153px"): "Restaurants",
    
    # Non-profit & Social Causes
    ("https://static.xx.fbcdn.net/rsrc.php/v4/y5/r/7Ia52m_bDk0.png", "-51px -153px"): "Non-profit",
    ("https://static.xx.fbcdn.net/rsrc.php/v4/y5/r/7Ia52m_bDk0.png", "-34px -170px"): "Charity",
    
    # Entertainment
    ("https://static.xx.fbcdn.net/rsrc.php/v4/yy/r/7K0VH3FkE1a.png", "-51px -170px"): "Entertainment",
    ("https://static.xx.fbcdn.net/rsrc.php/v4/yy/r/7K0VH3FkE1a.png", "-68px -170px"): "Movies",
    ("https://static.xx.fbcdn.net/rsrc.php/v4/yy/r/7K0VH3FkE1a.png", "-85px -170px"): "Music",
    ("https://static.xx.fbcdn.net/rsrc.php/v4/yy/r/7K0VH3FkE1a.png", "-102px -170px"): "Sports"
}

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
    # You can also provide proxies directly if needed:
    # proxy_manager = ProxyManager(proxies=["IP:PORT", "IP:PORT"])

# Set up stealth driver with anti-detection measures
custom_print("Creating stealth browser driver with anti-detection measures")
driver = create_stealth_driver(
    use_proxy=(proxy_manager is not None),
    proxy_manager=proxy_manager,
    headless=True  # Set to False to see the browser in action
)

# Configure dynamic wait times (variable to appear more human-like)
wait_time = random.uniform(8, 12)  # Random wait between 8-12 seconds
wait = WebDriverWait(driver, wait_time)

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

# Function to extract transparency URLs from Milk worksheet
def extract_transparency_urls(worksheet):
    """Extract Page Transparency URLs from the Milk worksheet"""
    if not worksheet:
        custom_print("Invalid worksheet provided", "error")
        return [], {}
        
    try:
        # Get all headers (first row)
        headers = worksheet.row_values(1)
        custom_print(f"Found headers in Milk worksheet: {headers}")
        
        # Find relevant column indices
        page_col_idx = None
        transparency_col_idx = None
        
        for i, header in enumerate(headers):
            # Check for exact matches including trailing spaces
            if header == "Page " or header == "Page":
                page_col_idx = i + 1  # gspread is 1-indexed
                custom_print(f"Found Page column at index {page_col_idx}: '{header}'")
            
            # Check for Page Transparency with possible typos
            if header.lower().strip() in ["page transperancy", "page transperancy ", "page transparency"]:
                transparency_col_idx = i + 1  # gspread is 1-indexed
                custom_print(f"Found Page Transparency column at index {transparency_col_idx}: '{header}'")
        
        if not page_col_idx or not transparency_col_idx:
            custom_print("Could not find required columns in the Milk worksheet!", "warning")
            return [], {}
            
        # Get all records with transparency links
        all_values = worksheet.get_all_values()
        urls = []
        url_row_mapping = {}
        
        # Use an ordered dictionary to maintain the exact order of URLs by row number
        ordered_url_mapping = {}
        for row_idx, row in enumerate(all_values[1:], 2):  # Start at row 2 (1-indexed)
            # Only proceed if we have valid data in the transparency column
            if len(row) >= transparency_col_idx:
                transparency_url = row[transparency_col_idx - 1].strip()  # Convert to 0-indexed
                
                if transparency_url and transparency_url.startswith("http"):
                    # Store with row_idx as key to maintain order
                    ordered_url_mapping[row_idx] = transparency_url
                    url_row_mapping[transparency_url] = row_idx
        
        # Extract URLs in strict row order
        sorted_rows = sorted(ordered_url_mapping.keys())  # Sort row indices numerically
        urls = [ordered_url_mapping[row] for row in sorted_rows]  # Get URLs in row order
        
        custom_print(f"Extracted {len(urls)} Page Transparency URLs from worksheet")
        return urls, url_row_mapping, page_col_idx, transparency_col_idx
        
    except Exception as e:
        custom_print(f"Error extracting URLs from worksheet: {e}", "error")
        return [], {}, None, None

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
                custom_print(f"Found Page Transperancy column via fallback at index {transperancy_col_idx} via fallback: '{header}'")
        
        if not page_col_idx or not transperancy_col_idx:
            custom_print("Could not find required columns in the Milk worksheet!", "warning")
            return [], {}
            
        # Get all records with transparency links
        all_values = worksheet.get_all_values()
        urls = []
        page_names = {}
        
        # Skip header row
        for row_idx, row in enumerate(all_values[1:], start=2):  # Start from row 2 (1-indexed)
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
                            # Persist the converted URL back to the Milk sheet so that the
                            # column always shows a complete Ad Library link, even when the
                            # original transparency link lacked a page_id.
                            try:
                                worksheet.update_cell(row_idx, transperancy_col_idx, ad_library_url)
                                custom_print(f"Updated Milk sheet row {row_idx}, column {transperancy_col_idx} with Ad Library URL")
                            except Exception as e:
                                custom_print(f"Error updating Milk sheet with Ad Library URL: {e}", "warning")
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

# Function to extract search term from URL
def extract_search_term(url):
    """Extract search term from Facebook search URL"""
    try:
        from urllib.parse import parse_qs, urlparse
        parsed = urlparse(url)
        query_params = parse_qs(parsed.query)
        return query_params.get('q', [''])[0]
    except Exception as e:
        custom_print(f"Error extracting search term from URL: {e}", "error")
        return None

# Connect to Google Sheets
custom_print("Connecting to Google Sheets...")
sheet_name = "Master Auto Swipe - Test ankur"

# Set up Milk worksheet connection for transparency URLs
custom_print("Setting up connection to Milk worksheet...")
milk_worksheet = setup_google_sheets(sheet_name=sheet_name, worksheet_name="Milk")

# Extract transparency URLs from Milk worksheet
transparency_urls, url_row_mapping, page_col_idx, transparency_col_idx = extract_transparency_urls(milk_worksheet)

if not transparency_urls:
    custom_print("No transparency URLs found in Milk worksheet. Exiting.", "error")
    sys.exit(1)

custom_print(f"Found {len(transparency_urls)} transparency URLs to process")

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
      
# Initialize a dictionary to collect all ads data
all_ads_data = {}

# Initialize the set for URLs already processed
previously_processed_urls = set()

# Create a file to store processed URLs for persistence across runs
processed_urls_file = "processed_urls.txt"

# Always force reprocessing of all URLs to ensure nothing is missed
force_reprocess = True
custom_print("FORCE_REPROCESS is enabled - will process all URLs")

# Simple function to clean up URLs (minimal processing)
def clean_url(url):
    """Basic URL cleaning - just remove whitespace and ensure it's a string"""
    if not url:
        return ""
    return str(url).strip()

# Load previously processed transparency URLs from file if it exists
processed_transparency_urls_file = "processed_transparency_urls.txt"
previously_processed_urls_from_file = set()

# Load previously processed URLs
if os.path.exists(processed_transparency_urls_file):
    try:
        with open(processed_transparency_urls_file, 'r') as f:
            previously_processed_urls_from_file = {line.strip() for line in f if line.strip()}
        custom_print(f"Loaded {len(previously_processed_urls_from_file)} previously processed transparency URLs")
    except Exception as e:
        custom_print(f"Error loading processed transparency URLs: {e}", "error")

# Filter out already processed URLs
urls_to_process = [url for url in transparency_urls if url not in previously_processed_urls_from_file]

if not urls_to_process:
    custom_print("All transparency URLs have already been processed. Exiting.", "info")
    sys.exit(0)

custom_print(f"Found {len(urls_to_process)} new transparency URLs to process")
try:
    if os.path.exists(processed_urls_file) and not force_reprocess:
        with open(processed_urls_file, 'r') as f:
            for line in f:
                url = clean_url(line)
                if url:  # Skip empty lines
                    previously_processed_urls_from_file.add(url)
        custom_print(f"Loaded {len(previously_processed_urls_from_file)} previously processed URLs from file")
except Exception as e:
    custom_print(f"Error loading previously processed URLs: {e}", "error")

# Clean all input URLs
cleaned_urls = {clean_url(url): url for url in urls}
custom_print(f"Cleaned {len(urls)} URLs for processing")

# Implement URL prioritization and filtering
# Use a copy of the URLs for randomization but track which ones were processed
custom_print("Setting up URL processing order...")
urls_to_process = []  # Start with an empty list
processed_urls = set()  # Keep track of processed URLs in this session

# Process all URLs when force_reprocess is True
urls_to_process = urls.copy()
skipped_count = 0
custom_print(f"Added {len(urls_to_process)} URLs to processing list")

custom_print(f"IMPORTANT: Will process {len(urls_to_process)} URLs and skip {skipped_count} already processed URLs")
custom_print(f"Total input URLs: {len(urls)}, URLs to process: {len(urls_to_process)}, URLs to skip: {skipped_count}")

# If no URLs to process, exit gracefully
if not urls_to_process:
    custom_print("All URLs have already been processed. Nothing new to process.")
    # Clean up and exit
    driver.quit()
    sys.exit(0)

# Reset total URLs to the final list we need to process
total_urls = len(urls_to_process)
urls = urls_to_process

# Function to save processed URL to file
def save_processed_url(url):
    """Save a processed URL to the persistent file"""
    try:
        with open(processed_urls_file, 'a') as f:
            # Save original URL, not normalized version
            f.write(f"{url}\n")
        return True
    except Exception as e:
        custom_print(f"Error saving processed URL to file: {e}", "error")
        return False

# Initialize progress tracking
total_urls = len(urls_to_process)
def update_progress_percentage():
    if total_urls > 0:
        current_index = min(len(processed_urls) + 1, total_urls)
        progress_percentage = (current_index / total_urls) * 100
        # Format with GitHub Actions-friendly output that's easy to spot in logs
        # Use simple ASCII characters for progress bar to avoid encoding issues
        progress_bar = f"[{'#' * int(progress_percentage / 2)}{'-' * (50 - int(progress_percentage / 2))}] {progress_percentage:.1f}%"
        
        # Check if running from master script to use the appropriate format
        running_from_master = os.environ.get('RUNNING_FROM_MASTER_SCRIPT') == 'true'
        
        if running_from_master:
            # When running from master script, simplified output for central display
            print(f"PROGRESS [Ad_details_scraper]: {progress_bar} ({current_index}/{total_urls} URLs)")
            sys.stdout.flush()
        else:
            # When running standalone, use a simpler format that's still clear
            print(f"\nPROGRESS [Ad_details_scraper]: {progress_percentage:.1f}% ({current_index}/{total_urls} URLs)\n{progress_bar}")
            
        # This ensures the progress is visible in GitHub Actions logs
        sys.stdout.flush()

# Process each URL from the Milk worksheet one at a time and update the sheet after each
# Continue until all URLs have been processed
url_index = 0

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

# Process transparency URLs one by one
for url in urls_to_process:
    custom_print(f"\nProcessing transparency URL: {url}")
    url_index += 1
    
    # Mark as processed at the start to handle retries
    if url not in previously_processed_urls_from_file:
        try:
            with open(processed_transparency_urls_file, 'a') as f:
                f.write(f"{url}\n")
            previously_processed_urls_from_file.add(url)
        except Exception as e:
            custom_print(f"Error saving processed URL: {e}", "error")
    
    # Set up tracking for this URL
    processed_urls.add(url)
    should_skip_url = False
    
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
            
            # Check for popups immediately after page load
            if handle_popups(driver, wait):
                custom_print("Handled popup after initial page load")
                # Add a short delay after handling popup
                time.sleep(random.uniform(0.5, 1.0))
            
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
                
                # Check for popups that might block content loading
                if handle_popups(driver, wait):
                    custom_print("Handled popup during page load verification", "info")
                    # Try again to verify the page loaded properly after dismissing popup
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
                    
                    # First try exact URL match
                    if url_col_index:
                        url_values = milk_worksheet.col_values(url_col_index)
                        for i, cell_url in enumerate(url_values):
                            if cell_url.strip() == url.strip():
                                row_index = i + 1
                                custom_print(f"Found exact URL match in Milk worksheet at row {row_index}")
                                break
                    
                    # If no exact match, check if it's a search URL and try to match by search term
                    if not row_index and "search_type=keyword_unordered" in url:
                        search_term = extract_search_term(url)
                        if search_term:
                            custom_print(f"Searching for term: {search_term} in Milk sheet...")
                            # Check in Page Transparency column first
                            if 'page_transperancy' in milk_column_indices:
                                page_trans_values = milk_worksheet.col_values(milk_column_indices['page_transperancy'])
                                for i, cell_value in enumerate(page_trans_values):
                                    if search_term.lower() in cell_value.lower():
                                        row_index = i + 1
                                        custom_print(f"Matched search term in Page Transparency at row {row_index}")
                                        break
                            
                            # If still no match, check in URL column
                            if not row_index and url_col_index:
                                url_values = milk_worksheet.col_values(url_col_index)
                                for i, cell_value in enumerate(url_values):
                                    if search_term.lower() in cell_value.lower():
                                        row_index = i + 1
                                        custom_print(f"Matched search term in URL column at row {row_index}")
                                        break
                    
                    # If still no match, try direct URL matching in Page Transparency column
                    if not row_index and 'page_transperancy' in milk_column_indices:
                        page_trans_values = milk_worksheet.col_values(milk_column_indices['page_transperancy'])
                        for i, cell_value in enumerate(page_trans_values):
                            if url.strip() in cell_value.strip():
                                row_index = i + 1
                                custom_print(f"Matched URL in Page Transparency column at row {row_index}")
                                break
                    
                    # Last resort: try matching by page_id if available
                    if not row_index and 'page_transperancy' in milk_column_indices and 'page_id' in url_params:
                        page_trans_values = milk_worksheet.col_values(milk_column_indices['page_transperancy'])
                        for i, cell_value in enumerate(page_trans_values):
                            if url_params['page_id'] in cell_value:
                                row_index = i + 1
                                custom_print(f"Matched page_id in Milk worksheet at row {row_index}")
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
        
        # Already marked as processed at the start
        # Update progress
        update_progress_percentage()
        continue
        
    # Check if this URL should be skipped (was already processed)
    if should_skip_url:
        custom_print(f"Skipping URL {url} as it was already processed")
        update_progress_percentage()
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
    
    # Custom improved scrolling function that resets attempts after successful scrolls
    def improved_human_like_scroll(driver, scroll_pause_base=1.0, max_scroll_attempts=4):
        """
        Scrolls down a webpage with human-like behavior, resetting attempt counter after successful scrolls
        
        Args:
            driver: The Selenium WebDriver instance
            scroll_pause_base: Base amount of time to pause between scrolls
            max_scroll_attempts: Max attempts to scroll when no height change is detected
            
        Returns:
            int: The number of scroll operations performed
        """
        custom_print("Starting improved human-like scrolling...")
        scroll_count = 0
        last_height = driver.execute_script("return document.body.scrollHeight")
        attempt = 0  # Counter for attempts when height doesn't change
        
        while attempt < max_scroll_attempts:
            # Scroll down with a random offset for more human-like behavior
            scroll_amount = random.randint(600, 1000)  # Varying scroll amounts
            driver.execute_script(f"window.scrollBy(0, {scroll_amount});")
            scroll_count += 1
            
            # Random pause time (with some variation to appear more human-like)
            variation = random.uniform(0.5, 1.5)  # 50% below to 50% above base time
            pause_time = scroll_pause_base * variation
            time.sleep(pause_time)
            
            # Calculate new scroll height
            new_height = driver.execute_script("return document.body.scrollHeight")
            
            # Check if the page height has changed
            if new_height != last_height:
                custom_print(f"Scroll height changed: {last_height} -> {new_height}")
                last_height = new_height
                attempt = 0  # Reset attempts counter after successful scroll
            else:
                # Scroll height hasn't changed, increment attempt counter
                attempt += 1
                custom_print(f"Scroll height ({new_height}) hasn't changed. Attempt {attempt}/{max_scroll_attempts} at bottom...")
                
                # Try a different scroll method on each attempt
                if attempt == 1:
                    # Try scrolling to bottom
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                elif attempt == 2:
                    # Try a smaller scroll amount
                    driver.execute_script("window.scrollBy(0, 200);")
                elif attempt == 3:
                    # Try scrolling with behavior: smooth
                    driver.execute_script("window.scrollTo({top: document.body.scrollHeight, behavior: 'smooth'});")
                    time.sleep(0.5)  # Extra time for smooth scroll
        
        custom_print(f"Completed {scroll_count} scrolls")
        return scroll_count

    # Replace the original scroll function call with our improved version
    # Perform ultra-fast scrolling with minimal pauses
    scroll_count = improved_human_like_scroll(
        driver, 
        scroll_pause_base=random.uniform(0.3, 0.8),  # Ultra-short pause time
        max_scroll_attempts=4                         # Attempts at bottom
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
    
    # Check for popups that might appear after scrolling
    if handle_popups(driver, wait):
        custom_print("Handled popup after scrolling")
        # Add a short delay after handling popup
        time.sleep(random.uniform(0.5, 1.0))

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
    target_class_2 = "xrvj5dj x18m771g x1p5oq8j xp48ta0 x18d9i69 xtssl2i xtqikln x1na6gtj x1jr1mh3 x15h0gye x7sq92a xlxr9qa"
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
    
    # Initialize set to track scraped ad links
    scraped_ad_links = set()

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
                    main_container = child_div.find_element(By.XPATH, './/div[contains(@class, "x78zum5") and contains(@class, "xdt5ytf") and contains(@class, "x2lwn1j") and contains(@class, "xeuugli")]')

                    # Extract Library ID
                    library_id_element = main_container.find_element(By.XPATH, './/div[contains(@class, "x1rg5ohu") and contains(@class, "x67bb7w")]/span[contains(text(), "Library ID:")]')
                    library_id = library_id_element.text.replace("Library ID: ", "").strip()
                    current_ad_id_for_logging = library_id # Update logging ID once found

                    # if library_id in ads_data:
                    #     # print(f"Skipping duplicate Library ID: {library_id}")
                    #     continue

                    # Initialize ad data with library_id
                    ad_data = {"library_id": library_id}
                    skip_primary_insert = False
                    
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
                        ads_count_element = main_container.find_element(By.XPATH, './/div[contains(@class, "x6s0dn4") and contains(@class, "x78zum5") and contains(@class, "xsag5q8")]//strong')
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
                        
                        # Check if ad link has already been scraped
                        if decoded_url in scraped_ad_links:
                            continue  # Skip duplicate ad link

                        # Add to scraped ad links set
                        scraped_ad_links.add(decoded_url)

                        # Store the full URL, not just the domain
                        ad_data["destination_url"] = actual_url


                        # Extract media from this link container
                        ad_data["media_type"] = None
                        ad_data["media_url"] = None
                        ad_data["thumbnail_url"] = None
                        
                        # Check for video first
                        video_found = False
                        try:
                            # Try to find video by class name first (most reliable)
                            video_element = child_div.find_element(By.CSS_SELECTOR, 'video.x1lliihq.x5yr21d.xh8yej3')
                            
                            if video_element:
                                # Extract video URL
                                media_url = video_element.get_attribute('src')
                                if media_url:
                                    ad_data["media_type"] = "video"
                                    ad_data["media_url"] = media_url
                                    
                                    # Extract thumbnail URL (poster attribute)
                                    thumbnail_url = video_element.get_attribute('poster')
                                    if thumbnail_url:
                                        ad_data["thumbnail_url"] = thumbnail_url
                                    video_found = True
                        except NoSuchElementException:
                            # Video not found, will try images next
                            pass
                        except Exception as e:
                            custom_print(f"Error processing video: {str(e)}")
                        
                        # Only try to find images if no video was found
                        if not video_found and (not ad_data.get("media_url") or not ad_data.get("media_type")):
                            try:
                                # First try with specific class names inside the primary link container
                                img_elements = link_container.find_elements(By.XPATH, './/img[contains(@class, "x168nmei") or contains(@class, "_8nqq") or contains(@class, "x15mokao") or contains(@class, "x1ga7v0g") or contains(@class, "x16uus16") or contains(@class, "xbiv7yw") or contains(@class, "x1ll5gia") or contains(@class, "x19kjcj4") or contains(@class, "xh8yej3") ]')
                                
                                # If no images found with specific classes, try any image in the link container
                                if not img_elements:
                                    img_elements = link_container.find_elements(By.TAG_NAME, 'img')
                                
                                # Use the first valid image found in the link container
                                for img in img_elements:
                                    try:
                                        media_url = img.get_attribute('src')
                                        if media_url and media_url.strip():
                                            ad_data["media_type"] = "image"
                                            ad_data["media_url"] = media_url
                                            break  # Use the first valid image
                                    except Exception:
                                        continue
                                
                                # Additional fallback: search for images anywhere inside the ad block
                                if not ad_data.get("media_url"):
                                    fallback_img_elements = child_div.find_elements(By.XPATH, './/img[contains(@class, "x15mokao") or contains(@class, "x1ga7v0g") or contains(@class, "x16uus16") or contains(@class, "xbiv7yw") or contains(@class, "x1ll5gia") or contains(@class, "x19kjcj4") or contains(@class, "x642log") or contains(@class, "xh8yej3") or @src]')
                                    for img in fallback_img_elements:
                                        try:
                                            media_url = img.get_attribute('src')
                                            if media_url and media_url.strip():
                                                ad_data["media_type"] = "image"
                                                ad_data["media_url"] = media_url
                                                break
                                        except Exception:
                                            continue
                            except Exception as e:
                                custom_print(f"Error finding images: {str(e)}")
                        
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
                    if not skip_primary_insert:
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
    custom_print(f"\nExtraction completed for URL {url_index}/{len(urls_to_process)}: Found {len(ads_data)} ads.")
    custom_print("Beginning data processing for Google Sheets update...")
    
    # Store data in memory (no need to create individual files)
    # This makes the process cleaner while still processing one URL at a time

    # Update the Ads Details worksheet with just this URL's data before moving to next URL
    if ads_worksheet and ads_data:
        custom_print(f"Updating Ads Details worksheet with {len(ads_data)} ads from URL {url_index}/{len(urls_to_process)}...")
        
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
                custom_print(f"Successfully updated Google Sheet with {len(rows_to_update)} new ads from URL {url_index}/{len(urls_to_process)}.")
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
    # Save to persistent file
    save_processed_url(url)
    
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

try:
    # Additional cleanup tasks can be added here if needed
    custom_print("Cleaning up resources...")
    
    # Final progress report
    custom_print(f"Processed {len(processed_urls)}/{len(urls)} URLs successfully")
    
    # Update any final status if needed
    if milk_worksheet:
        custom_print("Updating final status in Google Sheets...")
        # Any final sheet updates would go here
finally:
    # Make sure the browser is closed even if errors occurred
    custom_print("Closing browser...")
    driver.quit()
    custom_print("Browser closed successfully")

print("Script execution complete")
