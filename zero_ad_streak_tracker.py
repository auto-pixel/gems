from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
import time
import json
import re
from datetime import datetime
from urllib.parse import unquote, urlparse
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
import random  # [Human behavior: for random delays]
from urllib.parse import urlparse, parse_qs
import json
import requests
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
import os
import sys
from functools import partial

# Google Sheets imports
import gspread
from google.oauth2.service_account import Credentials
from gspread.utils import rowcol_to_a1
from google.auth.exceptions import GoogleAuthError
import logging

# ============== CONFIGURATION =====================
load_dotenv()

# Google Sheets configuration
SHEET_NAME = "Master Auto Swipe - Test ankur"
CREDENTIALS_PATH = "credentials.json"

# Platform identification mapping (unchanged)
PLATFORM_MAPPING = {
    ("https://static.xx.fbcdn.net/rsrc.php/v4/yW/r/TP7nCDju1B-.png", "0px -1171px"): "Facebook",
    ("https://static.xx.fbcdn.net/rsrc.php/v4/yj/r/0dseWS3_nMM.png", "-34px -353px"): "Instagram",
    ("https://static.xx.fbcdn.net/rsrc.php/v4/y3/r/r35dp7ubbrO.png", "-16px -528px"): "Audience Network",
    ("https://static.xx.fbcdn.net/rsrc.php/v4/y3/r/r35dp7ubbrO.png", "-29px -528px"): "Messenger",
    ("https://static.xx.fbcdn.net/rsrc.php/v4/yW/r/TP7nCDju1B-.png", "0px -1184px"): "Thread"
}
CATEGORY_MAPPING = {
    ("https://static.xx.fbcdn.net/rsrc.php/v4/y3/r/r35dp7ubbrO.png", "-65px -557px"): "Employment",
    ("https://static.xx.fbcdn.net/rsrc.php/v4/y3/r/r35dp7ubbrO.png", "0px -544px"): "Housing",
    ("https://static.xx.fbcdn.net/rsrc.php/v4/y3/r/r35dp7ubbrO.png", "-13px -557px"): "Financial products and services",
}


def setup_google_sheets(sheet_name=SHEET_NAME, worksheet_name="Ads Details", credentials_path=CREDENTIALS_PATH):
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
        print(f"Successfully connected to spreadsheet: {sheet_name}")
        
        # Access the specified worksheet
        worksheet = spreadsheet.worksheet(worksheet_name)
        print(f"Successfully accessed worksheet: {worksheet_name}")
        
        return worksheet, spreadsheet
    except FileNotFoundError:
        print(f"Error: Credentials file '{credentials_path}' not found.")
        return None, None
    except GoogleAuthError as e:
        print(f"Authentication error: {e}")
        return None, None
    except Exception as e:
        print(f"Error connecting to Google Sheets: {e}")
        return None, None


def debug_worksheet_columns(worksheet_name):
    """
    Debug function to print all column names in a worksheet
    """
    try:
        worksheet, _ = setup_google_sheets(worksheet_name=worksheet_name)
        if worksheet:
            headers = worksheet.row_values(1)
            print(f"\n=== DEBUG: {worksheet_name} Worksheet Columns ===")
            for i, header in enumerate(headers, 1):
                print(f"Column {i}: '{header}' (length: {len(header)})")
            print("=" * 50)
            return headers
    except Exception as e:
        print(f"Error debugging worksheet {worksheet_name}: {e}")
    return []


def get_urls_from_milk_worksheet():
    """
    Extract URLs from the Milk worksheet for processing.
    Returns a list of URLs to scrape.
    """
    try:
        worksheet, spreadsheet = setup_google_sheets(worksheet_name="Milk")
        if not worksheet:
            print("Failed to connect to Milk worksheet")
            return []
        
        # Debug: Print all column names
        headers = debug_worksheet_columns("Milk")
        
        # Get all records from the worksheet
        records = worksheet.get_all_records()
        urls = []
        
        print(f"Found {len(records)} records in Milk worksheet")
        
        # Filter records that have transparency URLs
        valid_records = []
        for i, record in enumerate(records):
            # Try multiple possible column names for transparency URL
            page_transparency = None
            possible_columns = ['Page Transperancy ', 'Page Transperancy', 'Page Transparency ', 'Page Transparency']
            
            for col_name in possible_columns:
                if col_name in record and record[col_name] and record[col_name].strip():
                    page_transparency = record[col_name].strip()
                    break
            
            if page_transparency and page_transparency.startswith('http'):
                valid_records.append((i+1, record, page_transparency))
            else:
                print(f"Skipping record {i+1} - no valid transparency URL found")
        
        print(f"\nFound {len(valid_records)} records with valid transparency URLs out of {len(records)} total records")
        
        for record_num, record, page_transparency in valid_records:
            print(f"\nProcessing record {record_num}: Page='{record.get('Page ', '')}', URL={page_transparency}")
            
            if page_transparency and page_transparency.startswith('http'):
                # Convert transparency URL to ads library URL
                # Extract page ID from transparency URL and create ads library URL
                if 'facebook.com' in page_transparency:
                    try:
                        # Extract page ID from URL - handle different URL formats
                        page_id = None
                        
                        # Try different patterns to extract page ID
                        if 'view_all_page_id=' in page_transparency:
                            # Extract from view_all_page_id parameter
                            page_id = page_transparency.split('view_all_page_id=')[1].split('&')[0]
                        elif '/page/' in page_transparency:
                            # Extract from /page/ path
                            page_id = page_transparency.split('/page/')[-1].split('/')[0]
                        elif page_transparency.split('/')[-1].isdigit():
                            # Last part of URL is numeric
                            page_id = page_transparency.split('/')[-1]
                        
                        # Only create URL if we have a valid numeric page ID
                        if page_id and page_id.isdigit():
                            ads_url = f"https://www.facebook.com/ads/library/?active_status=all&ad_type=all&country=US&view_all_page_id={page_id}&search_type=page&media_type=all"
                            urls.append((ads_url, page_transparency))  # Store both URLs as tuple
                            print(f"Extracted page ID {page_id} from {page_transparency}")
                            print(f"Created ads URL: {ads_url}")
                        else:
                            print(f"Could not extract valid page ID from: {page_transparency}")
                    except Exception as e:
                        print(f"Error processing transparency URL {page_transparency}: {e}")
            else:
                print(f"No valid transparency URL found in record {i+1}")
        
        print(f"\nExtracted {len(urls)} URLs from Milk worksheet")
        return urls
        
    except Exception as e:
        print(f"Error extracting URLs from Milk worksheet: {e}")
        return []


def get_current_ip():
    """Get current IP address"""
    try:
        import requests
        response = requests.get('https://httpbin.org/ip', timeout=10)
        return response.json().get('origin', 'Unknown')
    except:
        return 'Unknown'


def ensure_ad_data_fields(ad_data):
    """Ensure all required fields exist in ad_data with default values and replace None values"""
    required_fields = {
        'library_id': '',
        'started_running': '',
        'total_active_time': '',
        'ads_count': '0',  # Default to '0' instead of empty string
        'ad_text': '',
        'cta_button_text': '',
        'media_type': '',
        'platforms': [],
        'media_url': '',
        'thumbnail_url': '',
        'source_url': '',
        'landing_page': '',
        'destination_url': '',
        'categories': [],
        'headline_text': ''
    }
    
    # Create a copy to avoid modifying the original
    cleaned_data = ad_data.copy()
    
    for field, default_value in required_fields.items():
        # Set default value if field is missing OR if it's None
        if field not in cleaned_data or cleaned_data[field] is None:
            cleaned_data[field] = default_value
    
    return cleaned_data


def save_data_to_google_sheets(ads_data, page_name, page_id, current_ip, original_url=None, total_ads_from_page=None):
    """
    Save scraped ad data to Google Sheets
    """
    try:
        # Connect to Ads Details worksheet
        ads_worksheet, spreadsheet = setup_google_sheets(worksheet_name="Ads Details")
        if not ads_worksheet:
            print("Failed to connect to Ads Details worksheet")
            return False
            
        # Connect to Milk worksheet to update ad count
        milk_worksheet, _ = setup_google_sheets(worksheet_name="Milk")
        
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Prepare data for Ads Details worksheet
        rows_to_add = []
        
        for library_id, ad_data in ads_data.items():
            # Debug: Check for None values before ensuring fields
            none_count_before = sum(1 for v in ad_data.values() if v is None)
            
            # Ensure all fields exist with default values
            ad_data = ensure_ad_data_fields(ad_data)
            
            # Debug: Check for None values after ensuring fields
            none_count_after = sum(1 for v in ad_data.values() if v is None)
            
            if none_count_before > 0 or none_count_after > 0:
                print(f"Debug: Library ID {library_id} - None values before: {none_count_before}, after: {none_count_after}")
            
            # Column mapping according to new specification:
            # "Name of page","page id","start_time","library_id","total_active_time","ads_count","headline_text","ad_text","cta_button_text","media_type","platform_names","media_url","thumbnail_url","source_url","landing_page","destination_url","categories","Last Update Time","IP Address"
            
            # Helper function to ensure all values are strings and handle edge cases
            def safe_str(value):
                if value is None:
                    return ''
                if isinstance(value, (list, dict)):
                    return ''  # Don't convert complex types to string
                try:
                    result = str(value).strip()
                    return result if result != 'None' else ''
                except:
                    return ''
            
            row_data = [
                safe_str(page_name),  # Name of page
                safe_str(page_id),    # page id
                safe_str(ad_data.get('started_running', '')),  # start_time
                safe_str(library_id),  # library_id
                safe_str(ad_data.get('total_active_time', '')),  # total_active_time
                safe_str(ad_data.get('ads_count', '')),  # ads_count
                safe_str(ad_data.get('headline_text', '')),  # headline_text
                safe_str(ad_data.get('ad_text', '')),  # ad_text
                safe_str(ad_data.get('cta_button_text', '')),  # cta_button_text
                safe_str(ad_data.get('media_type', '')),  # media_type
                ', '.join([str(p) for p in ad_data.get('platforms', []) if p is not None and str(p).strip()]) if ad_data.get('platforms') else '',  # platform_names
                safe_str(ad_data.get('media_url', '')),  # media_url
                safe_str(ad_data.get('thumbnail_url', '')),  # thumbnail_url
                safe_str(ad_data.get('source_url', '')),  # source_url
                safe_str(ad_data.get('landing_page', '')),  # landing_page
                safe_str(ad_data.get('destination_url', '')),  # destination_url
                ', '.join([str(c) for c in ad_data.get('categories', []) if c is not None and str(c).strip()]) if ad_data.get('categories') else '',  # categories
                safe_str(current_time),  # Last Update Time
                safe_str(current_ip)  # IP Address
            ]
            rows_to_add.append(row_data)
        
        # Add data to Ads Details worksheet
        if rows_to_add:
            # Debug: Check for None values in all rows
            print(f"Debug: Preparing to add {len(rows_to_add)} rows to Ads Details worksheet")
            
            # Find and fix None values
            cleaned_rows = []
            for row_idx, row in enumerate(rows_to_add):
                cleaned_row = []
                for col_idx, cell in enumerate(row):
                    if cell is None:
                        print(f"Debug: Found None value at row {row_idx}, column {col_idx}")
                        cleaned_row.append('')
                    else:
                        cleaned_row.append(str(cell))
                cleaned_rows.append(cleaned_row)
            
            print(f"Debug: Sample cleaned row: {cleaned_rows[0][:5]}...")
            
            try:
                ads_worksheet.append_rows(cleaned_rows)
                print(f"Successfully added {len(cleaned_rows)} ads to Ads Details worksheet")
            except Exception as e:
                print(f"Error adding rows to worksheet: {e}")
                print(f"Sample row causing error: {cleaned_rows[0]}")
                return False
        
        # Update Milk worksheet with ad count
        if milk_worksheet:
            try:
                # Get all records and find matching page ID
                records = milk_worksheet.get_all_records()
                print(f"Looking for page_id '{page_id}' in Milk worksheet with {len(records)} records")
                
                page_found = False
                for i, record in enumerate(records, start=2):  # start=2 because row 1 is headers
                    # Check if this row has a valid transparency URL first
                    transparency_url = record.get('Page Transperancy ', '')
                    if not transparency_url or not transparency_url.strip():
                        continue  # Skip rows without transparency URLs
                    
                    # Check if this transparency URL matches our original URL or contains our page_id
                    page_value = record.get('Page ', '')
                    print(f"Row {i}: Checking page_value '{page_value}' and transparency_url '{transparency_url}' for page_id '{page_id}'")
                    
                    # Match by exact transparency URL first, then by page_id
                    url_match = False
                    if original_url and str(transparency_url).strip() == str(original_url).strip():
                        url_match = True
                        print(f"  ✅ Exact URL match found")
                    elif str(page_id) in str(transparency_url):
                        url_match = True
                        print(f"  ✅ Page ID match found in transparency URL")
                    
                    if url_match:
                        # Update columns according to actual column structure:
                        # Column 1: 'Type', Column 2: 'Page ', Column 3: 'Page Transperancy ', 
                        # Column 4: 'no.of ads By Ai', Column 5: 'Last Update Time', Column 6: 'IP Address'
                        # Column 7: (empty), Column 8: 'LP'
                        
                        print(f"Updating Milk worksheet row {i} for page {page_id}")
                        
                        # Use total_ads_from_page if available, otherwise use len(ads_data)
                        ads_count_to_update = total_ads_from_page if total_ads_from_page is not None else len(ads_data)
                        
                        milk_worksheet.update_cell(i, 4, str(ads_count_to_update))  # no.of ads By Ai (column 4)
                        milk_worksheet.update_cell(i, 5, str(current_time))   # Last Update Time (column 5)
                        milk_worksheet.update_cell(i, 6, str(current_ip))     # IP Address (column 6)
                        
                        # Update Zero Ads Streak (Column 11 based on debug output)
                        try:
                            if ads_count_to_update > 0:
                                milk_worksheet.update_cell(i, 11, '0')  # Reset Zero Ads Streak
                                print(f"Reset Zero Ads Streak to 0 for page {page_id}")
                            else:
                                try:
                                    current_streak = int(milk_worksheet.cell(i, 11).value or 0)
                                    new_streak = current_streak + 1
                                    
                                    # Check if streak is > 30, if so, delete the row
                                    if new_streak > 30:
                                        milk_worksheet.delete_rows(i)
                                        print(f"Deleted row {i} for page {page_id} - Zero Ads Streak exceeded 30 ({new_streak})")
                                    else:
                                        milk_worksheet.update_cell(i, 11, str(new_streak))
                                        print(f"Incremented Zero Ads Streak to {new_streak} for page {page_id}")
                                except Exception as inner_e:
                                    milk_worksheet.update_cell(i, 11, '1')
                                    print(f"Set Zero Ads Streak to 1 for page {page_id} (error: {inner_e})")
                        except Exception as streak_error:
                            print(f"Error updating Zero Ads Streak: {streak_error}")
                        
                        print(f"Updated Milk worksheet for page {page_id} with {len(ads_data)} ads")
                        page_found = True
                        break
                
                if not page_found:
                    print(f"Warning: Page '{page_id}' (name: '{page_name}') not found in Milk worksheet")
                    print(f"Available pages: {[record.get('Page ', '') for record in records]}")
            except Exception as e:
                print(f"Error updating Milk worksheet: {e}")
        
        return True
        
    except Exception as e:
        print(f"Error saving data to Google Sheets: {e}")
        return False


def save_transcript_to_google_sheets(page_name, page_id, library_id, ads_count, media_url, transcript, current_ip):
    """
    Save transcript data to Google Sheets Transcript worksheet
    Columns: "Name of page","page_id","library_id","ads_count","media_url","Transcript","Last Update Time","IP Address"
    """
    try:
        # Connect to Transcript worksheet
        transcript_worksheet, _ = setup_google_sheets(worksheet_name="Transcript")
        if not transcript_worksheet:
            print("Failed to connect to Transcript worksheet")
            return False
            
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Prepare data for Transcript worksheet
        row_data = [
            page_name,      # Name of page
            page_id,        # page_id
            library_id,     # library_id
            ads_count,      # ads_count
            media_url,      # media_url
            transcript,     # Transcript
            current_time,   # Last Update Time
            current_ip      # IP Address
        ]
        
        # Add data to Transcript worksheet
        transcript_worksheet.append_row(row_data)
        print(f"Successfully added transcript for library_id {library_id} to Transcript worksheet")
        
        return True
        
    except Exception as e:
        print(f"Error saving transcript to Google Sheets: {e}")
        return False




def extract_page_id(url):
    # Extract the page ID from the URL for output file naming
    match = re.search(r'view_all_page_id=(\d+)', url)
    return match.group(1) if match else 'output'

def scrape_ads(url, driver_path, original_transparency_url=None):
    print(f"\nNavigating to {url}...")
    driver = None  # Initialize driver to None
    start_time = time.time()
    competitor_name_for_logging = urlparse(url).query # Fallback name for logging

    # --- Robust Main Execution Block ---
    try:
        # --- Variable Initialization ---
        current_page_id = None
        competitor_name_from_search_box = None
        total_ad_count_of_page = 0
        ads_data = {}
        total_child_ads_found = 0
        print(f"\n[START] Navigating to {url}...")

        # --- Driver Setup ---
        import tempfile
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--log-level=3")
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        options.add_argument(f'--user-data-dir={tempfile.mkdtemp()}')
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")

        service = Service(executable_path=driver_path)
        driver = webdriver.Chrome(service=service, options=options)
        wait = WebDriverWait(driver, 10)
        
        # Set a timeout for the initial page load to prevent hangs
        driver.set_page_load_timeout(60)
        driver.get(url)

        # ... (The rest of your scraping logic goes inside this try block) ...
        # (I've copied your logic below, with improved logging)
        
        print(f"[{url[-30:]}] Waiting for initial ad content to load...")
        initial_content_locator = (By.CSS_SELECTOR, 'div[class="xrvj5dj x18m771g x1p5oq8j xp48ta0 x18d9i69 xtssl2i xtqikln x1na6gtj x1jr1mh3 x15h0gye x7sq92a xlxr9qa"]')
        try:
            wait.until(EC.presence_of_element_located(initial_content_locator))
            print(f"[{url[-30:]}] ✅ Initial content loaded.")
        except TimeoutException:
            print(f"[{url[-30:]}] Timeout waiting for initial content. Checking for '0 results' message...")
            try:
                # Check for the "0 results" element.
                zero_results_locator = (By.XPATH, "//div[contains(text(), '0 results')]")
                driver.find_element(*zero_results_locator)
                print(f"[{url[-30:]}] ✅ Confirmed '0 results' on page. No ads to process.")

                # If no ads are found, prepare and send a payload to the API to update the count to 0.
                parsed_url = urlparse(url)
                query_params = parse_qs(parsed_url.query)
                current_page_id = query_params.get("view_all_page_id", [None])[0]
                
                competitor_name = "Unknown"
                try:
                    search_box_selectors = [
                        (By.CSS_SELECTOR, 'input[placeholder="Search by keyword or advertiser"][type="search"]'),
                        (By.XPATH, '//input[@type="search" and contains(@placeholder, "Search")]')
                    ]
                    for by, value in search_box_selectors:
                        try:
                            element = driver.find_element(by, value)
                            competitor_name = element.get_attribute("value")
                            if competitor_name:
                                break
                        except NoSuchElementException:
                            continue
                except Exception as e:
                    print(f"Could not fetch competitor name from search box for '0 ads' case: {e}")

                if competitor_name == "Unknown" and current_page_id:
                    competitor_name = f"Competitor_{current_page_id}"

                # Update Google Sheets for 0 ads case
                current_ip = get_current_ip()
                success = save_data_to_google_sheets(
                    {},  # Empty ads_data
                    competitor_name,
                    current_page_id,
                    current_ip
                )
                if success:
                    print(f"[{competitor_name}] Updated Google Sheets with 0 ads count.")
                else:
                    print(f"[{competitor_name}] Failed to update Google Sheets for 0 ads case.")
                
                # Exit the function since there's nothing more to scrape.
                return

            except NoSuchElementException:
                # If "0 results" is not found, it was a genuine timeout. Re-raise it.
                print(f"[{url[-30:]}] ❌ Timeout was not due to '0 results'. This is a genuine error.")
                raise

        time.sleep(random.uniform(0.5, 1.5))  # Human-like delay

        #fetch page id
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        current_page_id = query_params.get("view_all_page_id", [None])[0]
        
        # Use page_id or a snippet of the URL for logging prefix
        log_prefix = f"PageID: {current_page_id}" if current_page_id else f"Keyword: {query_params.get('q', ['unknown'])[0]}"
        print(f"[{log_prefix}] Extracted page_id: {current_page_id}")

        
        print("Extracted page_id:", current_page_id)


        # Robust selectors (based on stable attributes)
        search_box_selectors = [
            (By.CSS_SELECTOR, 'input[placeholder="Search by keyword or advertiser"][type="search"]'),
            (By.XPATH, '//input[@type="search" and contains(@placeholder, "Search")]')
        ]
        competitor_name_from_search_box = None
        for by, value in search_box_selectors:
            try:
                element = driver.find_element(by, value)
                competitor_name_from_search_box = element.get_attribute("value")
                break
            except NoSuchElementException:
                continue
        
        
        competitor_name_for_logging = competitor_name_from_search_box or log_prefix
        print(f"[{competitor_name_for_logging}] Competitor name from search box: {competitor_name_from_search_box}")
        time.sleep(random.uniform(0.5, 1.5))  # Human-like delay


        # Try to extract the div with "results" text using the correct element structure
        try:
            # First try the specific element structure from the HTML
            element = driver.find_element(By.XPATH, "//div[@aria-level='3'][@role='heading'][contains(@class, 'x8t9es0')][contains(text(), 'results')]")
            value_text = element.text.strip()
            print(f"Found count text (method 1): {value_text}")
        except NoSuchElementException:
            try:
                # Fallback to more general selector
                element = driver.find_element(By.XPATH, "//div[@role='heading'][contains(text(), 'results') or contains(text(), 'result')]")
                value_text = element.text.strip()
                print(f"Found count text (method 2): {value_text}")
            except NoSuchElementException:
                try:
                    # Final fallback to original method
                    element = driver.find_element(By.XPATH, "(//div[contains(text(), 'results')])[1]")
                    value_text = element.text.strip()
                    print(f"Found count text (method 3): {value_text}")
                except NoSuchElementException:
                    print("Ad count element not found with any method.")
                    value_text = ""

        # Parse the number from the string
        if value_text:
            print(f"Parsing ad count from: '{value_text}'")
            
            # Handle tilde (~) and extract numeric value with K/M suffixes
            # Examples: "~1,000 results", "950 results", "~5K results"
            match = re.search(r'~?(\d+(?:,\d+)*)([KMkm]?)\s*results?', value_text, re.IGNORECASE)
            
            if match:
                number_str = match.group(1).replace(',', '')  # Remove commas
                suffix = match.group(2).upper() if match.group(2) else ''
                
                try:
                    number = float(number_str)
                    if suffix == "K":
                        total_ad_count_of_page = int(number * 1_000)
                    elif suffix == "M":
                        total_ad_count_of_page = int(number * 1_000_000)
                    else:
                        total_ad_count_of_page = int(number)
                    
                    print(f"Successfully parsed ad count: {total_ad_count_of_page} (from '{value_text}')")
                except ValueError as e:
                    print(f"Error converting '{number_str}' to number: {e}")
                    total_ad_count_of_page = 0
            else:
                print(f"Could not parse ad count from text: '{value_text}'")
                total_ad_count_of_page = 0
        else:
            print("No ad count text found")
            total_ad_count_of_page = 0
                
        time.sleep(random.uniform(0.5, 1.5))  # Human-like delay
        # The rest of the script from scroll to JSON save and driver.quit will go here, but will use the local driver variable
        # ... (this will be filled in the next chunk)
        # At the end, return nothing (or could return stats if needed)
        # return driver, start_time

        # Target XPaths for end-of-list marker (unchanged)

        target_xpaths = [
            "/html/body/div[1]/div/div/div/div/div/div/div[1]/div/div/div/div[5]/div[2]/div[9]/div[3]/div[2]/div",
            "/html/body/div[1]/div/div/div/div/div/div[1]/div/div/div/div[6]/div[2]/div[9]/div[3]/div[2]/div"
        ]

        print("Starting scroll loop to load all ads...")
        scroll_count = 0
        last_height = driver.execute_script("return document.body.scrollHeight")
        scroll_pause_time = 0.7 # Reduced pause time after scroll
        max_scroll_attempts_at_bottom = 3 # How many times to scroll after height stops changing, just in case
        attempts_at_bottom = 0


        # scrolling part
        while attempts_at_bottom < max_scroll_attempts_at_bottom:
            # Scroll down to bottom
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            scroll_count += 1
            
            # --- Optimization: Shorter, dynamic wait ---
            time.sleep(scroll_pause_time) # Wait briefly for page to load

            print("Hmm, loading...")

            # Calculate new scroll height and compare with last scroll height
            new_height = driver.execute_script("return document.body.scrollHeight")
            
            element_found = False
            # Let's only check for the end element when the height hasn't changed
            if new_height == last_height:
                for xpath in target_xpaths:
                    try:
                        # Use a very short wait for the end element check
                        WebDriverWait(driver, 0.5).until(EC.presence_of_element_located((By.XPATH, xpath)))
                        print(f"✅ End-of-list element found using XPath: {xpath}")
                        element_found = True
                        break
                    except (NoSuchElementException, TimeoutException):
                        continue

            if element_found:
                print(f"✅ End-of-list element found after {scroll_count} scrolls. Stopping scroll.")
                break

            if new_height == last_height:
                attempts_at_bottom += 1
                print(f"Scroll height ({new_height}) hasn't changed. Attempt {attempts_at_bottom}/{max_scroll_attempts_at_bottom} at bottom...")
            else:
                attempts_at_bottom = 0 # Reset counter if height changed
                print(f"Scrolled {scroll_count} time(s). New height: {new_height}")

            last_height = new_height

            # Optional safety break: Prevent infinite loops
            if scroll_count > 500: # Adjust limit as needed
                print("⚠️ Reached maximum scroll limit (500). Stopping scroll.")
                break

            time.sleep(random.uniform(0.5, 1.5))  # Human-like delay

        if not element_found and attempts_at_bottom >= max_scroll_attempts_at_bottom:
            print("🏁 Reached bottom of page (height stabilized).")

        scroll_time = time.time()
        print(f"Scrolling finished in {scroll_time - start_time:.2f} seconds.")

        print("Waiting briefly for final elements to render...")
        time.sleep(1) # Short pause just in case rendering is slightly delayed

        # Count divs with the first class (unchanged selector logic)
        target_class_1 = "x6s0dn4 x78zum5 xdt5ytf xl56j7k x1n2onr6 x1ja2u2z x19gl646 xbumo9q"
        try:
            divs_1 = driver.find_elements(By.CSS_SELECTOR, f'div[class="{target_class_1}"]')
            print(f"Total <div> elements with target class 1: {len(divs_1)}")
        except Exception as e:
            print(f"Error finding elements with target class 1: {e}")
            divs_1 = []

        # Count divs with the second class (unchanged selector logic)
        # target_class_2 = "xrvj5dj x18m771g x1p5oq8j xbxaen2 x18d9i69 x1u72gb5 xtqikln x1na6gtj x1jr1mh3 xm39877 x7sq92a xxy4fzi"
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

        # --- Optimization: Process elements already found, minimize waits inside loop ---
        for i, div in enumerate(divs_2, 1):
            print("in loop")
            try:
                child_divs = div.find_elements(By.XPATH, './div[contains(@class, "xh8yej3")]')
                num_children = len(child_divs)
                print('num_children', num_children)
                total_child_ads_found += num_children

                # Process each xh8yej3 child
                for j, child_div in enumerate(child_divs, 1):
                    current_ad_id_for_logging = f"Group {i}, Ad {j}"
                    library_id = None # Initialize library_id for potential error logging
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

                        # Extract started_running, total_active_time
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

                                    # platforms_data.append({
                                    #     # "style": style, # Usually not needed in final data
                                    #     "mask_image": mask_image,
                                    #     "mask_position": mask_position,
                                    #     "platform_name": platform_name if platform_name else "Unknown"
                                    # })
                                    platforms_data.append(
                                        # "style": style, # Usually not needed in final data
                                        platform_name 
                                    )
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
                                    # Find the icon div within each category div
                                    icon_div = category_div.find_element(By.XPATH, './/div[contains(@class, "xtwfq29")]')
                                    style = icon_div.get_attribute("style")
                                    
                                    if style:
                                        mask_image_match = re.search(r'mask-image: url\("([^"]+)"\)', style)
                                        mask_pos_match = re.search(r'mask-position: ([^;]+)', style)
                                        mask_image = mask_image_match.group(1) if mask_image_match else None
                                        mask_position = mask_pos_match.group(1).strip() if mask_pos_match else None
                                        
                                        # Identify category name from mapping
                                        category_name = CATEGORY_MAPPING.get((mask_image, mask_position), "Unknown")
                                        
                                        # category_data.append({
                                        #     "mask_image": mask_image,
                                        #     "mask_position": mask_position,
                                        #     "category_name": category_name
                                        # })
                                        category_data.append(
                                            category_name
                                        )
                                except Exception as e:
                                    print(f"Could not process a category icon: {str(e)}")
                                    continue
                                    
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
                            ad_data["ads_count"] = None
                        except Exception as e:
                            print(f"Error extracting ads count for ad {current_ad_id_for_logging}: {str(e)}")
                            ad_data["ads_count"] = None

                        # Continue processing - don't add to ads_data yet as more data will be extracted

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

                        # Extract media, destination URL, and other missing fields
                        try:
                            # First find the xh8yej3 div inside child_div if we're not already looking at it
                            xh8yej3_div = child_div
                            if "xh8yej3" not in child_div.get_attribute("class"):
                                xh8yej3_div = child_div.find_element(By.XPATH, './/div[contains(@class, "xh8yej3")]')
                            
                            # Try to find the link container first as it often contains both media and CTA
                            link_container = xh8yej3_div.find_element(By.XPATH, './/a[contains(@class, "x1hl2dhg") and contains(@class, "x1lku1pv")]')
                            
                            # Extract and store the link URL for destination_url and landing_page
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
                            
                            # Store destination URL and landing page (they are the same)
                            ad_data["destination_url"] = actual_url
                            ad_data["landing_page"] = actual_url  # Same as destination_url
                            
                            # Generate source_url (Facebook ads library URL)
                            ad_data["source_url"] = f"https://www.facebook.com/ads/library/?id={library_id}"

                            # Initialize media fields
                            ad_data["media_type"] = None
                            ad_data["media_url"] = None
                            ad_data["thumbnail_url"] = None
                            
                            # Check for video first - comprehensive video extraction
                            video_found = False
                            try:
                                # List of possible video element selectors to try
                                video_selectors = [
                                    'video.x1lliihq.x5yr21d.xh8yej3',
                                    'video.x1lliihq.x5yr21d.xh8yej3.x1n2onr6',
                                    'video[class*="x1lliihq"][class*="x5yr21d"][class*="xh8yej3"]',
                                    'video[class*="x1lliihq"][class*="x5yr21d"]',
                                    'video[class*="x5yr21d"]',
                                    'video'
                                ]
                                
                                # Try each selector until we find a video
                                for selector in video_selectors:
                                    try:
                                        video_elements = child_div.find_elements(By.CSS_SELECTOR, selector)
                                        for video_element in video_elements:
                                            try:
                                                # Check if element is actually a video and has src attribute
                                                if video_element.tag_name.lower() == 'video':
                                                    # Extract video URL
                                                    media_url = video_element.get_attribute('src')
                                                    if media_url and media_url.strip() and 'blob:' not in media_url:
                                                        ad_data["media_type"] = "video"
                                                        ad_data["media_url"] = media_url
                                                        
                                                        # Extract thumbnail URL (poster attribute)
                                                        thumbnail_url = video_element.get_attribute('poster')
                                                        if thumbnail_url and thumbnail_url.strip() and 'data:image' not in thumbnail_url:
                                                            ad_data["thumbnail_url"] = thumbnail_url
                                                        
                                                        video_found = True
                                                        print(f"Found video with URL: {media_url[:100]}...")
                                                        if 'thumbnail_url' in ad_data:
                                                            print(f"Found thumbnail: {ad_data['thumbnail_url'][:100]}...")
                                                        break  # Found a valid video, exit the loop
                                            except Exception as e:
                                                print(f"Error processing video element: {str(e)}")
                                                continue
                                        
                                        if video_found:
                                            break  # Exit selector loop if video was found
                                            
                                    except NoSuchElementException:
                                        continue  # Try next selector if current one doesn't match
                                    except Exception as e:
                                        print(f"Error with selector {selector}: {str(e)}")
                                        continue
                                
                                # If video not found with CSS selectors, try XPath as fallback
                                if not video_found:
                                    try:
                                        video_elements = child_div.find_elements(
                                            By.XPATH,
                                            './/video[contains(@class, "x1lliihq") or contains(@class, "x5yr21d") or contains(@class, "xh8yej3")]'
                                        )
                                        
                                        for video_element in video_elements:
                                            try:
                                                media_url = video_element.get_attribute('src')
                                                if media_url and media_url.strip() and 'blob:' not in media_url:
                                                    ad_data["media_type"] = "video"
                                                    ad_data["media_url"] = media_url
                                                    
                                                    # Extract thumbnail URL (poster attribute)
                                                    thumbnail_url = video_element.get_attribute('poster')
                                                    if thumbnail_url and thumbnail_url.strip() and 'data:image' not in thumbnail_url:
                                                        ad_data["thumbnail_url"] = thumbnail_url
                                                    
                                                    video_found = True
                                                    print(f"Found video via XPath: {media_url[:100]}...")
                                                    break
                                            except Exception as e:
                                                print(f"Error processing fallback video element: {str(e)}")
                                                continue
                                            
                                    except Exception as e:
                                        print(f"Error in XPath fallback video search: {str(e)}")
                            
                            except Exception as e:
                                print(f"Unexpected error in video extraction: {str(e)}")
                            
                            # Only try to find images if no video was found
                            if not video_found and (not ad_data.get("media_url") or not ad_data.get("media_type")):
                                try:
                                    # First try with specific class names inside the primary link container
                                    img_elements = link_container.find_elements(By.XPATH, './/img[contains(@class, "x168nmei") or contains(@class, "_8nqq") or contains(@class, "x15mokao") or contains(@class, "x1ga7v0g") or contains(@class, "x16uus16") or contains(@class, "xbiv7yw") or contains(@class, "x1ll5gia") or contains(@class, "x19kjcj4") or contains(@class, "x642log") or contains(@class, "xh8yej3") or @src]')
                                    
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
                                    print(f"Error finding images: {str(e)}")
                            
                        except Exception as e:
                            print(f"Error extracting media, destination URL, or source URL for ad {current_ad_id_for_logging}: {str(e)}")
                            # Initialize with None if not already set
                            if "media_type" not in ad_data:
                                ad_data["media_type"] = None
                            if "media_url" not in ad_data:
                                ad_data["media_url"] = None
                            if "thumbnail_url" not in ad_data:
                                ad_data["thumbnail_url"] = None
                            if "destination_url" not in ad_data:
                                ad_data["destination_url"] = None
                            if "landing_page" not in ad_data:
                                ad_data["landing_page"] = None
                            if "source_url" not in ad_data:
                                ad_data["source_url"] = f"https://www.facebook.com/ads/library/?id={library_id}"

                        try:
                            # ① container that wraps headline + CTA area
                            cta_container = child_div.find_element(
                                By.XPATH,
                                './/div[contains(@class, "x6s0dn4 x2izyaf x78zum5 x1qughib '
                                'x15mokao x1ga7v0g xde0f50 x15x8krk xexx8yu xf159sx xwib8y2 xmzvs34")]'
                            )

                            # ② sub‑container that holds headline + legal copy
                            head_line_container = cta_container.find_element(
                                By.XPATH,
                                './/div[contains(@class, "x1iyjqo2 x2fvf9 x6ikm8r x10wlt62 xt0b8zv")]'
                            )

                            # ③ CTA button div
                            cta_div = cta_container.find_element(
                                By.XPATH,
                                './/div[contains(@class, "x2lah0s")]'
                            )

                            # -- CTA TEXT (existing)
                            cta_text_element = cta_div.find_element(
                                By.XPATH,
                                './/div[contains(@class, "x8t9es0 x1fvot60 xxio538 x1heor9g '
                                'xuxw1ft x6ikm8r x10wlt62 xlyipyv x1h4wwuj x1pd3egz xeuugli")]'
                            )
                            ad_data["cta_button_text"] = cta_text_element.text.strip()

                            # -- HEADLINE TEXT (NEW) -----------------------------------------------
                            try:
                                headline_element = head_line_container.find_element(
                                    By.XPATH,
                                    './/div[contains(@class, "x6ikm8r x10wlt62 xlyipyv x1mcwxda")]'
                                )
                                ad_data["headline_text"] = headline_element.text.strip()
                            except NoSuchElementException:
                                ad_data["headline_text"] = None
                            # ----------------------------------------------------------------------

                        except NoSuchElementException:
                            # No CTA container found ⇒ keep previous behaviour
                            ad_data["cta_button_text"] = None
                            ad_data["headline_text"] = None
                        except Exception as e:
                            print(f"Error extracting CTA or headline text for ad {current_ad_id_for_logging}: {str(e)}")
                            ad_data["cta_button_text"] = None
                            ad_data["headline_text"] = None
                        # Ensure all required fields exist before adding to main dictionary
                        ad_data = ensure_ad_data_fields(ad_data)
                        
                        # Add to main dictionary with library_id as key
                        ads_data[library_id] = ad_data
                        total_processed += 1
                        # Reduce console noise: print progress periodically instead of every ad
                        if total_processed % 50 == 0:
                            print(f"Processed {total_processed}/{total_child_ads_found} ads...")

                    except NoSuchElementException as e:
                        # This might happen if the structure is unexpected, often failure to find library ID
                        print(f"Critical element missing for ad {current_ad_id_for_logging}, skipping. Error: {e.msg}")
                        continue # Skip this child_div entirely if critical info (like ID) is missing
                    except Exception as e:
                        print(f"Unexpected error processing ad {current_ad_id_for_logging}: {str(e)}")
                        continue # Skip this child_div on unexpected errors

            except Exception as e:
                print(f"Error finding or processing xh8yej3 children for div group {i}: {str(e)}")
                continue

        processing_time = time.time()
        print(f"\nData extraction finished in {processing_time - scroll_time:.2f} seconds.")

        # Construct the final output using the REAL scraped variables
        final_output = {
            "competitor_name": competitor_name_from_search_box,
            "no_of_ads": total_ad_count_of_page,
            "page_id": current_page_id,
            "total_ads_found": total_child_ads_found,
            "total_ads_processed": len(ads_data), # The count of successfully processed ads
            "ads_data": ads_data,                 # The REAL dictionary of scraped ads
            "page_link": url,
        }
        # [END OF YOUR SCRAPING LOGIC]

        # --- FIX #2: Unique JSON Filename ---
        # Use the page ID or a timestamp to create a unique filename
        # Clean the page_id to remove any invalid characters for filename
        if current_page_id:
            # Remove any query parameters or invalid characters
            clean_page_id = re.sub(r'[^\w\-_]', '_', str(current_page_id))
            output_id = clean_page_id
        else:
            output_id = f"keyword_{int(time.time())}"
        output_file = f"ad_data_{output_id}.json"
        
        try:
            with open(output_file, "w", encoding='utf-8') as f:
                json.dump(final_output, f, indent=4, ensure_ascii=False)
            print(f"[{competitor_name_for_logging}] Successfully processed data for {len(final_output['ads_data'])} unique ads.")
            print(f"[{competitor_name_for_logging}] Data saved to {output_file}")
        except Exception as e:
            print(f"[{competitor_name_for_logging}] ⚠️ Error saving data to JSON file: {e}")

        # --- Google Sheets Submission ---
        current_ip = get_current_ip()
        if final_output.get("ads_data"):
            success = save_data_to_google_sheets(
                final_output["ads_data"], 
                competitor_name_from_search_box or "Unknown", 
                current_page_id, 
                current_ip,
                original_transparency_url,  # Pass the original transparency URL for better matching
                final_output.get("no_of_ads")  # Pass the total ads count from page
            )
            if success:
                print(f"[{competitor_name_for_logging}] Successfully saved {len(final_output['ads_data'])} ads to Google Sheets.")
            else:
                print(f"[{competitor_name_for_logging}] Failed to save data to Google Sheets.")
        else:
            print(f"[{competitor_name_for_logging}] No ad data to save to Google Sheets.")

        total_time = time.time()
        print(f"[COMPLETE] Total script execution time for {competitor_name_for_logging}: {total_time - start_time:.2f} seconds.")

    except Exception as e:
        # --- FIX #3: Catch All Other Errors ---
        # This will catch timeouts, crashes, or any other Python error in the thread.
        print(f"\n" + "="*60)
        print(f"FATAL ERROR while processing URL: {url}")
        print(f"Error Type: {type(e).__name__}")
        print(f"Error Details: {e}")
        import traceback
        traceback.print_exc()
        print("="*60 + "\n")

    finally:
        # --- FIX #1: Guaranteed Cleanup ---
        # This block will run ALWAYS, even if the 'try' block crashes.
        if driver:
            print(f"[CLEANUP] Quitting browser for {competitor_name_for_logging}.")
            driver.quit()


def fetch_competitors_urls():
    """
    Fetches the list of competitor URLs from Google Sheets Milk worksheet
    """
    return get_urls_from_milk_worksheet()

def process_urls_in_parallel(urls):
    """
    Process a list of URLs using ThreadPoolExecutor
    Each thread has 2 worker pools (Right now if total link are 4 , then its devided 4 into 2 links list. and open 4 browser. Each thread has 2 worker. (each worker will open one browser))
    """
    with ThreadPoolExecutor(max_workers=2) as executor:
        executor.map(scrape_ads, urls)

def split_list_into_two(lst):
    """
    Split a list into two parts. If the list has an odd length,
    the first part will have one more element than the second.
    """
    mid = (len(lst) + 1) // 2
    return lst[:mid], lst[mid:]

def run_parallel_scraping():
    """
    Fetches competitor URLs and runs the scraping process in a controlled parallel manner.

    This function is designed to be robust for CI/CD environments like GitHub Actions.
    It performs the following steps:
    1. Fetches the list of URLs to be scraped from the API.
    2. Performs a pre-emptive cleanup of today's data to prevent duplicates.
       - If cleanup fails, the script will exit with an error status.
    3. Uses a ThreadPoolExecutor to run the `scrape_ads` function for multiple URLs
       concurrently, significantly speeding up the total execution time.
    4. The number of concurrent browsers is configurable via an environment variable.
    """
    # --- Step 1: Fetch URLs to Process ---
    print("\n" + "="*50)
    print("--- Step 1: Fetching Competitor URLs from Google Sheets ---")
    print("="*50)
    
    urls = fetch_competitors_urls()
    
    if not urls:
        print("No URLs found to process. Exiting gracefully.")
        return
    # urls = ["https://www.facebook.com/ads/library/?active_status=all&ad_type=all&country=US&view_all_page_id=358831854864382&search_type=page&media_type=all"]
    print(f"Successfully fetched {len(urls)} URLs to process.")

    # --- Step 2: Verify Google Sheets Connection ---
    print("\n" + "="*50)
    print("--- Step 2: Verifying Google Sheets Connection ---")
    print("="*50)

    # Test connection to Google Sheets
    test_worksheet, _ = setup_google_sheets(worksheet_name="Ads Details")
    if not test_worksheet:
        print("\nFATAL: Failed to connect to Google Sheets. Please check credentials.json file.")
        sys.exit(1)
    
    print("Google Sheets connection successful. Proceeding to scrape data.")

    # --- NEW: Pre-install the WebDriver ONCE ---
    print("\n" + "="*50)
    print("--- Pre-installing WebDriver ---")
    print("="*50)
    try:
        driver_executable_path = ChromeDriverManager().install()
        print(f"WebDriver cached successfully at: {driver_executable_path}")
    except Exception as e:
        print(f"\nFATAL: Failed to install Chrome Driver. Error: {e}")
        sys.exit(1)

    # --- Step 3: Scrape Ads in Parallel ---
    print("\n" + "="*50)
    print("--- Step 3: Starting Parallel Scraping ---")
    print("="*50)
    
    # Control the maximum number of concurrent browsers.
    # For a standard GitHub Actions runner (2-core CPU), 2 is a safe starting point.
    # This can be overridden by setting a `MAX_WORKERS` environment variable.
    try:
        max_workers = int(os.getenv("MAX_WORKERS", "2"))
    except ValueError:
        print("Warning: Invalid MAX_WORKERS environment variable. Defaulting to 2.")
        max_workers = 2
        
    print(f"Configured to run with a maximum of {max_workers} concurrent browsers.")
    
    start_time = time.time()
    
    # Use a single ThreadPoolExecutor to manage all scraping tasks directly.
    # This is much simpler and more direct than the previous nested-thread model.
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Create a wrapper function to handle URL tuples
        def scrape_with_original_url(url_tuple):
            ads_url, original_transparency_url = url_tuple
            return scrape_ads(ads_url, driver_executable_path, original_transparency_url)
        
        # Map the list of URL tuples to the wrapper function
        executor.map(scrape_with_original_url, urls)
    
    end_time = time.time()
    total_time = end_time - start_time
    
    # --- Step 4: Completion Summary ---
    print("\n" + "="*50)
    print("--- Scraping Complete ---")
    print("="*50)
    print(f"All {len(urls)} URLs have been processed.")
    print(f"Total parallel execution time: {total_time:.2f} seconds (~{total_time / 60:.2f} minutes).")

if __name__ == "__main__":
    run_parallel_scraping()
