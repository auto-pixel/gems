from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
import time
import re
from datetime import datetime
from urllib.parse import urlparse, parse_qs
from selenium.webdriver.common.keys import Keys
import random
import os
import sys
import logging
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import tempfile
from threading import Lock
from collections import defaultdict

# Google Sheets imports
import gspread
from google.oauth2.service_account import Credentials

# ============== CONFIGURATION =====================
# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Google Sheets configuration
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

# Rate limiting configuration
RATE_LIMIT_DELAY = 2.0  # Seconds between API calls (increased for safety)
MAX_RETRIES = 5
BATCH_SIZE = 5  # Reduced batch size to be more conservative

# Global variables for rate limiting and batching
api_call_lock = Lock()
last_api_call_time = 0
pending_updates = defaultdict(list)  # Store pending updates for batching


def rate_limited_api_call(func, *args, **kwargs):
    """
    Execute API call with rate limiting and retry logic.
    """
    global last_api_call_time
    
    with api_call_lock:
        # Ensure we don't exceed rate limits
        current_time = time.time()
        time_since_last_call = current_time - last_api_call_time
        if time_since_last_call < RATE_LIMIT_DELAY:
            sleep_time = RATE_LIMIT_DELAY - time_since_last_call
            time.sleep(sleep_time)
        
        # Retry logic with exponential backoff
        for attempt in range(MAX_RETRIES):
            try:
                result = func(*args, **kwargs)
                last_api_call_time = time.time()
                return result
            except Exception as e:
                if "429" in str(e) or "RATE_LIMIT_EXCEEDED" in str(e) or "quota" in str(e).lower():
                    wait_time = (2 ** attempt) * RATE_LIMIT_DELAY
                    logger.warning(f"Rate limit hit, waiting {wait_time:.1f}s before retry {attempt + 1}/{MAX_RETRIES}")
                    time.sleep(wait_time)
                    if attempt == MAX_RETRIES - 1:
                        logger.error(f"Max retries exceeded for API call: {e}")
                        raise
                else:
                    logger.error(f"Non-rate-limit API error: {e}")
                    raise
        
        last_api_call_time = time.time()
        return None


def get_google_sheets_client(credentials_file):
    """
    Initialize and return Google Sheets client.
    """
    try:
        credentials = Credentials.from_service_account_file(
            credentials_file, scopes=SCOPES
        )
        client = gspread.authorize(credentials)
        return client
    except Exception as e:
        logger.error(f"Failed to initialize Google Sheets client: {e}")
        return None


def get_urls_from_sheets(sheet_name, worksheet_name, credentials_file):
    """
    Fetch URLs from Google Sheets from 'Page Transparency' column.
    """
    try:
        client = get_google_sheets_client(credentials_file)
        if not client:
            return []
        
        sheet = client.open(sheet_name)
        worksheet = sheet.worksheet(worksheet_name)
        
        # Get all records from the worksheet
        records = worksheet.get_all_records()
        
        # Extract URLs from 'Page Transparency' column
        urls = []
        for i, record in enumerate(records, start=2):  # Start from row 2 (header is row 1)
            url = record.get('Page Transperancy ')  # Note: keeping original spelling
            if url and url.strip():
                urls.append((url.strip(), i))  # Store URL with row number
        
        logger.info(f"Retrieved {len(urls)} URLs from 'Page Transperancy ' column")
        return urls
        
    except Exception as e:
        logger.error(f"Error fetching URLs from Google Sheets: {e}")
        return []


def batch_update_sheets(sheet_name, worksheet_name, credentials_file):
    """
    Process all pending updates in batches to reduce API calls.
    """
    global pending_updates
    
    if not pending_updates[sheet_name]:
        return
    
    try:
        client = get_google_sheets_client(credentials_file)
        if not client:
            return
        
        sheet = rate_limited_api_call(client.open, sheet_name)
        worksheet = rate_limited_api_call(sheet.worksheet, worksheet_name)
        
        # Get column positions once
        try:
            ad_count_col = rate_limited_api_call(worksheet.find, 'no.of ads By Ai').col
        except:
            logger.error("Could not find 'no.of ads By Ai' column")
            return
            
        zero_streak_col = None
        try:
            zero_streak_col = rate_limited_api_call(worksheet.find, 'Zero Ads Streak').col
        except gspread.exceptions.CellNotFound:
            # Add the column header if it doesn't exist
            headers = rate_limited_api_call(worksheet.row_values, 1)
            new_col = len(headers) + 1
            rate_limited_api_call(worksheet.update_cell, 1, new_col, 'Zero Ads Streak')
            zero_streak_col = new_col
            logger.info("Created 'Zero Ads Streak' column")
        
        # Find timestamp column
        updated_col = None
        try:
            updated_col = rate_limited_api_call(worksheet.find, 'last_updated').col
        except gspread.exceptions.CellNotFound:
            pass
        
        # Process updates in batches
        updates_to_process = pending_updates[sheet_name][:BATCH_SIZE]
        pending_updates[sheet_name] = pending_updates[sheet_name][BATCH_SIZE:]
        
        # Prepare batch updates
        batch_updates = []
        rows_to_delete = []
        
        for update_data in updates_to_process:
            row_number, ad_count, competitor_name = update_data
            
            # Get current streak value
            current_streak = 0
            try:
                current_streak_value = rate_limited_api_call(worksheet.cell, row_number, zero_streak_col).value
                current_streak = int(current_streak_value) if current_streak_value and current_streak_value.isdigit() else 0
            except:
                current_streak = 0
            
            # Add ad count update
            batch_updates.append({
                'range': f'{gspread.utils.rowcol_to_a1(row_number, ad_count_col)}',
                'values': [[ad_count]]
            })
            
            # Handle Zero Ads Streak logic
            if ad_count == 0:
                new_streak = current_streak + 1
                batch_updates.append({
                    'range': f'{gspread.utils.rowcol_to_a1(row_number, zero_streak_col)}',
                    'values': [[new_streak]]
                })
                
                if new_streak >= 30:
                    rows_to_delete.append(row_number)
                    logger.info(f"Marking row {row_number} for deletion after 30 consecutive days of zero ads")
            else:
                if current_streak > 0:
                    batch_updates.append({
                        'range': f'{gspread.utils.rowcol_to_a1(row_number, zero_streak_col)}',
                        'values': [[0]]
                    })
            
            # Add timestamp update if column exists
            if updated_col:
                batch_updates.append({
                    'range': f'{gspread.utils.rowcol_to_a1(row_number, updated_col)}',
                    'values': [[datetime.now().strftime('%Y-%m-%d %H:%M:%S')]]
                })
            
            logger.info(f"Prepared update for {competitor_name}: {ad_count} (Row {row_number})")
        
        # Execute batch update
        if batch_updates:
            rate_limited_api_call(worksheet.batch_update, batch_updates)
            logger.info(f"Batch updated {len(updates_to_process)} rows")
        
        # Delete rows if needed (in reverse order to maintain row numbers)
        for row_number in sorted(rows_to_delete, reverse=True):
            rate_limited_api_call(worksheet.delete_rows, row_number)
            logger.info(f"Deleted row {row_number}")
            
    except Exception as e:
        logger.error(f"Error in batch update: {e}")
        # Re-add failed updates back to pending
        pending_updates[sheet_name].extend(updates_to_process)


def update_sheets_with_ad_count(sheet_name, worksheet_name, credentials_file, url, ad_count, competitor_name, row_number):
    """
    Queue update for batch processing to reduce API calls.
    """
    global pending_updates
    
    # Add to pending updates
    pending_updates[sheet_name].append((row_number, ad_count, competitor_name))
    logger.info(f"Queued update for {competitor_name}: {ad_count} (Row {row_number})")
    
    # Process batch if we have enough updates or this is the last update
    if len(pending_updates[sheet_name]) >= BATCH_SIZE:
        batch_update_sheets(sheet_name, worksheet_name, credentials_file)
    
    return True


def flush_pending_updates(sheet_name, worksheet_name, credentials_file):
    """
    Process all remaining pending updates.
    """
    global pending_updates
    
    while pending_updates[sheet_name]:
        batch_update_sheets(sheet_name, worksheet_name, credentials_file)
        time.sleep(RATE_LIMIT_DELAY)  # Small delay between batches


def extract_page_id(url):
    """Extract page ID from Facebook Ads Library URL."""
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    return query_params.get("view_all_page_id", [None])[0]


def extract_ad_count_only(url_data, driver_path, sheet_name, worksheet_name, credentials_file):
    """
    Extract only the ad count from Facebook Ads Library page.
    """
    url, row_number = url_data  # Unpack URL and row number
    driver = None
    page_name = url[-30:]  # For logging
    
    try:
        logger.info(f"Starting ad count extraction for: {page_name}")
        
        # --- Driver Setup ---
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
        wait = WebDriverWait(driver, 15)
        
        driver.set_page_load_timeout(60)
        driver.get(url)
        
        # Extract page ID and competitor name
        current_page_id = extract_page_id(url)
        
        # Get competitor name from search box
        competitor_name = "Unknown"
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
        
        if competitor_name == "Unknown" and current_page_id:
            competitor_name = f"Competitor_{current_page_id}"
        
        logger.info(f"Competitor name: {competitor_name}")
        
        # Handle popups and close buttons
        def handle_popups_and_close_buttons():
            """Handle popups and close buttons that might interfere with ad count extraction."""
            try:
                # Press ESC to close any popups
                logger.info(f"Pressing ESC to close potential popups for '{page_name}'")
                driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.ESCAPE)
                time.sleep(2)
                
                # Look for common close button patterns and click them
                close_button_selectors = [
                    "//button[@aria-label='Close']",
                    "//button[contains(@class, 'close')]",
                    "//div[@role='button' and contains(@aria-label, 'Close')]",
                    "//span[contains(@class, 'close')]",
                    "//i[contains(@class, 'close')]",
                    "//button[text()='×']",
                    "//button[text()='Close']",
                    "//div[contains(@class, 'modal')]//button",
                    "//div[contains(@class, 'popup')]//button",
                    "//div[contains(@class, 'overlay')]//button"
                ]
                
                for selector in close_button_selectors:
                    try:
                        close_buttons = driver.find_elements(By.XPATH, selector)
                        for button in close_buttons:
                            if button.is_displayed() and button.is_enabled():
                                logger.info(f"Found and clicking close button for '{page_name}': {selector}")
                                button.click()
                                time.sleep(1)
                                break
                    except Exception:
                        continue
            except Exception as e:
                logger.warning(f"Error handling popups: {e}")
        
        # Handle popups first
        handle_popups_and_close_buttons()
        
        # Wait for either the ad count element or "No ads" message
        try:
            # Try to find the exact element structure from the examples
            try:
                # First try the exact structure from examples
                ad_count_element = wait.until(
                    EC.presence_of_element_located((
                        By.XPATH, 
                        "//div[@role='heading' and @aria-level='3' and contains(@class, 'x8t9es0')]"
                    ))
                )
                
                ad_count_text = ad_count_element.text.strip()
                logger.info(f"Found ad count text: {ad_count_text}")
                
                # Extract number using regex that handles all example cases
                match = re.search(r'[~]?(\d{1,3}(?:,\d{3})*|\d+)', ad_count_text)
                if match:
                    ad_count = int(match.group(1).replace(',', ''))
                    logger.info(f"Extracted ad count for '{page_name}': {ad_count}")
                    
                    # Update Google Sheets
                    update_sheets_with_ad_count(sheet_name, worksheet_name, credentials_file, url, ad_count, competitor_name, row_number)
                    return ad_count
                
                # If no numbers found, check for "0 results" case
                if '0 results' in ad_count_text:
                    update_sheets_with_ad_count(sheet_name, worksheet_name, credentials_file, url, 0, competitor_name, row_number)
                    return 0
                    
            except Exception as e:
                logger.warning(f"Error with exact element match: {str(e)}")
            
            # Fallback to more general patterns if exact match fails
            logger.info("Trying fallback patterns for ad count extraction")
            
            # Try to find any element containing 'results' text
            try:
                results_elements = wait.until(
                    EC.presence_of_all_elements_located((
                        By.XPATH, 
                        "//*[contains(text(), 'result')]"
                    ))
                )
                
                for element in results_elements:
                    try:
                        text = element.text.strip()
                        match = re.search(r'[~]?(\d{1,3}(?:,\d{3})*|\d+)\s+results?', text, re.IGNORECASE)
                        if match:
                            ad_count = int(match.group(1).replace(',', ''))
                            logger.info(f"Fallback extracted ad count for '{page_name}': {ad_count}")
                            
                            # Update Google Sheets
                            update_sheets_with_ad_count(sheet_name, worksheet_name, credentials_file, url, ad_count, competitor_name, row_number)
                            return ad_count
                    except:
                        continue
                        
            except Exception as e:
                logger.warning(f"Fallback pattern failed: {str(e)}")
            
            logger.warning(f"Could not extract numeric ad count from page")
            return None
                
        except TimeoutException:
            # Check if "No ads" message is present
            try:
                no_ads_element = driver.find_element(By.XPATH, "//div[contains(text(), 'No ads')]")
                logger.info(f"Page '{page_name}' has no ads")
                update_sheets_with_ad_count(sheet_name, worksheet_name, credentials_file, url, 0, competitor_name, row_number)
                return 0
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
                        matches = re.search(r'~?(\d+(?:,\d+)?)', ad_count_text)
                        if matches:
                            ad_count = int(matches.group(1).replace(',', ''))
                            logger.info(f"JavaScript-extracted ad count for '{page_name}': {ad_count}")
                            
                            # Update Google Sheets
                            update_sheets_with_ad_count(sheet_name, worksheet_name, credentials_file, url, ad_count, competitor_name, row_number)
                            return ad_count
                except Exception as js_error:
                    logger.warning(f"JavaScript ad count extraction failed: {str(js_error)}")
                
                return None
    
    except Exception as e:
        logger.error(f"Error extracting ad count from {page_name}: {str(e)}")
        return None
    
    finally:
        if driver:
            try:
                driver.quit()
            except Exception as e:
                logger.warning(f"Error closing driver: {e}")


def process_urls_from_sheets(sheet_name, worksheet_name, credentials_file, max_workers=2):
    """
    Process URLs from Google Sheets to extract ad counts.
    """
    try:
        # Get URLs from Google Sheets
        urls = get_urls_from_sheets(sheet_name, worksheet_name, credentials_file)
        
        if not urls:
            logger.warning("No URLs found in Google Sheets")
            return
        
        logger.info(f"Processing {len(urls)} URLs from Google Sheets")
        
        # Pre-install WebDriver
        try:
            driver_executable_path = ChromeDriverManager().install()
            logger.info(f"WebDriver installed at: {driver_executable_path}")
        except Exception as e:
            logger.error(f"Failed to install Chrome Driver: {e}")
            return
        
        # Process URLs in parallel
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Create partial function with all required parameters
            extract_task = partial(
                extract_ad_count_only, 
                driver_path=driver_executable_path,
                sheet_name=sheet_name,
                worksheet_name=worksheet_name,
                credentials_file=credentials_file
            )
            
            # Map URLs to extraction tasks
            results = list(executor.map(extract_task, urls))
        
        end_time = time.time()
        total_time = end_time - start_time
        
        # Summary
        successful_extractions = sum(1 for result in results if result is not None)
        logger.info(f"Processing complete. {successful_extractions}/{len(urls)} URLs processed successfully in {total_time:.2f} seconds")
        
        # Flush any remaining pending updates
        logger.info("Flushing remaining pending updates...")
        flush_pending_updates(sheet_name, worksheet_name, credentials_file)
        logger.info("All updates completed.")
        
    except Exception as e:
        logger.error(f"Error processing URLs from sheets: {e}")
        # Still try to flush pending updates even if there was an error
        try:
            flush_pending_updates(sheet_name, worksheet_name, credentials_file)
        except Exception as flush_error:
            logger.error(f"Error flushing pending updates: {flush_error}")


def main():
    """Main function to run the scraper - configured for GitHub Actions."""
    
    # Default configuration for GitHub Actions
    sheet_name = 'Master Auto Swipe - Test ankur'
    worksheet_name = 'Milk'
    credentials_file = 'credentials.json'
    max_workers = int(os.getenv("MAX_WORKERS", "2"))  # Allow override via environment variable
    
    logger.info("Starting Facebook Ad Count Scraper (GitHub Actions Mode)")
    logger.info(f"Sheet: {sheet_name}")
    logger.info(f"Worksheet: {worksheet_name}")
    logger.info(f"Credentials: {credentials_file}")
    logger.info(f"Max Workers: {max_workers}")
    
    # Check if credentials file exists
    if not os.path.exists(credentials_file):
        logger.error(f"Credentials file not found: {credentials_file}")
        logger.error("Please ensure credentials.json is available in the repository")
        sys.exit(1)
    
    try:
        # Process URLs from Google Sheets
        process_urls_from_sheets(
            sheet_name=sheet_name,
            worksheet_name=worksheet_name,
            credentials_file=credentials_file,
            max_workers=max_workers
        )
        logger.info("Script completed successfully")
        
    except Exception as e:
        logger.error(f"Script failed with error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
