from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from webdriver_manager.firefox import GeckoDriverManager
import time
import re
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
from google.auth.exceptions import GoogleAuthError
import logging
import os
import sys
import random

# Import anti-detection utilities
from fb_antidetect_utils import (
    ProxyManager,
    create_stealth_driver,
    perform_human_like_scroll,
    simulate_random_mouse_movements,    
    add_random_delays
)

# Check if running in GitHub Actions
is_github_actions = os.environ.get('GITHUB_ACTIONS') == 'true'

# Set up logging system
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"page_transparency_scraper.log")

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
    
    # Print progress updates immediately
    if message.startswith("PROGRESS ["):
        sys.stdout.flush()

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
def setup_google_sheets(sheet_name="Master Auto Swipe - Test ankur", worksheet_name="Milk", credentials_path="credentials.json"):
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
            
            # Check for Page Transperancy with trailing space 
            if header == "Page Transperancy " or header == "Page Transperancy":
                transparency_col_idx = i + 1  # gspread is 1-indexed
                custom_print(f"Found Page Transperancy column at index {transparency_col_idx}: '{header}'")
            # Fallback to case-insensitive matching if needed
            elif header.lower().strip() in ["page transperancy", "page transparency"]:
                transparency_col_idx = i + 1  # gspread is 1-indexed
                custom_print(f"Found Page Transperancy column at index {transparency_col_idx} via fallback: '{header}'")
        
        if not page_col_idx or not transparency_col_idx:
            custom_print("Could not find required columns in the Milk worksheet!", "warning")
            return [], {}
            
        # Get all records with transparency links
        all_values = worksheet.get_all_values()
        urls = []
        url_row_mapping = {}
        
        # Skip header row
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
        
        custom_print(f"Extracted {len(urls)} Page Transparency URLs from worksheet in exact sheet order")
        custom_print(f"URLs will be processed strictly from row {sorted_rows[0]} to {sorted_rows[-1]}")
        return urls, url_row_mapping, page_col_idx, transparency_col_idx
        
    except Exception as e:
        custom_print(f"Error extracting URLs from worksheet: {e}", "error")
        return [], {}, None, None

# Function to update progress percentage
def update_progress_percentage(processed_count, total_count):
    """Update and display the progress percentage"""
    if total_count > 0:
        percentage = (processed_count / total_count) * 100
        # Create progress bar with # for completed and - for remaining
        bar_length = 20
        completed_length = int(bar_length * processed_count / total_count)
        progress_bar = '#' * completed_length + '-' * (bar_length - completed_length)
        
        # Format using the consistent format from memory
        progress_message = f"PROGRESS [PageTransparency]: [{progress_bar}] {percentage:.1f}% ({processed_count}/{total_count} URLs)"
        print(progress_message)
        sys.stdout.flush()  # Ensure progress is displayed immediately

# Main execution
if __name__ == "__main__":
    custom_print("Starting Page Transparency Scraper")
    
    # Connect to Google Sheets
    custom_print("Connecting to Google Sheets...")
    sheet_name = "Master Auto Swipe - Test ankur"
    
    # Set up the Milk worksheet connection
    milk_worksheet = setup_google_sheets(sheet_name=sheet_name, worksheet_name="Milk")
    if not milk_worksheet:
        custom_print("Failed to connect to Milk worksheet. Exiting.", "error")
        sys.exit(1)
    
    # Extract URLs from the worksheet
    urls, url_row_mapping, page_col_idx, transparency_col_idx = extract_transparency_urls(milk_worksheet)
    
    if not urls:
        custom_print("No Page Transparency URLs found to process. Exiting.", "warning")
        sys.exit(0)
    
    custom_print(f"Found {len(urls)} Page Transparency URLs to process")
    
    # Set up stealth driver with anti-detection measures
    custom_print("Creating stealth browser driver with anti-detection measures")
    # Check if we're in GitHub Actions
    is_github_actions = os.environ.get('GITHUB_ACTIONS') == 'true'
    
    if is_github_actions:
        custom_print("Running in GitHub Actions environment - using enhanced stealth settings")
        # Set environment variables for GitHub Actions to improve browser stability
        os.environ['MOZ_HEADLESS'] = '1'
        os.environ['MOZ_DISABLE_GPU_SANDBOX'] = '1'
        os.environ['MOZ_DISABLE_CONTENT_SANDBOX'] = '1'
        
        # Use standard create_stealth_driver call - the detection in fb_antidetect_utils will handle GitHub Actions settings
        driver = create_stealth_driver(
            use_proxy=(proxy_manager is not None),
            proxy_manager=proxy_manager,
            headless=True  # Must be headless in GitHub Actions
        )
    else:
        # Use regular settings for local environment
        driver = create_stealth_driver(
            use_proxy=(proxy_manager is not None),
            proxy_manager=proxy_manager,
            headless=True  # Set to False to see the browser in action
        )
    
    # Configure dynamic wait times (variable to appear more human-like)
    # Use longer wait times in CI environment
    is_github_actions = os.environ.get('GITHUB_ACTIONS') == 'true'
    
    if is_github_actions:
        custom_print("Running in GitHub Actions environment - using extended timeouts")
        wait_time = 45  # Fixed longer timeout for GitHub Actions (45 seconds)
        page_load_timeout = 60  # Longer page load timeout for GitHub Actions
        driver.set_page_load_timeout(page_load_timeout)
        custom_print(f"Set page load timeout to {page_load_timeout} seconds for GitHub Actions")
    else:
        wait_time = random.uniform(8, 12)  # Random wait for local environment (8-12 seconds)
        
    custom_print(f"Using element wait timeout of {wait_time} seconds")
    wait = WebDriverWait(driver, wait_time)
    
    # Process URLs in sequential order (top to bottom) as they appear in the sheet
    custom_print("Processing URLs in sequential order (top to bottom)")
    
    # Process each URL
    processed_count = 0
    update_progress_percentage(processed_count, len(urls))
    
    for url in urls:
        try:
            # Navigate to the transparency URL
            custom_print(f"Processing URL: {url}")
            driver.get(url)
            
            # Add random delay to look human-like
            add_random_delays(2.0, 4.0)
            
            # Wait for the page name element to be visible
            try:
                # Try multiple selectors to find the page name element
                selectors = [
                    # Original specific selector
                    "//div[@aria-level='1' and contains(@class, 'x8t9es0') and contains(@class, 'x1ldc4aq') and contains(@class, 'x1xlr1w8') and contains(@class, 'x1cgboj8') and contains(@class, 'x4hq6eo') and contains(@class, 'xq9mrsl') and contains(@class, 'x1yc453h') and contains(@class, 'x1h4wwuj') and contains(@class, 'xeuugli') and @role='heading']",
                    # Broader selector - any heading with aria-level 1
                    "//div[@role='heading' and @aria-level='1']",
                    # Very broad h1 selector
                    "//h1",
                    # Fallback to any element containing the page name
                    "//div[contains(@class, 'x1yc453h')]"
                ]
                
                # Try each selector until one works
                page_name_element = None
                for selector in selectors:
                    try:
                        custom_print(f"Trying selector: {selector}")
                        page_name_element = wait.until(EC.visibility_of_element_located((By.XPATH, selector)))
                        if page_name_element:
                            custom_print(f"Found element using selector: {selector}")
                            break
                    except (TimeoutException, NoSuchElementException):
                        custom_print(f"Selector failed: {selector}", "warning")
                        continue
                        
                if not page_name_element:
                    raise TimeoutException("All selectors failed to find page name element")
                
                # Extract the page name
                page_name = page_name_element.text.strip()
                custom_print(f"Found page name: {page_name}")
                
                # Update the milk sheet with the page name
                if page_name and url in url_row_mapping:
                    row_index = url_row_mapping[url]
                    milk_worksheet.update_cell(row_index, page_col_idx, page_name)
                    custom_print(f"Updated row {row_index}, column {page_col_idx} with page name: {page_name}")
                else:
                    custom_print(f"Unable to update sheet: page_name={page_name}, url_in_mapping={url in url_row_mapping}", "warning")
            
            except TimeoutException:
                custom_print(f"Timed out waiting for page name element on URL: {url}", "warning")
                
                # Add more robust recovery methods for GitHub Actions
                try:
                    # Take a screenshot to debug in GitHub Actions
                    if is_github_actions:
                        try:
                            screenshot_path = os.path.join("logs", f"fb_error_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
                            driver.save_screenshot(screenshot_path)
                            custom_print(f"Saved error screenshot to {screenshot_path}", "info")
                        except Exception as ss_err:
                            custom_print(f"Failed to save screenshot: {ss_err}", "error")
                    
                    # Try to get the page title as a fallback
                    try:
                        page_title = driver.title
                        if page_title and "Facebook" in page_title:
                            # Extract page name from title if possible
                            title_parts = page_title.split("|")
                            if len(title_parts) > 1:
                                page_name = title_parts[0].strip()
                                custom_print(f"Extracted page name from title: {page_name}")
                                
                                # Update the milk sheet with the page name
                                if page_name and url in url_row_mapping:
                                    row_index = url_row_mapping[url]
                                    milk_worksheet.update_cell(row_index, page_col_idx, page_name)
                                    custom_print(f"Updated row {row_index}, column {page_col_idx} with page name from title: {page_name}")
                                    continue  # Skip to next URL since we succeeded
                    except Exception as title_err:
                        custom_print(f"Failed to extract from title: {title_err}", "error")
                    
                    # If in GitHub Actions, retry with longer wait and page refresh
                    if is_github_actions:
                        custom_print("GitHub Actions recovery attempt: refreshing page and retrying", "info")
                        driver.refresh()
                        time.sleep(5)  # Wait for refresh
                        
                        # Try a very broad selector as last resort
                        try:
                            # Look for any text that might contain the page name
                            elements = driver.find_elements(By.XPATH, "//div[contains(@class, 'x1yc453h')][string-length(text()) > 3]")
                            if elements:
                                page_name = elements[0].text.strip()
                                custom_print(f"Found possible page name using last resort method: {page_name}")
                                
                                # Update the milk sheet with the page name
                                if page_name and url in url_row_mapping:
                                    row_index = url_row_mapping[url]
                                    milk_worksheet.update_cell(row_index, page_col_idx, page_name)
                                    custom_print(f"Updated row {row_index}, column {page_col_idx} with possible page name: {page_name}")
                                else:
                                    custom_print(f"Unable to update sheet with last resort method", "warning")
                        except Exception as last_err:
                            custom_print(f"Last resort method failed: {last_err}", "error")
                    
                except Exception as recovery_err:
                    custom_print(f"All recovery attempts failed for URL: {url}. Error: {recovery_err}", "error")
            
            except NoSuchElementException as e:
                custom_print(f"Element not found: {e}", "error")
            
            except Exception as e:
                custom_print(f"Error processing URL {url}: {e}", "error")
        
        except Exception as e:
            custom_print(f"Error navigating to URL {url}: {e}", "error")
        
        finally:
            # Update progress regardless of success or failure
            processed_count += 1
            update_progress_percentage(processed_count, len(urls))
            
            # Add random delay between URLs to avoid detection
            add_random_delays(1.0, 3.0)
    
    # Clean up
    custom_print("Finished processing all URLs")
    driver.quit()
    custom_print("Browser closed")
