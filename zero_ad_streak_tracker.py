import os
import time
import logging
import re
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
from webdriver_manager.chrome import ChromeDriverManager
import gspread
from google.oauth2.service_account import Credentials
from google.auth.exceptions import GoogleAuthError
import random

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

class FacebookAdScraper:
    def __init__(self, sheet_name, worksheet_name, credentials_path="credentials.json"):
        """Initialize the Facebook Ad Scraper.
        
        Args:
            sheet_name (str): Name of the Google Sheets document
            worksheet_name (str): Name of the worksheet/tab within the document
            credentials_path (str): Path to Google API credentials
        """
        self.sheet_name = sheet_name
        self.worksheet_name = worksheet_name
        self.credentials_path = credentials_path
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
        """Set up Selenium WebDriver with Chrome in headless mode."""
        try:
            chrome_options = Options()
            # Remove headless mode for debugging - you can add it back later
            # chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument("--disable-notifications")
            chrome_options.add_argument("--disable-infobars")
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            # Suppress common Chrome warnings/errors
            chrome_options.add_argument("--disable-logging")
            chrome_options.add_argument("--log-level=3")
            chrome_options.add_argument("--disable-gpu-logging")
            chrome_options.add_argument("--disable-software-rasterizer")
            chrome_options.add_argument("--disable-background-timer-throttling")
            chrome_options.add_argument("--disable-renderer-backgrounding")
            chrome_options.add_argument("--disable-features=TranslateUI")
            chrome_options.add_argument("--disable-default-apps")
            chrome_options.add_argument("--no-first-run")
            chrome_options.add_argument("--disable-web-security")
            chrome_options.add_argument("--allow-running-insecure-content")
            
            # Rotate user agents to avoid detection
            user_agents = [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ]
            chrome_options.add_argument(f"--user-agent={random.choice(user_agents)}")
            
            # Use webdriver_manager to handle ChromeDriver installation
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            
            # Execute script to remove webdriver property
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            logger.info("Selenium WebDriver initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error setting up Selenium: {str(e)}")
            return False
    
    def random_delay(self, min_seconds=2, max_seconds=5):
        """Add random delay to avoid detection."""
        delay = random.uniform(min_seconds, max_seconds)
        time.sleep(delay)
    
    def get_page_id_from_page(self, page_url, page_name):
        """Navigate to the Facebook page, go to About section, and extract Page ID from transparency.
        
        Args:
            page_url (str): URL of the Facebook page
            page_name (str): Name of the Facebook page
            
        Returns:
            tuple: (page_id, transparency_url) or (None, None) if extraction fails or transparency link not found
        """
        if not page_url or not isinstance(page_url, str):
            logger.warning(f"Invalid page URL for '{page_name}': {page_url}")
            return None, None
        
        if not page_url.startswith("http"):
            page_url = f"https://{page_url}" if not page_url.startswith("www.") else f"https://{page_url}"
            
        # Check if the URL contains 'about_profile_transparency', if not, return early
        if '/about_profile_transparency' not in page_url:
            logger.info(f"No transparency link found in sheet for '{page_name}'. Skipping...")
            return None, None
        
        try:
            logger.info(f"Navigating to page URL for '{page_name}': {page_url}")
            self.driver.get(page_url)
            self.random_delay(5, 8)  # Allow page to fully load with random delay

            # Handle potential cookies/consent banner
            try:
                # Try to click "Accept All" or similar cookie consent button
                cookie_buttons = [
                    "//button[contains(text(), 'Accept all')]",
                    "//button[contains(text(), 'Accept All')]",
                    "//button[contains(text(), 'Allow all')]",
                    "//div[@data-testid='cookie-policy-manage-dialog-accept-button']"
                ]
                for button_xpath in cookie_buttons:
                    try:
                        cookie_button = WebDriverWait(self.driver, 3).until(
                            EC.element_to_be_clickable((By.XPATH, button_xpath))
                        )
                        self.driver.execute_script("arguments[0].click();", cookie_button)
                        logger.info("Clicked cookie consent button")
                        self.random_delay(2, 3)
                        break
                    except TimeoutException:
                        continue
            except Exception:
                pass  # Continue if no cookie banner found

            # Extract Page ID using multiple methods for robustness
            try:
                # Method 1: Look for page ID in the URL parameters or page source
                current_url = self.driver.current_url
                page_id_match = re.search(r'page_id=(\d+)', current_url)
                if page_id_match:
                    page_id = page_id_match.group(1)
                    logger.info(f"Method 1: Extracted Page ID from URL for '{page_name}': {page_id}")
                else:
                    # Method 2: Look in page source for page ID patterns
                    page_source = self.driver.page_source
                    
                    # Look for common patterns where page ID appears
                    patterns = [
                        r'"page_id":"(\d+)"',
                        r'"pageID":"(\d+)"',
                        r'"id":"(\d{10,})"',
                        r'pageID=(\d+)',
                        r'page_id=(\d+)'
                    ]
                    
                    page_id = None
                    for pattern in patterns:
                        match = re.search(pattern, page_source)
                        if match:
                            page_id = match.group(1)
                            logger.info(f"Method 2: Extracted Page ID from source for '{page_name}': {page_id}")
                            break
                
                # Method 3: Try to find the page ID in transparency section
                if not page_id:
                    try:
                        # Look for numeric text that could be page ID
                        numeric_elements = self.driver.find_elements(By.XPATH, "//div[text()[normalize-space()]]")
                        for element in numeric_elements:
                            text = element.text.strip()
                            if text.isdigit() and len(text) >= 10:  # Page IDs are typically 10+ digits
                                page_id = text
                                logger.info(f"Method 3: Found potential Page ID for '{page_name}': {page_id}")
                                break
                    except Exception as e:
                        logger.warning(f"Method 3 failed: {str(e)}")
                
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
            
            # Navigate to the URL
            self.driver.get(url)
            self.random_delay(8, 12)  # Give more time for content to load
            
            # Handle potential cookies/consent banner again
            try:
                cookie_buttons = [
                    "//button[contains(text(), 'Accept all')]",
                    "//button[contains(text(), 'Accept All')]",
                    "//button[contains(text(), 'Allow all')]"
                ]
                for button_xpath in cookie_buttons:
                    try:
                        cookie_button = WebDriverWait(self.driver, 3).until(
                            EC.element_to_be_clickable((By.XPATH, button_xpath))
                        )
                        self.driver.execute_script("arguments[0].click();", cookie_button)
                        logger.info("Clicked cookie consent button")
                        self.random_delay(2, 3)
                        break
                    except TimeoutException:
                        continue
            except Exception:
                pass
            
            # Wait for content to load
            wait = WebDriverWait(self.driver, 20)
            
            ad_count = None
            ad_count_text = None
            
            try:
                # Target the specific Facebook class for ad count results
                # Using CSS selector for the exact class combination
                css_selector = "div.x8t9es0.x1uxerd5.xrohxju.x108nfp6.xq9mrsl.x1h4wwuj.x117nqv4.xeuugli"
                
                # Wait for the element with the specific class to be present
                result_element = wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, css_selector))
                )
                
                ad_count_text = result_element.text.strip()
                logger.info(f"Found ad count text: {ad_count_text}")
                
            except TimeoutException:
                # Fallback: Try finding by XPath with the class attributes
                try:
                    xpath_selector = "//div[@class='x8t9es0 x1uxerd5 xrohxju x108nfp6 xq9mrsl x1h4wwuj x117nqv4 xeuugli']"
                    result_element = wait.until(
                        EC.presence_of_element_located((By.XPATH, xpath_selector))
                    )
                    ad_count_text = result_element.text.strip()
                    logger.info(f"Found ad count text with XPath: {ad_count_text}")
                except TimeoutException:
                    logger.warning(f"Could not find element with specific class for '{page_name}'")
            
            # If still no text found, try JavaScript approach targeting the specific class
            if not ad_count_text:
                try:
                    ad_count_text = self.driver.execute_script("""
                        // Look for the specific class combination
                        const targetElement = document.querySelector('div.x8t9es0.x1uxerd5.xrohxju.x108nfp6.xq9mrsl.x1h4wwuj.x117nqv4.xeuugli');
                        if (targetElement) {
                            return targetElement.textContent.trim();
                        }
                        
                        // Fallback: Look for any element with this class pattern
                        const elements = document.querySelectorAll('div[class*="x8t9es0"][class*="x1uxerd5"]');
                        for (const el of elements) {
                            const text = el.textContent.trim();
                            if (text.includes('result') || text.includes('ads')) {
                                return text;
                            }
                        }
                        return null;
                    """)
                    if ad_count_text:
                        logger.info(f"JavaScript found ad count text: {ad_count_text}")
                except Exception as js_error:
                    logger.warning(f"JavaScript extraction failed: {str(js_error)}")
            
            # Parse the ad count from the text
            if ad_count_text:
                logger.info(f"Processing text: '{ad_count_text}'")
                
                # Extract only the numeric value from text like "0 results", "100 results", "~380 results", etc.
                # Handle tilde (~) for approximate counts
                matches = re.search(r'^~?(\d+(?:,\d+)*)', ad_count_text.strip())
                if matches:
                    # Remove commas and convert to int
                    ad_count = int(matches.group(1).replace(',', ''))
                    logger.info(f"Extracted ad count for '{page_name}': {ad_count}")
                    return ad_count
                else:
                    logger.warning(f"Could not extract numeric ad count from: '{ad_count_text}'")
                    # If text doesn't start with a number, might be "No results" or similar
                    if 'no' in ad_count_text.lower() and ('result' in ad_count_text.lower() or 'ads' in ad_count_text.lower()):
                        logger.info(f"Page '{page_name}' has no ads (text: '{ad_count_text}')")
                        return 0
                    return None
            else:
                # Last resort: save screenshot and page source for debugging
                screenshot_path = f"debug_{page_name.replace(' ', '_').replace('/', '_')}.png"
                html_path = f"debug_{page_name.replace(' ', '_').replace('/', '_')}.html"
                
                try:
                    self.driver.save_screenshot(screenshot_path)
                    with open(html_path, "w", encoding="utf-8") as f:
                        f.write(self.driver.page_source)
                    logger.warning(f"Could not find ad count. Debug files saved: {screenshot_path}, {html_path}")
                except Exception as save_error:
                    logger.warning(f"Could not save debug files: {str(save_error)}")
                
                logger.warning(f"Could not find ad count element for '{page_name}'")
                return None
                
        except Exception as e:
            logger.error(f"Error extracting ad count for '{page_name}': {str(e)}")
            return None
        
    def process_sheet(self):
        """Process all rows in the Google Sheet."""
        if not self.setup_google_sheets():
            logger.error("Failed to set up Google Sheets. Exiting.")
            return
            
        if not self.setup_selenium():
            logger.error("Failed to set up Selenium. Exiting.")
            return
            
        try:
            # Get all values from the worksheet
            all_values = self.worksheet.get_all_values()
            
            if not all_values:
                logger.error("Worksheet is empty. Exiting.")
                return
                
            # Get headers and find required columns
            headers = all_values[0]
            
            try:
                # Handle column names carefully, stripping spaces and making case-insensitive
                column_mapping = {header.strip().lower(): idx for idx, header in enumerate(headers)}
                logger.info(f"Column mapping: {column_mapping}")

                # Find Page link column (matching 'Page ' with space)
                page_link_col_idx = None
                for name in ["page ", "page"]:
                    if name in column_mapping:
                        page_link_col_idx = column_mapping[name]
                        break

                # Find Page Transparency column (matching 'Page Transperancy ' with typo and space)
                url_col_idx = None
                for name in ["page transperancy ", "page transperancy", "page transparency ", "page transparency"]:
                    if name in column_mapping:
                        url_col_idx = column_mapping[name]
                        break

                if page_link_col_idx is None or url_col_idx is None:
                    # Last resort: try to find columns by position or partial match
                    if page_link_col_idx is None:
                        for idx, header in enumerate(headers):
                            if "page link" in header.lower():
                                page_link_col_idx = idx
                                break

                    if url_col_idx is None:
                        for idx, header in enumerate(headers):
                            if "transperancy" in header.lower() or "transparency" in header.lower():
                                url_col_idx = idx
                                break

                if page_link_col_idx is None or url_col_idx is None:
                    logger.error("Required columns not found. Please check your sheet headers.")
                    logger.error(f"Available columns are: {headers}")
                    return

                logger.info(f"Found Page link column at index {page_link_col_idx}")
                logger.info(f"Found Page Transparency column at index {url_col_idx}")

            except ValueError as e:
                logger.error(f"Required columns not found in the worksheet: {str(e)}")
                # Print available columns to help debug
                logger.error(f"Available columns are: {headers}")
                return
                
            # Check if "no.of ads By Ai" column exists, add if it doesn't
            ads_col_idx = None
            for idx, header in enumerate(headers):
                if "no.of ads by ai" in header.lower():
                    ads_col_idx = idx
                    break
                    
            if ads_col_idx is None:
                ads_col_idx = len(headers)
                self.worksheet.update_cell(1, ads_col_idx + 1, "no.of ads By Ai")
                logger.info("Added 'no.of ads By Ai' column")
            
            # Check if "Last Update Time" column exists, add if it doesn't
            time_col_idx = None
            for idx, header in enumerate(headers):
                if "last update time" in header.lower():
                    time_col_idx = idx
                    break
                    
            if time_col_idx is None:
                time_col_idx = len(headers) + (1 if ads_col_idx == len(headers) else 0)
                self.worksheet.update_cell(1, time_col_idx + 1, "Last Update Time")
                logger.info("Added 'Last Update Time' column")
                
            # Check if "Zero Ads Streak" column exists, add if it doesn't
            streak_col_idx = None
            for idx, header in enumerate(headers):
                if "zero ads streak" in header.lower():
                    streak_col_idx = idx
                    break
                    
            if streak_col_idx is None:
                # Add the column after the last existing column
                streak_col_idx = max(len(headers), ads_col_idx + 1, time_col_idx + 1)
                self.worksheet.update_cell(1, streak_col_idx + 1, "Zero Ads Streak")
                logger.info("Added 'Zero Ads Streak' column")

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
                        current_time = time.strftime("%Y-%m-%d %H:%M:%S")
                        
                        # Get current streak value
                        current_streak = 0
                        if len(row) > streak_col_idx and row[streak_col_idx].strip():
                            try:
                                current_streak = int(float(row[streak_col_idx]))
                            except (ValueError, TypeError):
                                current_streak = 0
                        
                        # Update streak based on ad count
                        if ad_count == 0:
                            # Increment streak
                            new_streak = current_streak + 1
                            
                            # Check if we've reached 30 days
                            if new_streak >= 30:
                                logger.info(f"Row {row_idx}: Reached 30+ days of zero ads. Deleting row...")
                                self.worksheet.delete_rows(row_idx)
                                logger.info(f"Deleted row {row_idx} after 30+ days of zero ads")
                                continue  # Skip the rest of the loop since row was deleted
                            
                            # Update streak in sheet
                            self.worksheet.update_cell(row_idx, streak_col_idx + 1, str(new_streak))
                            logger.info(f"Row {row_idx}: Updated zero ads streak to {new_streak}")
                        elif current_streak > 0:
                            # Reset streak if there are ads now
                            self.worksheet.update_cell(row_idx, streak_col_idx + 1, "0")
                            logger.info(f"Row {row_idx}: Reset zero ads streak (found {ad_count} ads)")
                        
                        # Update ad count and timestamp
                        self.worksheet.update_cell(row_idx, ads_col_idx + 1, ad_count)
                        self.worksheet.update_cell(row_idx, time_col_idx + 1, current_time)
                        logger.info(f"Updated ad count for row {row_idx}: {ad_count} and timestamp: {current_time}")
                        self.successful_processed += 1
                    else:
                        logger.warning(f"Failed to extract ad count for row {row_idx}")
                        self.failed_processed += 1
                    
                    # Add random delay to avoid rate limiting
                    self.random_delay(3, 8)
                    
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
    
    parser = argparse.ArgumentParser(description='Facebook Ad Scraper')
    parser.add_argument('--sheet_name', default='Master Auto Swipe - Test ankur',
                        help='Name of the Google Sheets document (default: Master Auto Swipe - Test ankur)')
    parser.add_argument('--worksheet_name', default='Milk',
                        help='Name of the worksheet/tab within the document (default: Milk)')
    parser.add_argument('--credentials', default='credentials.json', 
                        help='Path to Google API credentials file (default: credentials.json)')
    
    args = parser.parse_args()
    
    logger.info("Starting Facebook Ad Scraper")
    scraper = FacebookAdScraper(args.sheet_name, args.worksheet_name, args.credentials)
    scraper.process_sheet()
    logger.info("Facebook Ad Scraper completed")

if __name__ == "__main__":
    main()
