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
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument("--disable-notifications")
            chrome_options.add_argument("--disable-infobars")
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36")
            
            # Use webdriver_manager to handle ChromeDriver installation
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            logger.info("Selenium WebDriver initialized successfully")
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
                    self.driver.get(about_url)
                    time.sleep(3)

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
                                    if (/^\d{12,}$/.test(text)) {
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
            
            # Navigate to the URL
            self.driver.get(url)
            time.sleep(5)  # Give more time for content to load
            
            # Wait for content to load (the ad count element)
            wait = WebDriverWait(self.driver, 15)
            
            # Wait for either the ad count element or "No ads" message
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
                    
                    logger.warning(f"Could not find ad count or 'No ads' message for '{page_name}'")
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
                # Handle column names carefully, stripping spaces
                column_mapping = {header.strip().lower(): idx for idx, header in enumerate(headers)}

                # Look for 'Page ' column (note the trailing space)
                page_link_col_idx = None
                for name in ["page", "page ", "page link", "page link "]:
                    if name in column_mapping:
                        page_link_col_idx = column_mapping[name]
                        break

                url_col_idx = None
                for name in ["page transperancy", "page transperancy ", "page transparency", "page transparency "]:
                    if name in column_mapping:
                        url_col_idx = column_mapping[name]
                        break
                        
                # Look for ads count column
                ads_col_idx = None
                for name in ["no.of ads by ai", "ads count", "ads"]:
                    if name in column_mapping:
                        ads_col_idx = column_mapping[name]
                        break
                        
                # Look for Zero Ads Streak column
                streak_col_idx = None
                for name in ["zero ads streak", "streak"]:
                    if name in column_mapping:
                        streak_col_idx = column_mapping[name]
                        break

                # Last resort: try to find columns by position or partial match
                if page_link_col_idx is None:
                    for idx, header in enumerate(headers):
                        if "page" in header.lower() and ("link" in header.lower() or header.strip().lower() == "page"):
                            page_link_col_idx = idx
                            break

                if url_col_idx is None:
                    for idx, header in enumerate(headers):
                        if "transperancy" in header.lower() or "transparency" in header.lower():
                            url_col_idx = idx
                            break
                            
                if ads_col_idx is None:
                    for idx, header in enumerate(headers):
                        if "ads" in header.lower() and "ai" in header.lower():
                            ads_col_idx = idx
                            break
                            
                if streak_col_idx is None:
                    for idx, header in enumerate(headers):
                        if "streak" in header.lower() and "zero" in header.lower():
                            streak_col_idx = idx
                            break

                # Check if required columns are found
                missing_columns = []
                if page_link_col_idx is None:
                    missing_columns.append("Page column (looking for 'Page ' or 'Page link')")
                if url_col_idx is None:
                    missing_columns.append("Page Transperancy column")
                if ads_col_idx is None:
                    missing_columns.append("Ads count column (looking for 'no.of ads By Ai')")
                if streak_col_idx is None:
                    missing_columns.append("Zero Ads Streak column")
                    
                if missing_columns:
                    logger.error(f"Required columns not found: {', '.join(missing_columns)}")
                    logger.error(f"Available columns are: {headers}")
                    logger.error("Please ensure your sheet has the correct column headers.")
                    return
                    
                logger.info(f"Found columns - Page: {page_link_col_idx}, Transperancy: {url_col_idx}, Ads: {ads_col_idx}, Streak: {streak_col_idx}")

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
                        
                        # Update Zero Ads Streak
                        if streak_col_idx is not None:
                            if ad_count > 0:
                                self.worksheet.update_cell(row_idx, streak_col_idx + 1, 0)
                                logger.info(f"Reset Zero Ads Streak for row {row_idx}")
                            else:
                                try:
                                    current_streak = int(all_values[row_idx - 1][streak_col_idx] or 0)
                                    new_streak = current_streak + 1
                                    
                                    if new_streak > 30:
                                        self.worksheet.delete_rows(row_idx)
                                        logger.info(f"Deleted row {row_idx} - Zero Ads Streak exceeded 30")
                                    else:
                                        self.worksheet.update_cell(row_idx, streak_col_idx + 1, new_streak)
                                        logger.info(f"Incremented Zero Ads Streak for row {row_idx} to {new_streak}")
                                except Exception as e:
                                    self.worksheet.update_cell(row_idx, streak_col_idx + 1, 1)
                                    logger.warning(f"Could not read streak for row {row_idx}, setting to 1. Error: {e}")
                        
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
    
    parser = argparse.ArgumentParser(description='Facebook Ad Scraper')
    parser.add_argument('--sheet_name', default='Debt 2025 Swipe File ',
                        help='Name of the Google Sheets document (default: Debt 2025 Swipe File )')
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
