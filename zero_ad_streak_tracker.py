"""
Integrated Facebook Ad Scraper with Zero-Ad Streak Tracker
Combines ad scraping and streak tracking with subtle human behavior for fast processing.

Features:
- Facebook ad count extraction
- Zero-ad streak tracking with 30-day deletion
- Subtle human behavior patterns
- Fast batch processing
- Comprehensive logging
"""

import os
import time
import logging
import re
import random
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
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass, field

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("integrated_fb_scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class ProcessingResults:
    """Data class to store processing results"""
    total_processed: int = 0
    successful_ads: int = 0
    failed_ads: int = 0
    streak_updated: int = 0
    streak_deleted: int = 0
    streak_errors: int = 0
    deleted_ips: List[str] = field(default_factory=list)

class HumanBehavior:
    """Simulates subtle human browsing patterns for faster but more natural automation"""
    
    @staticmethod
    def random_delay(min_seconds=0.5, max_seconds=2.0):
        """Random delay with human-like patterns - kept short for speed"""
        delay = random.uniform(min_seconds, max_seconds)
        time.sleep(delay)
    
    @staticmethod
    def typing_delay():
        """Very short typing delay"""
        time.sleep(random.uniform(0.1, 0.3))
    
    @staticmethod
    def scroll_behavior(driver):
        """Quick scroll simulation"""
        if random.random() < 0.3:  # 30% chance to scroll
            scroll_amount = random.randint(100, 300)
            driver.execute_script(f"window.scrollBy(0, {scroll_amount});")
            time.sleep(0.2)
    
    @staticmethod
    def mouse_movement_delay():
        """Short mouse movement simulation"""
        time.sleep(random.uniform(0.1, 0.5))

class IntegratedFacebookScraper:
    def __init__(self, sheet_name, worksheet_name, credentials_path="credentials.json"):
        """Initialize the Integrated Facebook Scraper with streak tracking."""
        self.sheet_name = sheet_name
        self.worksheet_name = worksheet_name
        self.credentials_path = credentials_path
        self.worksheet = None
        self.driver = None
        self.human = HumanBehavior()
        
        # Results tracking
        self.results = ProcessingResults()
        
    def setup_google_sheets(self):
        """Set up connection to Google Sheets."""
        try:
            scope = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            
            creds = Credentials.from_service_account_file(
                self.credentials_path, scopes=scope
            )
            client = gspread.authorize(creds)
            
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
        """Set up Selenium WebDriver with human-like characteristics."""
        try:
            chrome_options = Options()
            
            # Human-like browser settings
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            chrome_options.add_argument("--window-size=1366,768")  # Common resolution
            chrome_options.add_argument("--disable-notifications")
            chrome_options.add_argument("--disable-infobars")
            
            # Rotate user agents for variety
            user_agents = [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ]
            chrome_options.add_argument(f"--user-agent={random.choice(user_agents)}")
            
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            
            # Remove automation indicators
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            logger.info("Selenium WebDriver initialized with human-like settings")
            return True
            
        except Exception as e:
            logger.error(f"Error setting up Selenium: {str(e)}")
            return False
    
    def get_page_id_from_page(self, page_url, page_name):
        """Navigate to Facebook page and extract Page ID with human behavior."""
        if not page_url or not isinstance(page_url, str):
            logger.warning(f"Invalid page URL for '{page_name}': {page_url}")
            return None, None
        
        if not page_url.startswith("http"):
            page_url = f"https://{page_url}" if not page_url.startswith("www.") else f"https://{page_url}"
        
        try:
            logger.info(f"Navigating to page URL for '{page_name}': {page_url}")
            self.driver.get(page_url)
            
            # Human-like page load behavior
            self.human.random_delay(2, 4)
            self.human.scroll_behavior(self.driver)

            if '/about_profile_transparency' not in page_url:
                # Click on About tab with human behavior
                try:
                    about_tab = WebDriverWait(self.driver, 10).until(
                        EC.element_to_be_clickable((By.XPATH, "//a[contains(text(), 'About')]"))
                    )
                    self.human.mouse_movement_delay()
                    self.driver.execute_script("arguments[0].click();", about_tab)
                    logger.info(f"Clicked on About tab for '{page_name}'")
                    self.human.random_delay(1, 2)
                except Exception as e:
                    logger.warning(f"Could not click on About tab for '{page_name}': {str(e)}")
                    about_url = f"{page_url.rstrip('/')}/about"
                    logger.info(f"Trying direct navigation to About page: {about_url}")
                    self.driver.get(about_url)
                    self.human.random_delay(1, 2)

                # Click on Page transparency section
                try:
                    transparency_section = WebDriverWait(self.driver, 10).until(
                        EC.element_to_be_clickable((By.XPATH, "//a[contains(text(), 'Page transparency') or contains(text(), 'Page Transparency')]"))
                    )
                    self.human.mouse_movement_delay()
                    self.driver.execute_script("arguments[0].click();", transparency_section)
                    logger.info(f"Clicked on Page transparency section for '{page_name}'")
                    self.human.random_delay(1, 2)
                except Exception as e:
                    logger.warning(f"Could not click on Page transparency section for '{page_name}': {str(e)}")
                    return None, None

            # Extract Page ID using multiple methods
            try:
                # Method 1: Direct XPath
                try:
                    page_id_label = WebDriverWait(self.driver, 5).until(
                        EC.presence_of_element_located((By.XPATH, "//div[contains(text(), 'Page ID')]"))
                    )
                    page_id_element = WebDriverWait(self.driver, 5).until(
                        EC.presence_of_element_located((By.XPATH, "//div[contains(text(), 'Page ID')]/following-sibling::div"))
                    )
                    page_id = page_id_element.text.strip()
                    logger.info(f"Method 1: Extracted Page ID for '{page_name}': {page_id}")
                except (TimeoutException, NoSuchElementException, StaleElementReferenceException):
                    page_id = None
                
                # Method 2: JavaScript extraction
                if not page_id or not page_id.isdigit():
                    try:
                        page_id = self.driver.execute_script("""
                            const selectors = [
                                'span[class*="193iq5w"]',
                                'span[dir="auto"]',
                                'div[class*="xzsf02u"]',
                                'div'
                            ];
                            
                            for (const selector of selectors) {
                                const elements = document.querySelectorAll(selector);
                                for (const el of elements) {
                                    const text = el.textContent.trim();
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
                
                # Method 3: HTML regex as fallback
                if not page_id or not page_id.isdigit():
                    html = self.driver.page_source
                    id_match = re.search(r'>\s*(\d{12,})\s*<', html)
                    if id_match:
                        page_id = id_match.group(1)
                        logger.info(f"Method 3: Extracted Page ID from HTML for '{page_name}': {page_id}")
                
                if page_id and page_id.isdigit():
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
        """Extract ad count with human behavior patterns."""
        if not url or not isinstance(url, str):
            logger.warning(f"Invalid URL for page '{page_name}': {url}")
            return None
            
        try:
            logger.info(f"Processing page: '{page_name}'")
            logger.info(f"Opening URL: {url}")
            
            # Navigate with human behavior
            self.driver.get(url)
            self.human.random_delay(2, 4)  # Initial load time
            self.human.scroll_behavior(self.driver)
            
            wait = WebDriverWait(self.driver, 12)
            
            try:
                # Look for ad count with human-like waiting
                ad_count_element = wait.until(
                    EC.presence_of_element_located((By.XPATH, "//div[contains(text(), 'results') or contains(text(), 'result')]"))
                )
                ad_count_text = ad_count_element.text
                logger.info(f"Found ad count text: {ad_count_text}")
                
                matches = re.search(r'~?(\d+(?:,\d+)?)', ad_count_text)
                if matches:
                    ad_count = int(matches.group(1).replace(',', ''))
                    logger.info(f"Extracted ad count for '{page_name}': {ad_count}")
                    return ad_count
                else:
                    logger.warning(f"Could not extract numeric ad count from: {ad_count_text}")
                    return 0
                    
            except TimeoutException:
                # Check for "No ads" message
                try:
                    no_ads_element = self.driver.find_element(By.XPATH, "//div[contains(text(), 'No ads')]")
                    logger.info(f"Page '{page_name}' has no ads")
                    return 0
                except NoSuchElementException:
                    # JavaScript fallback
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
                    
                    logger.warning(f"Could not find ad count for '{page_name}'")
                    return None
                
        except Exception as e:
            logger.error(f"Error extracting ad count for '{page_name}': {str(e)}")
            return None
    
    def find_columns(self, headers):
        """Find required columns efficiently."""
        column_mapping = {header.strip().lower(): idx for idx, header in enumerate(headers)}
        
        # Find columns
        page_link_col = None
        for name in ["page ", "page"]:
            if name in column_mapping:
                page_link_col = column_mapping[name]
                break
        
        url_col = None
        for name in ["page transperancy ", "page transperancy", "page transparency ", "page transparency"]:
            if name in column_mapping:
                url_col = column_mapping[name]
                break
        
        ads_col = None
        for idx, header in enumerate(headers):
            if "no.of ads by ai" in header.lower():
                ads_col = idx
                break
        
        ip_col = None
        for idx, header in enumerate(headers):
            if 'ip' in header.lower() and 'address' in header.lower():
                ip_col = idx
                break
        
        streak_col = None
        for idx, header in enumerate(headers):
            if 'zero' in header.lower() and 'ads' in header.lower() and 'streak' in header.lower():
                streak_col = idx
                break
        
        time_col = None
        for idx, header in enumerate(headers):
            if "last update time" in header.lower():
                time_col = idx
                break
        
        return page_link_col, url_col, ads_col, ip_col, streak_col, time_col
    
    def process_sheet_integrated(self):
        """Process sheet with integrated ad scraping and streak tracking."""
        if not self.setup_google_sheets():
            logger.error("Failed to set up Google Sheets. Exiting.")
            return self.results
            
        if not self.setup_selenium():
            logger.error("Failed to set up Selenium. Exiting.")
            return self.results
            
        try:
            # Get all data
            all_values = self.worksheet.get_all_values()
            
            if not all_values:
                logger.error("Worksheet is empty. Exiting.")
                return self.results
            
            headers = all_values[0]
            page_link_col, url_col, ads_col, ip_col, streak_col, time_col = self.find_columns(headers)
            
            # Add missing columns if needed
            if ads_col is None:
                ads_col = len(headers)
                self.worksheet.update_cell(1, ads_col + 1, "no.of ads By Ai")
                logger.info("Added 'no.of ads By Ai' column")
            
            if time_col is None:
                time_col = len(headers) + (1 if ads_col == len(headers) else 0)
                self.worksheet.update_cell(1, time_col + 1, "Last Update Time")
                logger.info("Added 'Last Update Time' column")
            
            if streak_col is None:
                streak_col = len(headers) + (1 if ads_col == len(headers) else 0) + (1 if time_col is None else 0)
                self.worksheet.update_cell(1, streak_col + 1, "Zero Ads Streak")
                logger.info("Added 'Zero Ads Streak' column")
            
            # Batch processing containers
            batch_updates = []
            rows_to_delete = []
            
            # Process each row
            for row_idx, row in enumerate(all_values[1:], start=2):
                try:
                    if len(row) <= max(page_link_col or 0, url_col or 0):
                        logger.warning(f"Row {row_idx} has insufficient columns. Skipping.")
                        continue

                    page_link = row[page_link_col] if page_link_col is not None and page_link_col < len(row) else ""
                    transparency_url = row[url_col] if url_col is not None and url_col < len(row) else ""
                    ip_address = row[ip_col] if ip_col is not None and ip_col < len(row) else ""
                    
                    # Skip if no IP address
                    if not ip_address.strip():
                        continue
                    
                    # Handle transparency URL extraction
                    if not transparency_url.strip() and page_link.strip():
                        about_transparency_url = f"{page_link.rstrip('/')}/about_profile_transparency"
                        logger.info(f"Row {row_idx}: Extracting page ID from {about_transparency_url}")
                        page_id, constructed_url = self.get_page_id_from_page(about_transparency_url, page_link)
                        if page_id and constructed_url:
                            self.worksheet.update_cell(row_idx, url_col + 1, constructed_url)
                            transparency_url = constructed_url
                        else:
                            self.results.failed_ads += 1
                            continue
                    elif not page_link.strip() and not transparency_url.strip():
                        continue
                    
                    # Extract ad count
                    ad_count = self.extract_ad_count(transparency_url, page_link or f"Row {row_idx}")
                    
                    if ad_count is not None:
                        # Update ad count and timestamp
                        current_time = time.strftime("%Y-%m-%d %H:%M:%S")
                        batch_updates.append({
                            'range': f'{chr(65 + ads_col)}{row_idx}',
                            'values': [[str(ad_count)]]
                        })
                        batch_updates.append({
                            'range': f'{chr(65 + time_col)}{row_idx}',
                            'values': [[current_time]]
                        })
                        
                        # Handle streak logic
                        current_streak = 0
                        if streak_col < len(row) and row[streak_col]:
                            try:
                                current_streak = int(float(row[streak_col]))
                            except (ValueError, TypeError):
                                current_streak = 0
                        
                        if ad_count == 0:
                            # Increment streak
                            new_streak = current_streak + 1
                            if new_streak >= 30:
                                rows_to_delete.append((row_idx, ip_address))
                                self.results.deleted_ips.append(ip_address)
                                logger.info(f"Row {row_idx} (IP: {ip_address}): 30+ day streak - marking for deletion")
                            else:
                                batch_updates.append({
                                    'range': f'{chr(65 + streak_col)}{row_idx}',
                                    'values': [[str(new_streak)]]
                                })
                                self.results.streak_updated += 1
                        elif ad_count > 0 and current_streak > 0:
                            # Reset streak
                            batch_updates.append({
                                'range': f'{chr(65 + streak_col)}{row_idx}',
                                'values': [['0']]
                            })
                            logger.info(f"Row {row_idx}: Reset streak (had {ad_count} ads after {current_streak} day streak)")
                            self.results.streak_updated += 1
                        
                        self.results.successful_ads += 1
                        logger.info(f"Updated row {row_idx}: {ad_count} ads, timestamp: {current_time}")
                    else:
                        self.results.failed_ads += 1
                    
                    # Human behavior between requests
                    self.human.random_delay(1, 3)
                    
                except Exception as e:
                    logger.error(f"Error processing row {row_idx}: {str(e)}")
                    self.results.failed_ads += 1
                
                self.results.total_processed += 1
            
            # Perform batch updates
            if batch_updates:
                logger.info(f"Performing batch update for {len(batch_updates)} cells...")
                try:
                    self.worksheet.batch_update(batch_updates)
                    logger.info("Batch update completed successfully")
                except Exception as e:
                    logger.error(f"Batch update failed: {e}")
            
            # Delete rows with 30+ day streaks
            if rows_to_delete:
                logger.info(f"Deleting {len(rows_to_delete)} rows with 30+ day streaks...")
                rows_to_delete.sort(key=lambda x: x[0], reverse=True)
                
                for row_num, ip_address in rows_to_delete:
                    try:
                        self.worksheet.delete_rows(row_num)
                        logger.info(f"Deleted row {row_num} (IP: {ip_address})")
                        self.results.streak_deleted += 1
                        time.sleep(1)  # Delay between deletions
                    except Exception as e:
                        logger.error(f"Failed to delete row {row_num}: {e}")
                        self.results.streak_errors += 1
            
            logger.info("Integrated processing completed")
            return self.results

        except Exception as e:
            logger.error(f"Error in integrated processing: {str(e)}")
            return self.results
        finally:
            if self.driver:
                self.driver.quit()
                logger.info("WebDriver closed")

def main():
    """Main function with argument parsing."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Integrated Facebook Ad Scraper with Streak Tracking')
    parser.add_argument('--sheet_name', default='Master Auto Swipe - Test ankur',
                        help='Name of the Google Sheets document')
    parser.add_argument('--worksheet_name', default='Milk',
                        help='Name of the worksheet/tab within the document')
    parser.add_argument('--credentials', default='credentials.json', 
                        help='Path to Google API credentials file')
    
    args = parser.parse_args()
    
    logger.info("Starting Integrated Facebook Ad Scraper with Streak Tracking")
    
    scraper = IntegratedFacebookScraper(args.sheet_name, args.worksheet_name, args.credentials)
    results = scraper.process_sheet_integrated()
    
    # Print final results
    print("\n" + "="*60)
    print("📈 INTEGRATED PROCESSING COMPLETE")
    print("="*60)
    print(f"📋 Total rows processed: {results.total_processed}")
    print(f"✅ Successful ad extractions: {results.successful_ads}")
    print(f"❌ Failed ad extractions: {results.failed_ads}")
    print(f"📊 Streak updates: {results.streak_updated}")
    print(f"🗑️  Rows deleted (30+ day streaks): {results.streak_deleted}")
    print(f"⚠️  Streak processing errors: {results.streak_errors}")
    
    if results.deleted_ips:
        print(f"\n🗑️ Deleted IPs ({len(results.deleted_ips)}):")
        for ip in results.deleted_ips[:10]:
            print(f"  - {ip}")
        if len(results.deleted_ips) > 10:
            print(f"  ... and {len(results.deleted_ips) - 10} more")
    
    print("\n✅ Integrated scraper completed!")
    logger.info("Integrated Facebook Ad Scraper completed")

if __name__ == "__main__":
    main()
