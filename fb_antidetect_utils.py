"""
Advanced Anti-Detection Utilities for Facebook Ad Scraping
This module provides utilities to help avoid detection when scraping Facebook Ad Library.
"""

import random
import time
import json
import os
import sys
import requests
import undetected_chromedriver as uc
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
import logging
from fake_useragent import UserAgent

# Modern, realistic user agents
USER_AGENTS = [
    # Firefox on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    # Firefox on macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:123.0) Gecko/20100101 Firefox/123.0",
    # Firefox on Linux
    "Mozilla/5.0 (X11; Linux x86_64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0"
]

# Common screen resolutions
VIEWPORT_SIZES = [
    (1920, 1080),  # Full HD
    (1366, 768),   # Common laptop
    (1536, 864),   # Common laptop
    (1440, 900),   # MacBook
    (1680, 1050),  # Common desktop
    (2560, 1440),  # 2K monitor
    (1280, 720),   # HD
]

# Common languages and locales
LANGUAGES = [
    "en-US,en;q=0.9",
    "en-GB,en;q=0.9",
    "en-CA,en;q=0.9",
    "en-IN,en;q=0.9,hi;q=0.8",
    "en-AU,en;q=0.9",
]

# Create a global instance of ProxyManager that can be used by get_current_ip
PROXY_MANAGER = None

class ProxyManager:
    """Manages traditional proxies with simple rotation mechanism"""
    
    def __init__(self, proxy_file=None, proxies=None):
        # Initialize list of proxies
        self.proxies = []
        self.failed_proxies = set()
        self.current_index = 0
        
        # Load proxies from file if provided
        if proxy_file and os.path.exists(proxy_file):
            try:
                with open(proxy_file, 'r') as f:
                    self.proxies = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
                logging.info(f"Loaded {len(self.proxies)} proxies from {proxy_file}")
            except Exception as e:
                logging.error(f"Error loading proxies from file: {e}")
        
        # Add direct proxies if provided
        if proxies and isinstance(proxies, list):
            self.proxies.extend(proxies)
            
        # Remove duplicates
        self.proxies = list(set(self.proxies))
        
        logging.info(f"ProxyManager initialized with {len(self.proxies)} proxies")
        
        # Set this instance as the global PROXY_MANAGER
        global PROXY_MANAGER
        PROXY_MANAGER = self
    
    def get_proxy_url(self, target_url):
        """Direct access without proxy for compatibility with existing code"""
        # No proxy is used - return the original URL
        return target_url
    
    def get_next_proxy(self):
        """Get the next proxy in the rotation"""
        if not self.proxies:
            return None
            
        attempts = 0
        while attempts < len(self.proxies):
            self.current_index = (self.current_index + 1) % len(self.proxies)
            proxy = self.proxies[self.current_index]
            
            if proxy not in self.failed_proxies:
                return proxy
            
            attempts += 1
        
        # If all proxies have failed, return None
        return None
    
    def _test_proxy(self, proxy):
        """Test if a proxy is working"""
        try:
            test_url = "https://api.ipify.org"
            proxies = {
                "http": f"http://{proxy}",
                "https": f"http://{proxy}"
            }
            response = requests.get(test_url, proxies=proxies, timeout=10)
            return response.status_code == 200
        except Exception as e:
            logging.error(f"Proxy test failed: {e}")
            return False
    
    def mark_proxy_failed(self, proxy):
        """Mark a proxy as failed"""
        if proxy in self.proxies:
            self.failed_proxies.add(proxy)
    
    def reset_failed_proxies(self):
        """Reset the list of failed proxies"""
        self.failed_proxies.clear()


def get_randomized_options(browser_type="firefox"):
    """
    Creates browser options with randomized user agent and other fingerprint masking features
    Args:
        browser_type: 'firefox', 'chrome', or 'undetected_chrome'
    Returns:
        Options object for the specified browser
    """
    # Random user agent
    user_agent = random.choice(USER_AGENTS)
    logging.info(f"Using User-Agent: {user_agent}")
    
    # Detect execution environment
    is_github_actions = os.environ.get('GITHUB_ACTIONS') == 'true'
    is_ci = is_github_actions or 'CI' in os.environ
    
    if browser_type.lower() == "firefox":
        options = FirefoxOptions()
        
        # Set user agent
        options.set_preference("general.useragent.override", user_agent)
        
        # Disable WebRTC - prevents IP leakage
        options.set_preference("media.peerconnection.enabled", False)
        options.set_preference("media.navigator.enabled", False)
        
        # Disable cache
        options.set_preference("browser.cache.disk.enable", False)
        options.set_preference("browser.cache.memory.enable", False)
        options.set_preference("browser.cache.offline.enable", False)
        options.set_preference("network.http.use-cache", False)
        
        # Disable things that can reveal your identity
        options.set_preference("privacy.trackingprotection.enabled", True)
        options.set_preference("dom.battery.enabled", False)
        options.set_preference("dom.gamepad.enabled", False)
        
        # Random language setting
        language = random.choice(LANGUAGES)
        options.set_preference("intl.accept_languages", language)
        
        # Always use headless mode in CI environments
        if is_ci:
            options.add_argument('--headless')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            
        # Add more privacy and anti-detection settings
        options.set_preference("privacy.resistFingerprinting", True)
        options.set_preference("privacy.trackingprotection.fingerprinting.enabled", True)
        
    elif browser_type.lower() in ["chrome", "undetected_chrome"]:
        # Use ChromeOptions for both chrome and undetected_chrome
        options = ChromeOptions()
        
        # Set user agent
        options.add_argument(f"--user-agent={user_agent}")
        
        # Add anti-detection arguments
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-infobars")
        options.add_argument("--disable-notifications")
        options.add_argument("--disable-popup-blocking")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        
        # Disable automation flags
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)
        
        # Add CI environment specific arguments
        if is_ci or browser_type.lower() == "undetected_chrome":
            options.add_argument("--headless=new")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            
        # Add additional stealth settings
        options.add_argument("--disable-features=IsolateOrigins,site-per-process")
        options.add_argument("--disable-site-isolation-trials")
        options.add_argument("--disable-web-security")
        options.add_argument("--allow-running-insecure-content")
        
        # Random language setting
        language = random.choice(LANGUAGES)
        options.add_argument(f"--lang={language}")
    else:
        raise ValueError(f"Unsupported browser type: {browser_type}")
    
    return options

def install_requirements():
    """Install required packages for undetected-chromedriver"""
    try:
        import subprocess
        import sys
        requirements = [
            'undetected-chromedriver>=3.5.5',
            'selenium>=4.15.2',
            'webdriver-manager>=4.0.1',
            'fake-useragent>=1.4.0'
        ]
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade"] + requirements)
        logging.info("Successfully installed/updated required packages")
        return True
    except Exception as e:
        logging.error(f"Failed to install requirements: {e}")
        return False

def create_stealth_driver(use_proxy=False, proxy_manager=None, headless=True, target_url=None):
    """Creates a WebDriver with enhanced anti-detection measures using Undetected Chromedriver"""
    # Try to import required packages
    try:
        import undetected_chromedriver as uc
        from selenium import webdriver
        from fake_useragent import UserAgent
    except ImportError:
        logging.warning("Required packages not found. Attempting to install...")
        if not install_requirements():
            raise ImportError("Failed to install required packages. Please install them manually.")
        import undetected_chromedriver as uc
        from selenium import webdriver
        from fake_useragent import UserAgent
    
    def create_driver(version=None):
        """Helper function to create a new driver instance"""
        options = uc.ChromeOptions()
        
        # Set a random user agent
        user_agent = random.choice(USER_AGENTS)
        
        # Basic options
        options.add_argument(f'--user-agent={user_agent}')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--disable-infobars')
        options.add_argument('--disable-notifications')
        options.add_argument('--disable-popup-blocking')
        options.add_argument('--disable-extensions')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-software-rasterizer')
        options.add_argument('--disable-web-security')
        options.add_argument('--disable-blink-features')
        
        # Headless mode
        if headless:
            options.add_argument('--headless=new')
        
        # Random window size
        width, height = random.choice(VIEWPORT_SIZES)
        options.add_argument(f'--window-size={width},{height}')
        
        # Create driver with the specified version or latest
        driver_kwargs = {
            'options': options,
            'use_subprocess': True,
            'headless': headless
        }
        
        if version:
            driver_kwargs['version_main'] = version
        
        return uc.Chrome(**driver_kwargs)
    
    try:
        # Detect execution environment
        is_github_actions = os.environ.get('GITHUB_ACTIONS') == 'true'
        is_ci = is_github_actions or 'CI' in os.environ
        logging.info(f"Running in {'CI environment' if is_ci else 'Local Environment'}")
        
        # Set proxy if needed
        proxy = None
        if use_proxy and proxy_manager:
            proxy = proxy_manager.get_next_proxy()
            if proxy:
                if '://' in proxy:
                    proxy = proxy.split('://')[1]  # Remove protocol if present
                proxy_parts = proxy.split(':')
                if len(proxy_parts) >= 2:  # Handle both host:port and user:pass@host:port
                    logging.info(f"Using proxy: {proxy}")
                    # Add proxy to Chrome options
                    proxy_arg = f'--proxy-server={proxy}'
                    uc.ChromeOptions.add_argument(uc.ChromeOptions(), proxy_arg)
                else:
                    logging.warning(f"Invalid proxy format: {proxy}. Expected format is host:port or user:pass@host:port")
                    proxy = None
        
        # Try Chrome 137 first, then fallback to other versions if needed
        versions_to_try = [137, 114, 112, None]  # Try v137 first, then fallback to others
        
        for version in versions_to_try:
            try:
                version_str = f"v{version}" if version else "latest"
                logging.info(f"Attempting to initialize Undetected Chrome {version_str}...")
                
                # Create a fresh driver instance for each attempt
                driver = create_driver(version)
                
                # Set proxy if needed
                if proxy:
                    driver.execute_cdp_cmd('Network.setCacheDisabled', {'cacheDisabled': True})
                    driver.execute_cdp_cmd('Network.setBypassServiceWorker', {'bypass': True})
                    driver.execute_cdp_cmd('Network.enable', {})
                
                # Additional anti-detection measures
                driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
                driver.execute_script("window.navigator.chrome = {runtime: {}};")
                driver.execute_script("const originalQuery = window.navigator.permissions.query; "
                                    "return originalQuery.name === 'notifications';")
                driver.set_page_load_timeout(60)
                
                # Set a random window size to avoid detection
                if not headless:
                    width, height = random.choice(VIEWPORT_SIZES)
                    driver.set_window_size(width, height)
                
                logging.info(f"Successfully created Undetected Chrome {version_str} WebDriver")
                return driver
                
            except Exception as e:
                logging.warning(f"Failed to initialize Chrome {version if version else 'latest'}: {str(e)}")
                if version is None:  # If we've tried the latest version and still failed
                    raise RuntimeError("Failed to initialize Chrome. Please check your installation and try again.")
                continue  # Try next version
                
    except Exception as e:
        logging.error(f"Error creating stealth driver: {e}")
        # Close driver if it was created but later steps failed
        if 'driver' in locals():
            try:
                driver.quit()
            except:
                pass
        raise  # Re-raise the exception after cleanup
        raise

def perform_human_like_scroll(driver, scroll_pause_base=3.0, max_scroll_attempts=3):
    """
    Simulates human-like scrolling behavior with random pauses and scroll distances
    Returns the total number of scrolls performed
    """
    scroll_count = 0
    last_height = driver.execute_script("return document.body.scrollHeight")
    attempts_at_bottom = 0
    
    while attempts_at_bottom < max_scroll_attempts:
        # Calculate a random scroll distance (70-100% of viewport height)
        viewport_height = driver.execute_script("return window.innerHeight")
        scroll_distance = random.uniform(0.7, 1.0) * viewport_height
        
        # Smooth scrolling simulation
        current_position = driver.execute_script("return window.pageYOffset")
        target_position = min(current_position + scroll_distance, last_height)
        
        # Perform several small scrolls to simulate smooth movement
        steps = random.randint(5, 15)
        for step in range(1, steps + 1):
            next_pos = current_position + (target_position - current_position) * (step / steps)
            driver.execute_script(f"window.scrollTo(0, {next_pos})")
            time.sleep(random.uniform(0.01, 0.05))  # Tiny pauses between micro-scrolls
        
        scroll_count += 1
        
        # Random pause between scrolls (variable timing)
        scroll_pause = random.uniform(scroll_pause_base * 0.7, scroll_pause_base * 1.5)
        time.sleep(scroll_pause)
        
        # Sometimes perform a small scroll back up (reading behavior)
        if random.random() < 0.3:  # 30% chance
            small_up_scroll = random.uniform(viewport_height * 0.05, viewport_height * 0.2)
            driver.execute_script(f"window.scrollBy(0, -{small_up_scroll})")
            time.sleep(random.uniform(0.5, 1.5))  # Pause as if reading
            # And scroll back down
            driver.execute_script(f"window.scrollBy(0, {small_up_scroll})")
            time.sleep(random.uniform(0.3, 0.7))
        
        # Check if we've reached the bottom
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            attempts_at_bottom += 1
            logging.info(f"Scroll height ({new_height}) hasn't changed. Attempt {attempts_at_bottom}/{max_scroll_attempts} at bottom...")
        else:
            attempts_at_bottom = 0
            logging.info(f"Scroll height changed from {last_height} to {new_height}")
        
        last_height = new_height
        
        # Add some randomness to when we give up
        if scroll_count >= random.randint(40, 60):
            logging.info("Reached random scroll limit. Breaking out.")
            break
            
    return scroll_count

def simulate_random_mouse_movements(driver, num_movements=5):
    """Simulates random mouse movements across the page with safety checks"""
    if not driver:
        return
    
    try:
        # Try to find a visible element to use as a starting point
        try:
            # First try to find the body element, which should always exist
            body_element = driver.find_element(By.TAG_NAME, "body")
            
            # Move to this element first
            actions = ActionChains(driver)
            actions.move_to_element(body_element)
            actions.perform()
            time.sleep(0.3)  # Brief pause after initial movement
        except Exception as e:
            logging.warning(f"Could not find body element: {e}")
            # If we can't move to body, we'll skip mouse movements
            return
        
        # Now perform small random movements from current position
        for _ in range(num_movements):
            # Small random offset from current position (maximum 50px in any direction)
            # Using smaller offsets to avoid moving out of bounds
            offset_x = random.randint(-50, 50)
            offset_y = random.randint(-50, 50)
            
            # Use move_by_offset which is relative to current position
            try:
                actions = ActionChains(driver)
                actions.move_by_offset(offset_x, offset_y)
                actions.pause(random.uniform(0.1, 0.3))
                actions.perform()
                time.sleep(random.uniform(0.1, 0.5))
            except Exception as e:
                logging.warning(f"Mouse movement failed: {e}")
                break  # Stop movements if one fails
    except Exception as e:
        # If anything fails, log and continue without mouse movements
        logging.warning(f"Could not perform mouse movements: {e}")
        # Don't let mouse movement errors affect the main functionality

def add_random_delays(min_delay=0.5, max_delay=2.0):
    """Adds a random delay between actions"""
    delay = random.uniform(min_delay, max_delay)
    time.sleep(delay)
    return delay

def get_current_ip():
    """Get the current public IP address being used for requests.
    
    Returns:
        str: The current public IP address
    """
    try:
        # Use httpbin.org to check our IP address
        response = requests.get('https://httpbin.org/ip', timeout=10)
        response.raise_for_status()
        ip_address = response.json().get('origin', 'Unknown')
        
        # If we get multiple IPs (happens with some proxies), just take the first one
        if ',' in ip_address:
            ip_address = ip_address.split(',')[0].strip()
            
        logging.info(f"Current IP address: {ip_address}")
        return ip_address
    except Exception as e:
        logging.error(f"Error retrieving current IP address: {str(e)}")
        return "Unknown IP"
