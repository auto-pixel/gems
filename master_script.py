import os
import sys
import re
import time
import logging
import subprocess
import platform
import os
from datetime import datetime

# Set up logging - only for errors
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
# Single error log file that gets appended to
log_file = os.path.join(log_dir, "fb_scraper_errors.log")

# Configure logging to only capture errors and higher severity
logging.basicConfig(
    level=logging.ERROR,  # Only log ERROR and higher severity
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),  # Write to file
        logging.StreamHandler()  # Also output to console
    ]
)

logger = logging.getLogger(__name__)

# Dictionary to store the most recent progress update from each script
script_progress = {}

def print_all_progress():
    """Print all progress information in a clean format"""
    # Clear the screen first (Windows)
    os.system('cls')
    
    # Print the header
    print("===== FACEBOOK AD SCRAPER PROGRESS =====\n")
    
    # Print master script progress if available
    if 'master' in script_progress:
        print(script_progress['master'])
    
    # Print progress for all other scripts
    for script_name, progress in script_progress.items():
        if script_name != 'master':
            print(progress)
    
    sys.stdout.flush()

def print_master_progress(current, total, script_name):
    """Update progress bar for the master script execution."""
    progress_percentage = (current / total) * 100
    # Use simple ASCII characters for progress bar to avoid encoding issues
    progress_bar = f"[{'#' * int(progress_percentage / 2)}{'-' * (50 - int(progress_percentage / 2))}] {progress_percentage:.1f}%"
    
    # Store the progress message
    script_progress['master'] = f"MASTER SCRIPT: {progress_bar} (Running script {current}/{total}: {script_name})"
    
    # Print all progress information
    print_all_progress()

def run_script(script_name, description):
    """Run a Python script and capture its output and errors."""
    logger.info(f"=== STARTING {description} ===")
    
    start_time = time.time()
    
    try:
        # Detect environment
        is_github_actions = os.environ.get('GITHUB_ACTIONS') == 'true'
        
        # Log environment information
        logger.info(f"Running in {'GitHub Actions' if is_github_actions else 'Local'} environment")
        logger.info(f"Python version: {platform.python_version()}")
        logger.info(f"System platform: {platform.platform()}")
        
        # Set headless mode for Firefox in CI environments
        if is_github_actions:
            os.environ["MOZ_HEADLESS"] = "1"
            
            # Create Firefox profiles directory in appropriate location
            temp_dir = os.path.join(os.getcwd(), 'tmp')
            os.makedirs(temp_dir, exist_ok=True)
            firefox_profiles_dir = os.path.join(temp_dir, 'firefox_profiles')
            os.makedirs(firefox_profiles_dir, exist_ok=True)
        
        # For specific scripts, import and run directly
        if script_name == "facebook-ad-scraper-updated.py":
            # Import and run the Facebook Ad Scraper directly
            try:
                # Create a backup of sys.argv to restore later
                original_argv = sys.argv.copy()
                
                # Set up arguments for the Facebook Ad Scraper
                sys.argv = [script_name]
                
                # Run the Facebook Ad Scraper's main function directly
                import importlib.util
                spec = importlib.util.spec_from_file_location("fb_ad_scraper", script_name)
                fb_module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(fb_module)
                fb_module.main()
                
                # Restore original sys.argv
                sys.argv = original_argv
                
                logger.info(f"{description} completed successfully")
                return True
            except Exception as e:
                logger.error(f"Error running {description} directly: {str(e)}")
                return False
        else:
            # For other scripts, use the normal subprocess approach for isolation
            logger.info(f"Running {script_name} using subprocess")
            
            # Prepare environment variables for the subprocess
            env = os.environ.copy()
            
            # Set a flag to indicate this is running from the master script
            # This allows the child scripts to detect they're being run from master_script
            env['RUNNING_FROM_MASTER_SCRIPT'] = 'true'
            
            # Run as subprocess
            process = subprocess.Popen(
                [sys.executable, script_name],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                bufsize=1,
                encoding='utf-8', # Explicitly set encoding to UTF-8
                errors='replace', # Replace invalid characters rather than crashing
                env=env
            )
            
            # Process stdout and stderr directly without waiting for completion
            import threading
            
            def log_output(stream, prefix):
                try:
                    for line in iter(stream.readline, ''):
                        if not line:
                            break
                        # Handle progress updates from child scripts
                        if line.strip().startswith('PROGRESS ['):
                            # Extract script name from progress line
                            script_match = re.search(r'\[([^\]]+)\]', line)
                            if script_match:
                                script_key = script_match.group(1)
                                # Store the progress line for this script
                                script_progress[script_key] = line.strip()
                                # Update the progress display
                                print_all_progress()
                        else:
                            logger.info(f"{prefix}: {line.strip()}")
                except Exception as e:
                    logger.error(f"Error processing output stream from {prefix}: {str(e)}")
                    # Try to continue despite error
            
            # This ensures the progress is visible in GitHub Actions logs
            stdout_thread = threading.Thread(target=log_output, args=(process.stdout, script_name))
            stderr_thread = threading.Thread(target=log_output, args=(process.stderr, f"{script_name} stderr"))
            stdout_thread.daemon = True
            stderr_thread.daemon = True
            stdout_thread.start()
            stderr_thread.start()
            
            # Allow a moment for the progress display to initialize
            time.sleep(1)
            
            # Wait for completion
            return_code = process.wait()
            
            # Give threads time to finish processing output
            stdout_thread.join(1)
            stderr_thread.join(1)
            
            # Check return code
            if return_code != 0:
                logger.error(f"{description} failed with return code {return_code}")
                return False
            
            elapsed_time = time.time() - start_time
            logger.info(f"{description} completed successfully in {elapsed_time:.2f} seconds")
            return True
    
    except Exception as e:
        logger.error(f"Error running {description}: {str(e)}")
        return False

def main():
    """Run all scripts in sequence."""
    logger.info("====== MASTER SCRIPT STARTED ======")
    
    # Define scripts to run in order
    scripts = [
        {
            "name": "Ad_details_scraper.py", 
            "description": "Ad Details Scraper"
        }
    ]
    
    # Run each script in sequence
    all_success = True
    for i, script in enumerate(scripts, 1):
        logger.info(f"Running script {i}/{len(scripts)}: {script['name']}")
        
        # Show progress in GitHub Actions console
        print_master_progress(i, len(scripts), script['name'])
        
        # Add a delay between scripts
        if i > 1:
            delay = 5
            logger.info(f"Waiting {delay} seconds before starting next script...")
            time.sleep(delay)
        
        # Run the script
        success = run_script(script["name"], script["description"])
        
        if not success:
            logger.error(f"Script {script['name']} failed. Continuing with next script.")
            all_success = False
    
    if all_success:
        logger.info("====== ALL SCRIPTS COMPLETED SUCCESSFULLY ======")
    else:
        logger.warning("====== MASTER SCRIPT COMPLETED WITH ERRORS ======")
        logger.warning("Check individual logs for details on failures.")
    
    return all_success

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("Master script interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unhandled exception in master script: {e}")
        sys.exit(1)
