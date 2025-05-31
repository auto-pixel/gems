import os
import sys
import time
import logging
import json
import os
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
from google.auth.exceptions import GoogleAuthError
import anthropic
from dotenv import load_dotenv

# Check if we're running in Cloud Shell
IS_CLOUD_SHELL = os.environ.get('CLOUD_SHELL') == 'true' or '/google/devshell/' in os.getcwd()

# ================== CONFIGURATION ==================
SHEET_NAME = "Master Auto Swipe - Test ankur"
WORKSHEET_NAME = "Transcript"
CREDENTIALS_PATH = "credentials.json"
BATCH_SIZE = float('inf')  # Process all transcripts

# Try to get API key from environment variable
CLAUDE_API_KEY = os.environ.get("CLAUDE_API_KEY")

# If not found in environment, check for a .env file
if not CLAUDE_API_KEY and os.path.exists(".env"):
    load_dotenv()
    CLAUDE_API_KEY = os.environ.get("CLAUDE_API_KEY")

# If still not found, you can set it directly here for testing
# CLAUDE_API_KEY = "your-api-key-here"  # Uncomment and replace with your key

# Marketing elements to extract for each transcript - only using specified columns
MARKETING_ELEMENTS = [
    # Marketing Analysis
    "Angle",
    "Hook",
    "Storyline/Body",
    "Characters used in script",
    "Offer",
    "Audience Targeted",
    "Fomo or emotions evoked if any",
    "CTA",
    "Why this video works",
    "Tips to improve the hook further",
    "List 5 more characters/persons",
    # Heygen Video Production Guidance
    "Avatar_Type",
    "Voice_Tone",
    "Background_Setting",
    "Script_Variations",
    "Animation_Style",
    "Key_Visuals",
    "Duration_Recommendation",
    "Thumbnail_Description",
    "claude Last Update Time"
]

# Handle trailing spaces in column names
COLUMN_MAPPINGS = {
    "Transcript": "Transcript",  # Update this if it has trailing spaces in your sheet
    # Add other column mappings if needed (with exact spacing as in sheet)
}

# ============ ENVIRONMENT SETUP =================
load_dotenv()  # Load environment variables from .env file

# ============== LOGGING SETUP =====================
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"transcript_analyzer_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

def log(msg, level="info"):
    getattr(logging, level)(msg)

# ============ GOOGLE SHEETS SETUP =================
def get_gsheet_client():
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=scope)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        log(f"Google Sheets authentication failed: {e}", "error")
        sys.exit(1)

def get_sheet(client, worksheet_name):
    try:
        sheet = client.open(SHEET_NAME).worksheet(worksheet_name)
        return sheet
    except Exception as e:
        log(f"Failed to open worksheet '{worksheet_name}': {e}", "error")
        sys.exit(1)

def ensure_marketing_element_headers(sheet):
    """Ensure all marketing element columns exist in the sheet"""
    headers = sheet.row_values(1)
    
    # Find Transcript column index - handle potential trailing spaces
    transcript_idx = None
    transcript_col_name = COLUMN_MAPPINGS["Transcript"]
    
    # Look for exact match first (including any trailing spaces)
    try:
        transcript_idx = headers.index(transcript_col_name)
    except ValueError:
        # Then try a case-insensitive match that ignores trailing spaces
        for idx, header in enumerate(headers):
            if header.strip().lower() == transcript_col_name.strip().lower():
                transcript_idx = idx
                # Update our mapping to use the exact column name from sheet
                COLUMN_MAPPINGS["Transcript"] = header
                log(f"Found Transcript column with actual name: '{header}'")
                break
    
    if transcript_idx is None:
        log("Cannot find 'Transcript' column in the sheet. Please check column name.", "error")
        sys.exit(1)
        
    # Check which marketing element columns need to be added
    new_headers = []
    for element in MARKETING_ELEMENTS:
        if element not in headers:
            new_headers.append(element)
    
    if new_headers:
        # Get the current sheet dimensions
        col_count = len(headers)
        
        # Create the new header row with added marketing elements
        for element in new_headers:
            col_count += 1
            # Convert column number to letter (1->A, 2->B, etc.)
            col_letter = gspread.utils.rowcol_to_a1(1, col_count)[:-1]
            sheet.update(f"{col_letter}1", element)
            log(f"Added header column '{element}'")
            
        log(f"Added {len(new_headers)} marketing element columns to the sheet")
    else:
        log("All required marketing element columns already exist")
    
    # Re-fetch headers to get updated indices
    headers = sheet.row_values(1)
    
    # Create a mapping of marketing element to column index
    element_indices = {}
    for element in MARKETING_ELEMENTS:
        try:
            element_indices[element] = headers.index(element) + 1  # 1-indexed for Google Sheets API
        except ValueError:
            log(f"Cannot find column '{element}' after adding it", "error")
            sys.exit(1)
    
    return transcript_idx + 1, element_indices  # 1-indexed for Google Sheets API

# ============ CLAUDE API ANALYSIS =================
def init_claude_client():
    """Initialize Claude AI client"""
    global CLAUDE_API_KEY
    
    if not CLAUDE_API_KEY:
        # Ask for API key input if not found
        print("\nCLAUDE_API_KEY not found in environment variables or .env file.")
        print("You can provide it now or add it to environment variables/create a .env file for future runs.")
        api_key = input("Enter your Claude API key: ").strip()
        
        if api_key:
            CLAUDE_API_KEY = api_key
            # Save to .env file for future runs if user agrees
            save_to_env = input("Would you like to save this API key to a .env file for future runs? (y/n): ").lower()
            if save_to_env == 'y':
                with open(".env", "w") as f:
                    f.write(f"CLAUDE_API_KEY={api_key}\n")
                print("API key saved to .env file.")
        else:
            log("No Claude API key provided", "error")
            sys.exit(1)
    
    try:
        client = anthropic.Anthropic(api_key=CLAUDE_API_KEY)
        return client
    except Exception as e:
        log(f"Failed to initialize Claude AI client: {e}", "error")
        sys.exit(1)

def analyze_transcript_with_claude(client, transcript):
    """Analyze transcript using Claude AI API"""
    if not transcript or transcript.strip() == "":
        return None
    
    prompt = f"""
Analyze this ad transcript and provide concise, specific insights for each category:

Transcript: {transcript}

Return a JSON object with these keys:
- Angle
- Hook
- Storyline/Body
- Characters used in script
- Offer
- Audience Targeted
- Fomo or emotions evoked if any
- CTA
- Why this video works
- Tips to improve the hook further
- List 5 more characters/persons
- Avatar_Type
- Voice_Tone
- Background_Setting
- Script_Variations
- Animation_Style
- Key_Visuals
- Duration_Recommendation
- Thumbnail_Description

For each element, be specific and actionable. Make reasonable inferences when needed.
"""

    try:
        response = client.messages.create(
            model="claude-3-7-sonnet-20250219",
            max_tokens=4000,
            temperature=0.1,
            system="You are an expert marketing analyst and video production consultant.Analyze ad transcripts and return ONLY valid, properly formatted JSON with no additional text, commentary, or markdown.Use these exact keys: Angle, Hook, Storyline/Body, Characters used in script, Offer, Audience Targeted, Fomo or emotions evoked if any, CTA, Why this video works, Tips to improve the hook further, List 5 more characters/persons, Avatar_Type, Voice_Tone, Background_Setting, Script_Variations, Animation_Style, Key_Visuals, Duration_Recommendation, Thumbnail_Description.Make insights specific, actionable, and tailored. Ensure the output is strictly JSON that can be parsed programmatically.",
            messages=[
                {"role": "user", "content": prompt}
            ]
        )
        
        # Extract and parse JSON from Claude's response
        response_text = response.content[0].text
        # Find JSON content using common patterns
        json_start = response_text.find('{')
        json_end = response_text.rfind('}') + 1
        
        if json_start == -1 or json_end == 0:
            log(f"Could not find JSON in Claude's response: {response_text[:100]}...", "error")
            return None
            
        json_content = response_text[json_start:json_end]
        
        try:
            result = json.loads(json_content)
            return result
        except json.JSONDecodeError as e:
            log(f"Failed to parse Claude's JSON response: {e}\nResponse: {json_content[:200]}...", "error")
            return None
            
    except Exception as e:
        log(f"Claude API error: {e}", "error")
        return None

# ============ PROGRESS TRACKING =============
def update_progress_percentage(current, total):
    """Print a progress bar for the Claude AI script execution."""
    if total > 0:
        progress_percentage = (current / total) * 100
        progress_bar = f"[{'=' * int(progress_percentage / 2)}{' ' * (50 - int(progress_percentage / 2))}] {progress_percentage:.1f}%"
        
        # Check if running from master script to use the appropriate format
        running_from_master = os.environ.get('RUNNING_FROM_MASTER_SCRIPT') == 'true'
        
        if running_from_master:
            # When running from master script, use GitHub Actions group format
            print(f"\n::group::PROGRESS UPDATE [Claude_ai]\n{progress_bar}\nProcessed: {current}/{total} transcripts\n::endgroup::")
        else:
            # When running standalone, use a simpler format that's still clear
            print(f"\nPROGRESS [Claude_ai]: {progress_percentage:.1f}% ({current}/{total} transcripts)\n{progress_bar}")
            
        # This ensures the progress is visible in GitHub Actions logs
        sys.stdout.flush()

# ============ MAIN LOGIC ======================
def main():
    log("==== TRANSCRIPT ANALYZER START ====")
    
    # Initialize Google Sheets client
    client = get_gsheet_client()
    sheet = get_sheet(client, WORKSHEET_NAME)
    
    # Initialize Claude client (will prompt for API key if needed)
    claude_client = init_claude_client()
    
    # Ensure marketing element columns exist
    transcript_col_idx, element_indices = ensure_marketing_element_headers(sheet)
    
    # Get all data from the sheet
    all_data = sheet.get_all_records()
    log(f"Loaded {len(all_data)} rows from '{WORKSHEET_NAME}' worksheet")
    
    # Initialize progress tracking
    processed_count = 0
    total_count = len(all_data)
    update_progress_percentage(processed_count, total_count)
    
    # Process each row with a transcript
    processed = 0
    failed = 0
    for idx, row in enumerate(all_data, start=2):  # Start from row 2 (after header)
        # Get transcript using the exact column name including any trailing spaces
        transcript = row.get(COLUMN_MAPPINGS["Transcript"], "")
        
        # Skip reasons tracking
        skip_reasons = []
        
        # Check if transcript is empty
        if not transcript.strip():
            skip_reasons.append("Empty transcript")
        
        # Check if any of the specified columns are already filled
        # Only check the columns we're using
        any_elements_filled = any(
            row.get(element, '').strip() != ''
            for element in MARKETING_ELEMENTS
            if element != "claude Last Update Time"  # Exclude the timestamp column
        )
        
        if any_elements_filled:
            skip_reasons.append("Some marketing elements already analyzed")
        
        # If there are skip reasons, log and continue
        if skip_reasons:
            log(f"Skipping row {idx}: {', '.join(skip_reasons)}")
            continue
            
        log(f"Analyzing transcript in row {idx}")
        
        # Call Claude API to analyze transcript
        analysis = analyze_transcript_with_claude(claude_client, transcript)
        if not analysis:
            log(f"Failed to analyze transcript in row {idx}", "error")
            failed += 1
            continue
            
        # Update the sheet with the analysis
        try:
            for element, value in analysis.items():
                if element in element_indices:
                    col_idx = element_indices[element]
                    sheet.update_cell(idx, col_idx, value)
                    time.sleep(0.2)  # Avoid rate limiting
            
            # Add timestamp for when Claude last updated this row
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if "claude Last Update Time" in element_indices:
                timestamp_col_idx = element_indices["claude Last Update Time"]
                sheet.update_cell(idx, timestamp_col_idx, now)
                time.sleep(0.2)  # Avoid rate limiting
            
            log(f"Updated row {idx} with analysis")
            processed += 1
            processed_count += 1
            
            # Update progress
            update_progress_percentage(processed_count, total_count)
            
            # Add delay to avoid rate limiting
            time.sleep(1)
        except Exception as e:
            log(f"Failed to update row {idx}: {e}", "error")
            failed += 1
            
    log(f"All done! Successfully processed: {processed}, Failed: {failed}, Total: {len(all_data)}")
    log("==== TRANSCRIPT ANALYZER END ====")

if __name__ == "__main__":
    main()

