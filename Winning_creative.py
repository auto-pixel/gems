#!/usr/bin/env python3
# winning_creative.py

import os
import gspread
import re
from google.oauth2.service_account import Credentials
from datetime import datetime

def custom_print(message, level=None):
    """Print messages with timestamp and optional level indicator"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    level_prefix = f"[{level.upper()}] " if level else ""
    print(f"[{timestamp}] {level_prefix}{message}")

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
        
        return worksheet, spreadsheet
    except FileNotFoundError:
        custom_print(f"Error: Credentials file '{credentials_path}' not found.", "error")
        return None, None
    except Exception as e:
        custom_print(f"Error connecting to Google Sheets: {e}", "error")
        return None, None

def update_winning_creatives():
    """Update Winning Creative Image sheet with ads that have ads_count > 10 and media_type = 'image'"""
    custom_print("Starting update of Winning Creative Image sheet...")
    
    # Connect to Google Sheets
    sheet_name = "Master Auto Swipe - Test ankur"
    ads_worksheet, spreadsheet = setup_google_sheets(sheet_name, "Ads Details")
    
    if not ads_worksheet or not spreadsheet:
        custom_print("Failed to connect to Ads Details worksheet. Exiting.", "error")
        return
    
    # Check if Winning Creative Image worksheet exists
    winning_worksheet = None
    worksheet_exists = False
    
    for worksheet in spreadsheet.worksheets():
        if worksheet.title == "Winning Creative Image":
            worksheet_exists = True
            break
    
    if worksheet_exists:
        custom_print("Winning Creative Image worksheet already exists, using existing worksheet")
        try:
            winning_worksheet = spreadsheet.worksheet("Winning Creative Image")
        except Exception as e:
            custom_print(f"Error accessing existing worksheet: {e}", "error")
            return
    else:
        # Create new worksheet if it doesn't exist
        try:
            winning_worksheet = spreadsheet.add_worksheet(title="Winning Creative Image", rows=1000, cols=20)
            custom_print("Created new Winning Creative Image worksheet")
        except Exception as e:
            custom_print(f"Failed to create Winning Creative Image worksheet: {e}", "error")
            return
    
    # Get headers from Ads Details sheet
    ads_headers = ads_worksheet.row_values(1)
    
    # Find required column indices
    ads_count_idx = None
    media_type_idx = None
    
    for idx, header in enumerate(ads_headers):
        header_lower = header.lower().strip()
        if header_lower == "ads_count" or header_lower == "no. of ads":
            ads_count_idx = idx
        elif header_lower == "media_type" or header_lower == "type":
            media_type_idx = idx
    
    if ads_count_idx is None:
        custom_print("Could not find 'ads_count' column in Ads Details sheet. Exiting.", "error")
        return
        
    if media_type_idx is None:
        custom_print("Could not find 'media_type' column in Ads Details sheet. Exiting.", "error")
        return
    
    # Get all data from Ads Details sheet
    all_ads_data = ads_worksheet.get_all_values()
    headers = all_ads_data[0]
    rows = all_ads_data[1:]
    
    # Check if Winning Creative Image sheet has the same headers
    try:
        winning_headers = winning_worksheet.row_values(1)
        if not winning_headers:
            # If sheet is empty, add headers
            winning_worksheet.update('A1', [headers])
            custom_print("Added headers to Winning Creative Image sheet")
        elif winning_headers != headers:
            # Update headers if they're different
            winning_worksheet.update('A1', [headers])
            custom_print("Updated headers in Winning Creative Image sheet")
    except Exception as e:
        custom_print(f"Error checking/updating headers: {e}", "error")
        # Add headers as a fallback
        winning_worksheet.update('A1', [headers])
    
    # Get existing data from Winning Creative Image sheet
    winning_data = winning_worksheet.get_all_values()[1:] if winning_worksheet.get_all_values() else []
    
    # Find column indices for unique identifiers
    page_idx = None
    for idx, header in enumerate(headers):
        if header == "Page" or header == "Name of page":
            page_idx = idx
            break
            
    lib_id_idx = None
    for idx, header in enumerate(headers):
        if header == "library_id":
            lib_id_idx = idx
            break
            
    media_url_idx = None
    for idx, header in enumerate(headers):
        if header == "media_url":
            media_url_idx = idx
            break
    
    # Create set of existing identifiers to check for duplicates
    existing_records = set()
    skipped_count = 0
    
    for row in winning_data:
        # Build unique identifiers based on available columns
        identifiers = []
        
        # Use library_id if available (best unique identifier)
        if lib_id_idx is not None and len(row) > lib_id_idx and row[lib_id_idx]:
            identifiers.append(row[lib_id_idx])
            
        # Use page + media_url if both available
        if page_idx is not None and media_url_idx is not None and len(row) > max(page_idx, media_url_idx):
            if row[page_idx] and row[media_url_idx]:
                identifiers.append(f"{row[page_idx]}_{row[media_url_idx]}")
        
        # Use just page name if that's all we have
        elif page_idx is not None and len(row) > page_idx and row[page_idx]:
            identifiers.append(row[page_idx])
            
        # Add all identifiers to our set
        for identifier in identifiers:
            existing_records.add(identifier)
    
    # Filter rows with ads_count > 10, media_type = "image", and not already in Winning Creative Image sheet
    winning_rows = []
    update_count = 0
    duplicates_count = 0
    
    for row in rows:
        if len(row) <= max(ads_count_idx, media_type_idx):
            continue
        
        try:
            # Check media_type first
            media_type = row[media_type_idx].strip().lower()
            if media_type != "image":
                continue
                
            # Check ads_count
            ads_count_str = row[ads_count_idx].strip()
            if not ads_count_str:
                continue
                
            # Extract only digits if there's text
            digits_only = re.sub(r'[^0-9]', '', str(ads_count_str))
            if not digits_only:
                continue
                
            ads_count = int(digits_only)
            
            # Check if ads_count > 10
            if ads_count > 5:
                # Build identifiers for this row
                identifiers = []
                
                # Use library_id if available
                if lib_id_idx is not None and len(row) > lib_id_idx and row[lib_id_idx]:
                    identifiers.append(row[lib_id_idx])
                    
                # Use page + media_url if both available
                if page_idx is not None and media_url_idx is not None and len(row) > max(page_idx, media_url_idx):
                    if row[page_idx] and row[media_url_idx]:
                        identifiers.append(f"{row[page_idx]}_{row[media_url_idx]}")
                
                # Use just page name if that's all we have
                elif page_idx is not None and len(row) > page_idx and row[page_idx]:
                    identifiers.append(row[page_idx])
                
                # Check if any identifier exists already
                is_duplicate = False
                for identifier in identifiers:
                    if identifier in existing_records:
                        is_duplicate = True
                        break
                        
                if is_duplicate:
                    duplicates_count += 1
                    continue  # Skip this row as it's a duplicate
                
                # This is a new row, add it
                winning_rows.append(row)
                update_count += 1
                
                # Add all identifiers to existing records
                for identifier in identifiers:
                    existing_records.add(identifier)
                    
        except Exception as e:
            custom_print(f"Error processing row: {e}", "error")
            continue
    
    # Update Winning Creative Image sheet with new rows
    if winning_rows:
        # Find the first empty row in the winning worksheet
        if winning_data:
            start_row = len(winning_data) + 2  # +1 for header row, +1 for 1-indexing
        else:
            start_row = 2  # Start at row 2 (after header)
        
        # Update the sheet with new rows
        cell_range = f'A{start_row}:{chr(65 + len(headers) - 1)}{start_row + len(winning_rows) - 1}'
        winning_worksheet.update(cell_range, winning_rows)
        
        custom_print(f"Successfully added {update_count} new image ads with ads_count > 10 to Winning Creative Image sheet")
        custom_print(f"Skipped {duplicates_count} duplicate entries that were already in the sheet")
    else:
        custom_print("No new qualifying image ads with ads_count > 10 found to add to Winning Creative Image sheet")
        if duplicates_count > 0:
            custom_print(f"Skipped {duplicates_count} duplicate entries that were already in the sheet")

if __name__ == "__main__":  
    update_winning_creatives()
