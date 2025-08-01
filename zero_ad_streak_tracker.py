"""
Optimized Zero-Ad Streak Tracker for Google Sheets - Fast Processing Version

This module provides fast functionality to track consecutive days with zero ads
and delete rows after 30 days. Optimized for speed with minimal API calls.

Key Features:
- Uses only essential columns: no.of ads By Ai, IP Address, Zero Ads Streak  
- Batch operations for maximum speed
- Minimal API calls with bulk updates
- Fast row deletion for 30+ day streaks
- No color processing for maximum performance
"""

import gspread
import logging
import time
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass, field

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class ProcessingResults:
    """Data class to store processing results"""
    processed: int = 0
    updated: int = 0
    deleted: int = 0
    errors: int = 0
    skipped: int = 0
    deleted_ips: List[str] = field(default_factory=list)

class FastZeroAdStreakTracker:
    """
    Optimized tracker for zero-ad streaks with focus on speed and essential operations only.
    """
    
    def __init__(self, spreadsheet_name: str = "Master Auto Swipe - Test ankur", 
                 credentials_path: str = 'credentials.json', dry_run: bool = False):
        """Initialize the Fast Zero-Ad Streak Tracker"""
        self.spreadsheet_name = spreadsheet_name
        self.dry_run = dry_run
        self.gc = None
        self.spreadsheet = None
        self.milk_sheet = None
        
        # Initialize connection
        self._initialize_connection(credentials_path)
        
        logger.info(f"Initialized fast tracker (Dry run: {dry_run})")
    
    def _initialize_connection(self, credentials_path: str) -> bool:
        """Initialize Google Sheets connection"""
        try:
            self.gc = gspread.service_account(filename=credentials_path)
            self.spreadsheet = self.gc.open(self.spreadsheet_name)
            self.milk_sheet = self.spreadsheet.worksheet("Milk")
            logger.info("Connected to Google Sheets")
            return True
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            return False
    
    def find_columns(self, headers: List[str]) -> Tuple[Optional[int], Optional[int], Optional[int]]:
        """Find the three essential columns quickly"""
        ads_col = ip_col = streak_col = None
        
        for i, header in enumerate(headers):
            if not header:
                continue
                
            header_lower = header.lower().strip()
            
            # Find ads column
            if 'ads' in header_lower and 'ai' in header_lower:
                ads_col = i + 1
            
            # Find IP column  
            elif 'ip' in header_lower and 'address' in header_lower:
                ip_col = i + 1
            
            # Find streak column
            elif 'zero' in header_lower and 'ads' in header_lower and 'streak' in header_lower:
                streak_col = i + 1
        
        return ads_col, ip_col, streak_col
    
    def process_streaks_fast(self) -> ProcessingResults:
        """
        Fast processing of zero-ad streaks with bulk operations
        """
        results = ProcessingResults()
        
        try:
            # Get all data in one API call
            logger.info("Fetching sheet data...")
            all_values = self.milk_sheet.get_all_values()
            
            if not all_values:
                logger.error("No data found")
                return results
            
            # Find columns
            headers = all_values[0]
            ads_col, ip_col, streak_col = self.find_columns(headers)
            
            if not all([ads_col, ip_col, streak_col]):
                logger.error(f"Required columns not found - Ads: {ads_col}, IP: {ip_col}, Streak: {streak_col}")
                return results
            
            logger.info(f"Using columns - Ads: {ads_col}, IP: {ip_col}, Streak: {streak_col}")
            
            # Process rows and collect updates
            updates = []  # For batch updates
            rows_to_delete = []  # For deletion
            
            for row_idx in range(1, len(all_values)):
                row_num = row_idx + 1
                row_data = all_values[row_idx]
                
                # Safety check for row length
                if len(row_data) < max(ads_col, ip_col, streak_col):
                    results.skipped += 1
                    continue
                
                # Get values
                ip_address = row_data[ip_col - 1].strip() if row_data[ip_col - 1] else ""
                ads_value = row_data[ads_col - 1].strip() if row_data[ads_col - 1] else ""
                streak_value = row_data[streak_col - 1].strip() if row_data[streak_col - 1] else "0"
                
                # Skip invalid rows
                if not ip_address:
                    results.skipped += 1
                    continue
                
                # Validate ads value
                try:
                    if not ads_value or ads_value.lower() in ['', 'none', 'null', 'n/a']:
                        results.skipped += 1
                        continue
                    ads_count = int(float(ads_value))
                except (ValueError, TypeError):
                    results.skipped += 1
                    continue
                
                # Get current streak
                try:
                    current_streak = int(float(streak_value)) if streak_value else 0
                except (ValueError, TypeError):
                    current_streak = 0
                
                # Process based on ads count
                if ads_count == 0:
                    # Zero ads - increment streak
                    new_streak = current_streak + 1
                    
                    if new_streak >= 30:
                        # Mark for deletion
                        rows_to_delete.append((row_num, ip_address))
                        results.deleted_ips.append(ip_address)
                        logger.info(f"Row {row_num} (IP: {ip_address}): 30+ day streak - marking for deletion")
                    else:
                        # Add to batch update
                        updates.append({
                            'range': f'{chr(64 + streak_col)}{row_num}',
                            'values': [[str(new_streak)]]
                        })
                        logger.debug(f"Row {row_num} (IP: {ip_address}): Streak {current_streak} → {new_streak}")
                        results.updated += 1
                
                elif ads_count > 0 and current_streak > 0:
                    # Reset streak to 0 when ad count is greater than 0
                    updates.append({
                        'range': f'{chr(64 + streak_col)}{row_num}',
                        'values': [['0']]
                    })
                    logger.info(f"Row {row_num} (IP: {ip_address}): Reset streak to 0 (had {ads_count} ads after {current_streak} day streak)")
                    results.updated += 1
                
                results.processed += 1
            
            # Perform batch updates
            if updates and not self.dry_run:
                logger.info(f"Performing batch update for {len(updates)} cells...")
                try:
                    self.milk_sheet.batch_update(updates)
                    logger.info("Batch update completed successfully")
                except Exception as e:
                    logger.error(f"Batch update failed: {e}")
                    results.errors += len(updates)
            elif updates and self.dry_run:
                logger.info(f"Dry run: Would batch update {len(updates)} cells")
            
            # Delete rows (in reverse order to avoid index shifts)
            if rows_to_delete:
                logger.info(f"Deleting {len(rows_to_delete)} rows with 30+ day streaks...")
                
                # Sort in descending order by row number
                rows_to_delete.sort(key=lambda x: x[0], reverse=True)
                
                for row_num, ip_address in rows_to_delete:
                    try:
                        if not self.dry_run:
                            self.milk_sheet.delete_rows(row_num)
                            logger.info(f"Deleted row {row_num} (IP: {ip_address})")
                        else:
                            logger.info(f"Dry run: Would delete row {row_num} (IP: {ip_address})")
                        
                        results.deleted += 1
                        time.sleep(0.5)  # Small delay between deletions
                        
                    except Exception as e:
                        logger.error(f"Failed to delete row {row_num} (IP: {ip_address}): {e}")
                        results.errors += 1
            
            logger.info("Fast processing completed")
            return results
            
        except Exception as e:
            logger.error(f"Error in fast processing: {e}")
            results.errors += 1
            return results

def main():
    """Main function optimized for speed"""
    import argparse
    import os
    import sys
    
    parser = argparse.ArgumentParser(description='Fast Zero-Ad Streak Tracker')
    parser.add_argument('--spreadsheet', '-s', 
                       default="Master Auto Swipe - Test ankur",
                       help='Google Sheets spreadsheet name')
    parser.add_argument('--credentials', '-c', 
                       default='credentials.json',
                       help='Path to credentials.json file')
    parser.add_argument('--dry-run', '-d', 
                       action='store_true',
                       help='Show what would be done without making changes')
    parser.add_argument('--verbose', '-v', 
                       action='store_true',
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    if not os.path.exists(args.credentials):
        print(f"Error: Credentials file not found: {args.credentials}")
        return 1
    
    try:
        print(f"🚀 Fast Zero-Ad Streak Tracker")
        print(f"📊 Spreadsheet: {args.spreadsheet}")
        print(f"🔧 Dry run: {args.dry_run}")
        print("-" * 50)
        
        # Initialize tracker
        tracker = FastZeroAdStreakTracker(
            spreadsheet_name=args.spreadsheet,
            credentials_path=args.credentials,
            dry_run=args.dry_run
        )
        
        # Process streaks
        print("Processing zero-ad streaks (fast mode)...")
        start_time = time.time()
        
        results = tracker.process_streaks_fast()
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        # Print results
        print("\n" + "="*50)
        print("📈 PROCESSING COMPLETE")
        print("="*50)
        print(f"⏱️  Processing time: {processing_time:.1f} seconds")
        print(f"📋 Rows processed: {results.processed}")
        print(f"✏️  Rows updated: {results.updated}")
        print(f"⏭️  Rows skipped: {results.skipped}")
        print(f"🗑️  Rows deleted: {results.deleted}")
        print(f"❌ Errors: {results.errors}")
        
        if results.deleted_ips:
            print(f"\n🗑️ Deleted IPs ({len(results.deleted_ips)}):")
            for ip in results.deleted_ips[:10]:  # Show first 10
                print(f"  - {ip}")
            if len(results.deleted_ips) > 10:
                print(f"  ... and {len(results.deleted_ips) - 10} more")
        
        if results.errors > 0:
            print(f"\n⚠️  {results.errors} error(s) occurred")
            return 1
        else:
            print(f"\n✅ Fast processing completed successfully!")
            return 0
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        return 1

if __name__ == "__main__":
    import sys
    sys.exit(main())