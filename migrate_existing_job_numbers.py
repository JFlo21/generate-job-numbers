#!/usr/bin/env python3
"""
One-time migration script to extract existing job number mappings from all sheets
and populate the state sheet with them.
"""

import os
import json
import logging
import smartsheet
from collections import defaultdict
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(message)s'
)

# Configuration
API_TOKEN = os.environ.get('SMARTSHEET_API_TOKEN')
if not API_TOKEN:
    raise ValueError("SMARTSHEET_API_TOKEN environment variable not set")

STATE_SHEET_ID = 6534534683119492
CACHE_FILE = "sheet_discovery_cache.json"

# Rate limiter configuration
RATE_LIMIT = 300  # 300 requests per minute
MIN_INTERVAL = 60.0 / RATE_LIMIT  # minimum seconds between requests

class RateLimiter:
    """Simple rate limiter for API calls"""
    def __init__(self):
        self.last_request = 0
    
    def wait_if_needed(self):
        elapsed = time.time() - self.last_request
        if elapsed < MIN_INTERVAL:
            time.sleep(MIN_INTERVAL - elapsed)
        self.last_request = time.time()

rate_limiter = RateLimiter()

def make_api_call(func, *args, **kwargs):
    """Make an API call with rate limiting"""
    rate_limiter.wait_if_needed()
    return func(*args, **kwargs)

def load_cache():
    """Load the sheet discovery cache"""
    if not os.path.exists(CACHE_FILE):
        logging.error(f"Cache file {CACHE_FILE} not found!")
        return None
    
    with open(CACHE_FILE, 'r') as f:
        return json.load(f)

def extract_mappings_from_sheet(client, sheet_id, sheet_name, col_mapping):
    """Extract existing job number mappings from a sheet"""
    logging.info(f"  Reading {sheet_name}...")
    
    main_mappings = {}
    helper_mappings = {}
    conflicts_main = []
    conflicts_helper = []
    
    try:
        # Fetch sheet with rows
        sheet = make_api_call(client.Sheets.get_sheet, sheet_id, include='rows')
        
        # Get column IDs from cache (using lowercase keys)
        dept_col = col_mapping.get('dept')
        work_req_col = col_mapping.get('work_request')
        job_col = col_mapping.get('job')
        helper_dept_col = col_mapping.get('helper_dept')
        helper_job_col = col_mapping.get('helper_job')
        
        if not all([dept_col, work_req_col, job_col]):
            logging.warning(f"  Skipping {sheet_name} - missing required columns")
            return main_mappings, helper_mappings, conflicts_main, conflicts_helper
        
        rows_processed = 0
        main_found = 0
        helper_found = 0
        
        # Process each row
        for row in sheet.rows:
            rows_processed += 1
            
            # Extract main job mapping
            dept_val = None
            work_req_val = None
            job_val = None
            
            for cell in row.cells:
                if cell.column_id == dept_col:
                    dept_val = str(cell.value).strip() if cell.value else None
                elif cell.column_id == work_req_col:
                    work_req_val = str(cell.value).strip() if cell.value else None
                elif cell.column_id == job_col:
                    job_val = str(cell.value).strip() if cell.value else None
            
            # Save main mapping if all values present
            if dept_val and work_req_val and job_val:
                key = f"{dept_val}|{work_req_val}"
                if key in main_mappings and main_mappings[key] != job_val:
                    conflicts_main.append({
                        'sheet': sheet_name,
                        'key': key,
                        'existing': main_mappings[key],
                        'new': job_val
                    })
                else:
                    main_mappings[key] = job_val
                    main_found += 1
            
            # Extract helper mapping if columns exist
            if helper_dept_col and helper_job_col:
                helper_dept_val = None
                helper_job_val = None
                
                for cell in row.cells:
                    if cell.column_id == helper_dept_col:
                        helper_dept_val = str(cell.value).strip() if cell.value else None
                    elif cell.column_id == helper_job_col:
                        helper_job_val = str(cell.value).strip() if cell.value else None
                
                # Save helper mapping if values present
                if helper_dept_val and helper_job_val:
                    key = f"HELPER|{helper_dept_val}"
                    if key in helper_mappings and helper_mappings[key] != helper_job_val:
                        conflicts_helper.append({
                            'sheet': sheet_name,
                            'key': key,
                            'existing': helper_mappings[key],
                            'new': helper_job_val
                        })
                    else:
                        helper_mappings[key] = helper_job_val
                        helper_found += 1
        
        logging.info(f"    Processed {rows_processed} rows, found {main_found} main mappings, {helper_found} helper mappings")
        
    except Exception as e:
        logging.error(f"  Error processing {sheet_name}: {e}")
    
    return main_mappings, helper_mappings, conflicts_main, conflicts_helper

def upload_to_state_sheet(client, all_mappings):
    """Upload mappings to the state sheet"""
    logging.info(f"\nUploading {len(all_mappings)} mappings to state sheet...")
    
    # Get state sheet structure
    sheet = make_api_call(client.Sheets.get_sheet, STATE_SHEET_ID, include='columns')
    
    # Find column IDs
    key_col_id = None
    gen_num_col_id = None
    
    for col in sheet.columns:
        if col.title.lower() == 'key':
            key_col_id = col.id
        elif col.title.lower() == 'generated number':
            gen_num_col_id = col.id
    
    if not key_col_id or not gen_num_col_id:
        raise ValueError("State sheet must have 'Key' and 'Generated Number' columns")
    
    logging.info(f"  Found columns: Key={key_col_id}, Generated Number={gen_num_col_id}")
    
    # Create rows for upload
    new_rows = []
    for key, value in all_mappings.items():
        new_row = smartsheet.models.Row()
        new_row.to_top = True
        new_row.cells.append({
            'column_id': key_col_id,
            'value': key
        })
        new_row.cells.append({
            'column_id': gen_num_col_id,
            'value': value
        })
        new_rows.append(new_row)
    
    # Upload in batches of 500
    batch_size = 500
    total_uploaded = 0
    
    for i in range(0, len(new_rows), batch_size):
        batch = new_rows[i:i+batch_size]
        try:
            response = make_api_call(client.Sheets.add_rows, STATE_SHEET_ID, batch)
            total_uploaded += len(batch)
            logging.info(f"  Uploaded batch: {total_uploaded}/{len(new_rows)} rows")
        except Exception as e:
            logging.error(f"  Error uploading batch: {e}")
    
    return total_uploaded

def main():
    """Main migration function"""
    logging.info("=" * 60)
    logging.info("🔄 MIGRATING EXISTING JOB NUMBERS TO STATE SHEET")
    logging.info("=" * 60)
    
    # Initialize client
    client = smartsheet.Smartsheet(API_TOKEN)
    client.errors_as_exceptions(True)
    
    # Load cache
    cache = load_cache()
    if not cache:
        logging.error("Cannot proceed without cache file")
        return
    
    sheets_data = cache.get('sheets', {})
    logging.info(f"\nFound {len(sheets_data)} sheets in cache")
    
    # Aggregate all mappings
    all_main_mappings = {}
    all_helper_mappings = {}
    all_conflicts = []
    
    logging.info("\n📖 Step 1: Extracting mappings from all sheets...")
    
    for sheet_id, sheet_info in sheets_data.items():
        sheet_name = sheet_info['name']
        col_mapping = sheet_info.get('columns', {})
        
        # Process primary sheet first, then backups in order
        is_primary = 'Backup' not in sheet_name
        
        main, helper, conflicts_main, conflicts_helper = extract_mappings_from_sheet(
            client, int(sheet_id), sheet_name, col_mapping
        )
        
        # Merge mappings (primary sheets override backups)
        for key, value in main.items():
            if key not in all_main_mappings or is_primary:
                all_main_mappings[key] = value
        
        for key, value in helper.items():
            if key not in all_helper_mappings or is_primary:
                all_helper_mappings[key] = value
        
        all_conflicts.extend(conflicts_main)
        all_conflicts.extend(conflicts_helper)
    
    # Log summary
    logging.info("\n📊 Summary:")
    logging.info(f"  Total main mappings found: {len(all_main_mappings)}")
    logging.info(f"  Total helper mappings found: {len(all_helper_mappings)}")
    
    if all_conflicts:
        logging.warning(f"\n⚠️ Found {len(all_conflicts)} conflicts:")
        for conflict in all_conflicts[:10]:  # Show first 10
            logging.warning(f"  {conflict}")
        if len(all_conflicts) > 10:
            logging.warning(f"  ... and {len(all_conflicts) - 10} more")
    
    # Combine all mappings
    all_mappings = {}
    all_mappings.update(all_main_mappings)
    all_mappings.update(all_helper_mappings)
    
    if not all_mappings:
        logging.warning("No mappings found to upload!")
        return
    
    # Upload to state sheet
    logging.info(f"\n📤 Step 2: Uploading {len(all_mappings)} total mappings to state sheet...")
    uploaded = upload_to_state_sheet(client, all_mappings)
    
    # Final report
    logging.info("\n" + "=" * 60)
    logging.info("✅ MIGRATION COMPLETED")
    logging.info(f"  Main job mappings: {len(all_main_mappings)}")
    logging.info(f"  Helper job mappings: {len(all_helper_mappings)}")
    logging.info(f"  Total rows uploaded: {uploaded}")
    logging.info("=" * 60)

if __name__ == "__main__":
    main()