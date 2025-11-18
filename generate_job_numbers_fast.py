"""
Dynamic Job Number Generator for Smartsheet - FAST Version

Ultra-optimized version that:
1. Allows specifying exact sheet IDs to skip discovery
2. Filters sheets by name patterns before checking columns
3. Processes only relevant sheets in parallel
4. Target runtime: Under 5 minutes total
"""

import os
import smartsheet
import logging
import json
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, Semaphore
from datetime import datetime, timedelta
import sys

API_TOKEN = os.getenv("SMARTSHEET_API_TOKEN")

# Required column names for sheets to be processed
REQUIRED_COLUMNS = ["Dept #", "Work Request #", "Job #"]
OPTIONAL_HELPER_COLUMNS = ["Helper Dept #", "Helper Job [#]"]

# Performance tuning parameters
MAX_WORKERS = 6  # Number of parallel threads for sheet processing
RATE_LIMIT_REQUESTS = 300  # Smartsheet allows 300 requests per minute
RATE_LIMIT_WINDOW = 60  # seconds
BATCH_SIZE = 500  # Process updates in batches

# OPTIMIZATION: Specify exact sheet IDs to skip discovery entirely
# These are the sheets that are known to have job number columns
KNOWN_JOB_SHEET_IDS = [
    3239244454645636,  # Original sheet 1
    2230129632694148,  # Original sheet 2 
    1732945426468740,  # Original sheet 3
    4126460034895748,  # Original sheet 4
    # Add more sheet IDs here as you discover them
]

# OPTIMIZATION: Sheet name patterns to check (case-insensitive)
# Only sheets with these patterns in their names will be checked
SHEET_NAME_PATTERNS = [
    "resiliency promax database",  # Primary pattern - only process these sheets
    # Add more patterns as needed
]

# Set to True to discover new sheets, False to only use KNOWN_JOB_SHEET_IDS
ENABLE_DISCOVERY = True  # ENABLED - Discover all sheets with required columns

STATE_SHEET_ID = 6534534683119492
STATE_COLUMN_NAMES = {
    'key': 'key',
    'value': 'value'
}
STATE_DATA_KEY = "StateData"
HELPER_STATE_DATA_KEY = "HelperStateData"

# Patterns to exclude from processing
EXCLUDE_PATTERNS = ["no match", "no match - 004", "not assigned"]

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# Global rate limiter
class RateLimiter:
    def __init__(self, max_requests=RATE_LIMIT_REQUESTS, window=RATE_LIMIT_WINDOW):
        self.semaphore = Semaphore(max_requests)
        self.max_requests = max_requests
        self.window = window
        self.request_times = []
        self.lock = Lock()
        
    def acquire(self):
        """Wait if necessary to respect rate limits"""
        with self.lock:
            now = time.time()
            # Remove old requests outside the window
            self.request_times = [t for t in self.request_times if now - t < self.window]
            
            # If we're at the limit, wait
            if len(self.request_times) >= self.max_requests:
                sleep_time = self.window - (now - self.request_times[0]) + 0.1
                if sleep_time > 0:
                    logging.debug(f"Rate limit reached, sleeping for {sleep_time:.2f} seconds")
                    time.sleep(sleep_time)
                    now = time.time()
                    self.request_times = [t for t in self.request_times if now - t < self.window]
            
            self.request_times.append(now)

rate_limiter = RateLimiter()

def should_exclude_value(value):
    """Check if a value should be excluded from processing"""
    if not value:
        return False
    
    value_str = str(value).strip().lower()
    for pattern in EXCLUDE_PATTERNS:
        if pattern.lower() in value_str:
            return True
    return False

def make_api_call(func, *args, **kwargs):
    """Wrapper for API calls with rate limiting and error handling"""
    rate_limiter.acquire()
    max_retries = 3
    retry_delay = 1
    
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except smartsheet.exceptions.ApiError as e:
            if e.error.result.error_code == 4003:  # Rate limit error
                wait_time = retry_delay * (2 ** attempt)
                logging.warning(f"Rate limit hit, waiting {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                raise
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            time.sleep(retry_delay)
    
    raise Exception(f"Failed after {max_retries} attempts")

def should_check_sheet(sheet_name):
    """Check if a sheet name matches our patterns"""
    if not SHEET_NAME_PATTERNS:
        return True
    
    sheet_name_lower = sheet_name.lower()
    for pattern in SHEET_NAME_PATTERNS:
        if pattern.lower() in sheet_name_lower:
            return True
    return False

def get_sheets_to_process(client):
    """Get list of sheets to process - either from known IDs or discovery"""
    sheets_to_process = []
    
    if KNOWN_JOB_SHEET_IDS and not ENABLE_DISCOVERY:
        # Use only known sheet IDs (fastest)
        logging.info(f"Using {len(KNOWN_JOB_SHEET_IDS)} known sheet IDs (discovery disabled)")
        
        for sheet_id in KNOWN_JOB_SHEET_IDS:
            try:
                # Get basic sheet info
                sheet_info = make_api_call(client.Sheets.get_sheet, sheet_id, include='sheetVersion')
                sheets_to_process.append({
                    'sheet_id': sheet_id,
                    'sheet_name': sheet_info.name
                })
                logging.info(f"  ✅ Added known sheet: {sheet_info.name} (ID: {sheet_id})")
            except Exception as e:
                logging.warning(f"  ⚠️  Could not access known sheet ID {sheet_id}: {e}")
        
    elif ENABLE_DISCOVERY:
        # Discover sheets (filtered)
        logging.info("Discovering sheets with job number columns...")
        
        # Get list of all sheets
        sheets_response = make_api_call(client.Sheets.list_sheets, include_all=True)
        total_sheets = len(sheets_response.data)
        logging.info(f"Found {total_sheets} total sheets")
        
        # First, filter by name patterns
        candidates = []
        for sheet_info in sheets_response.data:
            if sheet_info.id == STATE_SHEET_ID:
                continue
                
            if should_check_sheet(sheet_info.name):
                candidates.append({
                    'sheet_id': sheet_info.id,
                    'sheet_name': sheet_info.name
                })
        
        logging.info(f"Filtered to {len(candidates)} candidate sheets based on name patterns")
        
        if len(candidates) > 50:
            logging.warning(f"Still have {len(candidates)} sheets to check. Consider adding known sheet IDs to KNOWN_JOB_SHEET_IDS")
        
        # Now check these candidates for required columns
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_sheet = {}
            
            for candidate in candidates:
                future = executor.submit(check_sheet_columns, client, candidate['sheet_id'], candidate['sheet_name'])
                future_to_sheet[future] = candidate
            
            for future in as_completed(future_to_sheet):
                candidate = future_to_sheet[future]
                
                try:
                    result = future.result()
                    if result:
                        sheets_to_process.append(candidate)
                        logging.info(f"  ✅ Found qualifying sheet: {candidate['sheet_name']} (ID: {candidate['sheet_id']})")
                        
                        # Suggestion for optimization
                        if candidate['sheet_id'] not in KNOWN_JOB_SHEET_IDS:
                            logging.info(f"     💡 Add this ID to KNOWN_JOB_SHEET_IDS for faster future runs: {candidate['sheet_id']}")
                except Exception as e:
                    logging.debug(f"  Sheet '{candidate['sheet_name']}' doesn't have required columns")
    
    return sheets_to_process

def check_sheet_columns(client, sheet_id, sheet_name):
    """Check if a sheet has required columns"""
    try:
        # Get only column information
        sheet = make_api_call(client.Sheets.get_sheet, sheet_id, include='columns')
        
        # Build column map
        column_map = {}
        for column in sheet.columns:
            if column.title:
                column_map[column.title.lower()] = column.id
        
        # For Resiliency Promax Database sheets, look for exact column names
        required_found = {}
        missing_columns = []
        
        # Check for exact Dept # column
        if "dept #" in column_map:
            required_found["dept"] = column_map["dept #"]
        else:
            missing_columns.append("Dept #")
        
        # Check for exact Work Request # column
        if "work request #" in column_map:
            required_found["wr_num"] = column_map["work request #"]
        else:
            missing_columns.append("Work Request #")
        
        # Check for exact Job # column
        if "job #" in column_map:
            required_found["job_num"] = column_map["job #"]
        else:
            missing_columns.append("Job #")
        
        # Check for helper columns (exact match)
        helper_found = {}
        if "helper dept #" in column_map:
            helper_found["helper_dept"] = column_map["helper dept #"]
            logging.info(f"  ✅ Found Helper Dept # column in '{sheet_name}'")
        
        if "helper job [#]" in column_map:
            helper_found["helper_job_num"] = column_map["helper job [#]"]
            logging.info(f"  ✅ Found Helper Job [#] column in '{sheet_name}'")
        
        # Log what was found
        if len(missing_columns) == 0:
            logging.info(f"  ✅ Found all required columns in '{sheet_name}'")
            if len(helper_found) > 0:
                logging.info(f"  ✅ Also has {len(helper_found)} helper column(s)")
        elif len(required_found) > 0:
            found_cols = []
            if "dept" in required_found:
                found_cols.append("Dept #")
            if "wr_num" in required_found:
                found_cols.append("Work Request #")
            if "job_num" in required_found:
                found_cols.append("Job #")
            logging.info(f"  🔸 Partial match in '{sheet_name}': Found {found_cols}, Missing {missing_columns}")
        
        # Only return if all required columns are found
        if len(missing_columns) == 0:
            return {
                'columns': {**required_found, **helper_found},
                'has_helper_columns': len(helper_found) == 2
            }
        else:
            return None
        
    except Exception as e:
        logging.debug(f"Error checking sheet '{sheet_name}': {e}")
        return None

def fetch_sheet_rows(client, sheet_info, column_info):
    """Fetch rows from a single sheet"""
    sheet_id = sheet_info['sheet_id']
    columns = column_info['columns']
    has_helper = column_info['has_helper_columns']
    
    all_rows = []
    
    try:
        # Fetch only required columns
        column_ids = list(columns.values())
        
        # Get sheet with only required columns
        sheet = make_api_call(
            client.Sheets.get_sheet,
            sheet_id,
            column_ids=column_ids
        )
        
        for row in sheet.rows:
            cell_map = {cell.column_id: cell for cell in row.cells}
            
            dept_cell = cell_map.get(columns["dept"])
            wr_num_cell = cell_map.get(columns["wr_num"])
            job_num_cell = cell_map.get(columns["job_num"])
            
            dept = dept_cell.display_value if dept_cell and dept_cell.display_value else None
            wr_num = wr_num_cell.display_value if wr_num_cell and wr_num_cell.display_value else None
            job_num = job_num_cell.display_value if job_num_cell else None
            
            # Read helper columns if present
            helper_dept = None
            helper_job_num = None
            if has_helper:
                helper_dept_cell = cell_map.get(columns.get("helper_dept"))
                helper_job_num_cell = cell_map.get(columns.get("helper_job_num"))
                helper_dept = helper_dept_cell.display_value if helper_dept_cell and helper_dept_cell.display_value else None
                helper_job_num = helper_job_num_cell.display_value if helper_job_num_cell else None
            
            # Filter out excluded values
            if dept and wr_num and not should_exclude_value(dept) and not should_exclude_value(wr_num):
                all_rows.append({
                    "sheet_id": sheet_id,
                    "sheet_name": sheet_info['sheet_name'],
                    "row_id": row.id,
                    "columns": columns,
                    "has_helper": has_helper,
                    "dept": dept,
                    "wr_num": wr_num,
                    "job_num": job_num,
                    "helper_dept": helper_dept,
                    "helper_job_num": helper_job_num,
                })
        
        logging.info(f"  📊 Fetched {len(all_rows)} rows from {sheet_info['sheet_name']}")
        return all_rows
        
    except Exception as e:
        logging.error(f"  ❌ Error fetching rows from {sheet_info['sheet_name']}: {e}")
        return []

def analyze_existing_job_number_format(all_rows):
    """Analyze existing job numbers to determine the naming convention"""
    existing_job_numbers = []
    
    for entry in all_rows:
        if entry["job_num"] and str(entry["job_num"]).strip() and not should_exclude_value(entry["job_num"]):
            existing_job_numbers.append(str(entry["job_num"]).strip())
    
    if not existing_job_numbers:
        logging.info("No existing job numbers found. Using default format: DEPT-###")
        return lambda dept, counter: f"{dept}-{counter:03d}"
    
    # Quick pattern check
    sample = existing_job_numbers[0] if existing_job_numbers else ""
    import re
    if re.match(r'^[A-Z]+-\d{3}$', sample):
        return lambda dept, counter: f"{dept}-{counter:03d}"
    elif re.match(r'^[A-Z]+-\d+$', sample):
        return lambda dept, counter: f"{dept}-{counter}"
    
    # Default
    return lambda dept, counter: f"{dept}-{counter:03d}"

def load_state_fast(client, state_key):
    """Load state efficiently"""
    try:
        # Get state sheet
        state_sheet = make_api_call(client.Sheets.get_sheet, STATE_SHEET_ID)
        
        # Find column IDs
        key_col_id = None
        value_col_id = None
        for column in state_sheet.columns:
            if column.title and column.title.lower() == 'key':
                key_col_id = column.id
            elif column.title and column.title.lower() == 'value':
                value_col_id = column.id
        
        if not key_col_id or not value_col_id:
            logging.warning("State sheet missing required columns")
            return {}
        
        # Find the state row
        for row in state_sheet.rows:
            key_cell = next((cell for cell in row.cells if cell.column_id == key_col_id), None)
            if key_cell and key_cell.value == state_key:
                value_cell = next((cell for cell in row.cells if cell.column_id == value_col_id), None)
                if value_cell and value_cell.value:
                    try:
                        state = json.loads(value_cell.value)
                        logging.info(f"  📚 Loaded {len(state)} existing job numbers from {state_key}")
                        return state
                    except:
                        return {}
        
        logging.info(f"  📝 No previous {state_key} found, starting fresh")
        return {}
        
    except Exception as e:
        logging.warning(f"Could not load {state_key}: {e}")
        return {}

def save_state_fast(client, state_data, state_key):
    """Save state efficiently"""
    try:
        state_json = json.dumps(state_data, indent=2)
        
        # Get state sheet columns
        state_sheet = make_api_call(client.Sheets.get_sheet, STATE_SHEET_ID, include='columns,rows')
        
        key_col_id = None
        value_col_id = None
        for column in state_sheet.columns:
            if column.title and column.title.lower() == 'key':
                key_col_id = column.id
            elif column.title and column.title.lower() == 'value':
                value_col_id = column.id
        
        if not key_col_id or not value_col_id:
            logging.error("Cannot save state - missing columns")
            return
        
        # Find or create row
        state_row_id = None
        for row in state_sheet.rows:
            key_cell = next((cell for cell in row.cells if cell.column_id == key_col_id), None)
            if key_cell and key_cell.value == state_key:
                state_row_id = row.id
                break
        
        if state_row_id:
            # Update existing row
            update_row = smartsheet.models.Row()
            update_row.id = state_row_id
            update_row.cells.append({'column_id': value_col_id, 'value': state_json})
            make_api_call(client.Sheets.update_rows, STATE_SHEET_ID, [update_row])
        else:
            # Create new row
            new_row = smartsheet.models.Row()
            new_row.cells.append({'column_id': key_col_id, 'value': state_key})
            new_row.cells.append({'column_id': value_col_id, 'value': state_json})
            make_api_call(client.Sheets.add_rows, STATE_SHEET_ID, [new_row])
        
        logging.info(f"  💾 Saved {len(state_data)} job numbers to {state_key}")
        
    except Exception as e:
        logging.error(f"Failed to save {state_key}: {e}")

def main():
    if not API_TOKEN:
        logging.error("FATAL: SMARTSHEET_API_TOKEN environment variable not set.")
        return

    start_time = time.time()
    
    logging.info("=" * 60)
    logging.info("🚀 FAST JOB NUMBER GENERATOR - STARTING")
    logging.info("=" * 60)
    
    client = smartsheet.Smartsheet(API_TOKEN)
    client.errors_as_exceptions(True)

    try:
        # Step 1: Load existing state
        logging.info("\n📖 Step 1: Loading existing job numbers...")
        wr_to_job_map = load_state_fast(client, STATE_DATA_KEY)
        helper_wr_to_job_map = load_state_fast(client, HELPER_STATE_DATA_KEY)
        
        # Step 2: Get sheets to process
        logging.info("\n🔍 Step 2: Getting sheets to process...")
        sheets_to_process = get_sheets_to_process(client)
        
        if not sheets_to_process:
            logging.warning("No sheets to process!")
            return
        
        logging.info(f"\n📋 Found {len(sheets_to_process)} sheets to process")
        
        # Step 3: Get column info for each sheet
        logging.info("\n🔧 Step 3: Getting column information...")
        sheet_configs = []
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_sheet = {}
            
            for sheet_info in sheets_to_process:
                future = executor.submit(check_sheet_columns, client, sheet_info['sheet_id'], sheet_info['sheet_name'])
                future_to_sheet[future] = sheet_info
            
            for future in as_completed(future_to_sheet):
                sheet_info = future_to_sheet[future]
                try:
                    column_info = future.result()
                    if column_info:
                        sheet_configs.append({
                            'sheet_info': sheet_info,
                            'column_info': column_info
                        })
                except Exception as e:
                    logging.error(f"Error checking columns for {sheet_info['sheet_name']}: {e}")
        
        # Step 4: Fetch rows from all sheets in parallel
        logging.info("\n📥 Step 4: Fetching rows from sheets...")
        all_rows = []
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = []
            
            for config in sheet_configs:
                future = executor.submit(fetch_sheet_rows, client, config['sheet_info'], config['column_info'])
                futures.append(future)
            
            for future in as_completed(futures):
                try:
                    rows = future.result()
                    all_rows.extend(rows)
                except Exception as e:
                    logging.error(f"Error fetching rows: {e}")
        
        logging.info(f"\n📊 Total rows to process: {len(all_rows)}")
        
        # Step 5: Process job numbers
        logging.info("\n⚙️ Step 5: Processing job numbers...")
        
        # Analyze format
        job_number_formatter = analyze_existing_job_number_format(all_rows)
        
        # Build WR map
        wr_row_map = defaultdict(list)
        for entry in all_rows:
            wr_row_map[entry["wr_num"]].append(entry)
        
        # Initialize counters
        dept_counters = defaultdict(int)
        for jobnum in list(wr_to_job_map.values()) + list(helper_wr_to_job_map.values()):
            try:
                if '-' in jobnum:
                    parts = jobnum.split('-')
                    if len(parts) >= 2 and parts[-1].isdigit():
                        dept = parts[-2] if len(parts) > 1 else parts[0]
                        num = int(parts[-1])
                        dept_counters[dept] = max(dept_counters[dept], num)
            except:
                continue
        
        # Assign job numbers
        updates_by_sheet = defaultdict(list)
        new_assignments = 0
        
        for wr_num, entries in wr_row_map.items():
            # Main job number
            if wr_num not in wr_to_job_map:
                dept = entries[0]["dept"]
                dept_counters[dept] += 1
                job_number = job_number_formatter(dept, dept_counters[dept])
                wr_to_job_map[wr_num] = job_number
                new_assignments += 1
            else:
                job_number = wr_to_job_map[wr_num]
            
            # Process all entries
            for entry in entries:
                needs_update = (entry["job_num"] != job_number or should_exclude_value(entry["job_num"]))
                
                # Helper job numbers
                helper_job_number = None
                helper_needs_update = False
                if entry["has_helper"] and entry["helper_dept"] and not should_exclude_value(entry["helper_dept"]):
                    helper_key = f"{wr_num}_{entry['helper_dept']}"
                    
                    if helper_key not in helper_wr_to_job_map:
                        dept_counters[entry['helper_dept']] += 1
                        helper_job_number = job_number_formatter(entry['helper_dept'], dept_counters[entry['helper_dept']])
                        helper_wr_to_job_map[helper_key] = helper_job_number
                        new_assignments += 1
                    else:
                        helper_job_number = helper_wr_to_job_map[helper_key]
                    
                    helper_needs_update = (entry["helper_job_num"] != helper_job_number or 
                                          should_exclude_value(entry["helper_job_num"]))
                
                if needs_update or helper_needs_update:
                    update_row = smartsheet.models.Row()
                    update_row.id = entry["row_id"]
                    
                    if needs_update:
                        update_row.cells.append({
                            'column_id': entry["columns"]["job_num"],
                            'value': job_number,
                            'strict': False
                        })
                    
                    if helper_needs_update and helper_job_number:
                        update_row.cells.append({
                            'column_id': entry["columns"]["helper_job_num"],
                            'value': helper_job_number,
                            'strict': False
                        })
                    
                    updates_by_sheet[entry["sheet_id"]].append(update_row)
        
        logging.info(f"  ✨ {new_assignments} new job numbers assigned")
        
        # Step 6: Send updates
        if updates_by_sheet:
            logging.info(f"\n📤 Step 6: Updating {len(updates_by_sheet)} sheets...")
            
            for sheet_id, rows in updates_by_sheet.items():
                if rows:
                    # Find sheet name for logging
                    sheet_name = next((r["sheet_name"] for r in all_rows if r["sheet_id"] == sheet_id), sheet_id)
                    
                    # Send in batches
                    for i in range(0, len(rows), BATCH_SIZE):
                        batch = rows[i:i+BATCH_SIZE]
                        logging.info(f"  📝 Updating {len(batch)} rows in {sheet_name}")
                        make_api_call(client.Sheets.update_rows, sheet_id, batch)
        else:
            logging.info("\n✅ No updates needed - all job numbers are current")
        
        # Step 7: Save state
        logging.info("\n💾 Step 7: Saving state...")
        save_state_fast(client, wr_to_job_map, STATE_DATA_KEY)
        save_state_fast(client, helper_wr_to_job_map, HELPER_STATE_DATA_KEY)
        
        # Done!
        elapsed = time.time() - start_time
        logging.info("\n" + "=" * 60)
        logging.info(f"✅ COMPLETE! Processed in {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)")
        logging.info("=" * 60)
        
        # Optimization suggestions
        if ENABLE_DISCOVERY and len(sheets_to_process) > 0:
            logging.info("\n💡 OPTIMIZATION TIP:")
            logging.info("To make future runs even faster, add these sheet IDs to KNOWN_JOB_SHEET_IDS:")
            for sheet in sheets_to_process[:10]:  # Show max 10
                logging.info(f"    {sheet['sheet_id']},  # {sheet['sheet_name']}")
            logging.info("Then set ENABLE_DISCOVERY = False")

    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)
        elapsed = time.time() - start_time
        logging.info(f"\n⏱️ Failed after {elapsed:.1f} seconds")

if __name__ == "__main__":
    main()