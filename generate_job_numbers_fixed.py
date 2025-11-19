#!/usr/bin/env python3
"""
Optimized Smartsheet Job Number Generator with CORRECTED Helper Job Logic
- Helper jobs use sequential integers (1, 2, 3...) not "H00001" format
- Helper job count is based on unique Work Request #s the department worked on
- Includes both helper jobs AND main jobs for that department in the count
"""

import smartsheet
import os
import json
import logging
from typing import Dict, List, Optional, Set, Tuple
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, RLock
from dataclasses import dataclass, asdict
import time
from collections import defaultdict
import sys

# Constants
STATE_SHEET_ID = 6534534683119492
WORKSPACE_ID = 2763941144225668
TARGET_FOLDER_IDS = [7644752003786628]
SHEET_NAME_PATTERNS = ['Resiliency Promax Database', 'Intake Promax']
EXCLUDE_PATTERNS = ['template', 'duplicate', 'do not use', 'junk']
CACHE_FILE = 'sheet_discovery_cache.json'
MAX_WORKERS = 5
BATCH_SIZE = 500
RATE_LIMIT_DELAY = 0.2  # 200ms between API calls

# Enhanced Rate Limiter
class EnhancedRateLimiter:
    def __init__(self, min_interval=0.2, burst_size=10):
        self.min_interval = min_interval
        self.burst_size = burst_size
        self.burst_count = 0
        self.last_call = 0
        self.lock = RLock()
        
    def wait_if_needed(self):
        with self.lock:
            now = time.time()
            if self.burst_count < self.burst_size:
                self.burst_count += 1
                self.last_call = now
                return
            
            elapsed = now - self.last_call
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
            
            self.burst_count = 1
            self.last_call = time.time()

rate_limiter = EnhancedRateLimiter()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

@dataclass
class CachedSheet:
    sheet_id: int
    name: str
    columns: Dict[str, Optional[int]]
    has_helper_columns: bool
    last_verified: str
    row_count: int = 0

class SheetCache:
    def __init__(self):
        self.sheets = {}
        self.load_cache()
    
    def load_cache(self):
        if os.path.exists(CACHE_FILE):
            try:
                with open(CACHE_FILE, 'r') as f:
                    data = json.load(f)
                    for sheet_data in data.get('sheets', []):
                        sheet = CachedSheet(**sheet_data)
                        self.sheets[sheet.sheet_id] = sheet
                    logging.info(f"📦 Loaded {len(self.sheets)} cached sheets")
            except Exception as e:
                logging.warning(f"Could not load cache: {e}")
    
    def save_cache(self):
        try:
            data = {
                'sheets': [asdict(sheet) for sheet in self.sheets.values()],
                'updated': datetime.now().isoformat()
            }
            with open(CACHE_FILE, 'w') as f:
                json.dump(data, f, indent=2)
            logging.info(f"💾 Saved {len(self.sheets)} sheets to cache")
        except Exception as e:
            logging.error(f"Could not save cache: {e}")
    
    def get_sheet(self, sheet_id: int) -> Optional[CachedSheet]:
        return self.sheets.get(sheet_id)
    
    def add_sheet(self, sheet: CachedSheet):
        self.sheets[sheet.sheet_id] = sheet
        self.save_cache()

sheet_cache = SheetCache()

class StateTracker:
    """Manages job number state with CORRECTED helper logic"""
    
    def __init__(self, client):
        self.client = client
        self.main_numbers = {}  # {dept|work_request: job_number}
        self.dept_work_requests = defaultdict(list)  # {dept: [work_request_ids]}
        self.dept_job_numbers = defaultdict(dict)  # {dept: {work_request: job_number}}
        self.lock = RLock()
        self.next_main_number = 1
        self.column_ids = {}
        self.load_state()
        
    def load_state(self):
        """Load existing numbers from state sheet"""
        try:
            sheet = make_api_call(self.client.Sheets.get_sheet, STATE_SHEET_ID, include='columns,rows')
            
            # Cache column IDs
            for col in sheet.columns:
                self.column_ids[col.title.lower()] = col.id
            
            # Load existing mappings
            for row in sheet.rows:
                key = None
                number = None
                
                for cell in row.cells:
                    col_name = next((c.title for c in sheet.columns if c.id == cell.column_id), '')
                    if col_name.lower() == 'key':
                        key = cell.value
                    elif col_name.lower() == 'generated number':
                        number = cell.value
                
                if key and number:
                    if key.startswith('HELPER|'):
                        # Parse helper format: HELPER|dept|work_request
                        parts = key.split('|')
                        if len(parts) == 3:
                            dept = parts[1]
                            work_req = parts[2]
                            
                            # Track this work request for the department
                            if work_req not in self.dept_work_requests[dept]:
                                self.dept_work_requests[dept].append(work_req)
                            
                            # Store the job number for this dept/work_request combo
                            self.dept_job_numbers[dept][work_req] = number
                    else:
                        # Main job format: dept|work_request
                        self.main_numbers[key] = number
                        try:
                            self.next_main_number = max(self.next_main_number, int(number) + 1)
                        except:
                            pass
                        
                        # Also track this for department counting (main jobs count too!)
                        parts = key.split('|')
                        if len(parts) == 2:
                            dept = parts[0]
                            work_req = parts[1]
                            
                            # Track this work request for the department
                            if work_req not in self.dept_work_requests[dept]:
                                self.dept_work_requests[dept].append(work_req)
                            
                            # Store the job number for this dept/work_request combo
                            if dept not in self.dept_job_numbers:
                                self.dept_job_numbers[dept] = {}
                            self.dept_job_numbers[dept][work_req] = number
            
            logging.info(f"📚 Loaded {len(self.main_numbers)} main job mappings")
            logging.info(f"📚 Loaded helper data for {len(self.dept_work_requests)} departments")
            
        except Exception as e:
            logging.error(f"Could not load state: {e}")
    
    def get_or_create_main_number(self, dept: str, work_request: str) -> str:
        """Get or create main job number"""
        with self.lock:
            key = f"{dept}|{work_request}"
            
            if key in self.main_numbers:
                return self.main_numbers[key]
            
            # Generate new main job number
            number = str(self.next_main_number)
            self.main_numbers[key] = number
            self.next_main_number += 1
            
            # Track this work request for the department
            if work_request not in self.dept_work_requests[dept]:
                self.dept_work_requests[dept].append(work_request)
            
            # Store in dept job numbers
            if dept not in self.dept_job_numbers:
                self.dept_job_numbers[dept] = {}
            self.dept_job_numbers[dept][work_request] = number
            
            # Save to state sheet
            self.save_number(key, number)
            return number
    
    def get_or_create_helper_number(self, helper_dept: str, work_request: str) -> str:
        """
        Get or create helper job number based on unique Work Request # count.
        This counts how many unique work requests this department has worked on
        (either as main dept or as helper dept).
        Returns format: "{dept}-{sequence}" e.g., "717-2"
        """
        with self.lock:
            # Track this work request for the helper department
            if work_request not in self.dept_work_requests[helper_dept]:
                self.dept_work_requests[helper_dept].append(work_request)
            
            # Check if we already have a job number for this dept/work_request
            if helper_dept in self.dept_job_numbers and work_request in self.dept_job_numbers[helper_dept]:
                return self.dept_job_numbers[helper_dept][work_request]
            
            # Count unique work requests for this department (position in list = job number)
            unique_work_requests = self.dept_work_requests[helper_dept]
            sequence_number = len(unique_work_requests)  # Sequential integer based on count
            
            # Format as "{dept}-{sequence}"
            job_number = f"{helper_dept}-{sequence_number}"
            
            # Store the mapping
            if helper_dept not in self.dept_job_numbers:
                self.dept_job_numbers[helper_dept] = {}
            self.dept_job_numbers[helper_dept][work_request] = job_number
            
            # Save to state sheet with new key format
            key = f"HELPER|{helper_dept}|{work_request}"
            self.save_number(key, job_number)
            
            return job_number
    
    def save_number(self, key: str, number: str):
        """Save a single number to state sheet"""
        try:
            if 'key' not in self.column_ids or 'generated number' not in self.column_ids:
                sheet = make_api_call(self.client.Sheets.get_sheet, STATE_SHEET_ID, include='columns')
                for col in sheet.columns:
                    self.column_ids[col.title.lower()] = col.id
            
            new_row = self.client.models.Row({
                'cells': [
                    {'column_id': self.column_ids['key'], 'value': key},
                    {'column_id': self.column_ids['generated number'], 'value': number}
                ]
            })
            
            make_api_call(self.client.Sheets.add_rows, STATE_SHEET_ID, [new_row])
            
        except Exception as e:
            logging.error(f"Could not save to state sheet: {e}")
    
    def save_all_pending(self):
        """Save any unsaved numbers (batch operation)"""
        # In this implementation, we save immediately, so nothing to do here
        pass

def make_api_call(func, *args, **kwargs):
    """Make an API call with rate limiting and retries"""
    max_retries = 3
    base_delay = 1.0
    
    for attempt in range(max_retries):
        try:
            rate_limiter.wait_if_needed()
            result = func(*args, **kwargs)
            return result
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(base_delay * (2 ** attempt))
                continue
            raise
    
    raise Exception(f"Failed after {max_retries} attempts")

def should_check_sheet(sheet_name: str) -> bool:
    """Check if sheet matches our patterns"""
    sheet_name_lower = sheet_name.lower()
    for pattern in SHEET_NAME_PATTERNS:
        if pattern.lower() in sheet_name_lower:
            return True
    return False

def should_exclude_value(value):
    """Check if value contains excluded patterns"""
    if not value:
        return False
    value_str = str(value).lower()
    for pattern in EXCLUDE_PATTERNS:
        if pattern.lower() in value_str:
            return True
    return False

def find_column_id(columns, patterns):
    """Find column ID matching any of the patterns"""
    for col in columns:
        col_title_lower = col.title.lower()
        for pattern in patterns:
            if pattern.lower() in col_title_lower:
                return col.id
    return None

def check_sheet_columns(client, sheet_id: int, sheet_name: str) -> Optional[CachedSheet]:
    """Check if sheet has required columns and cache the result"""
    try:
        # Check cache first
        cached = sheet_cache.get_sheet(sheet_id)
        if cached:
            logging.info(f"  📋 Using cached info for '{sheet_name}'")
            return cached
        
        # Fetch sheet with columns
        sheet = make_api_call(client.Sheets.get_sheet, sheet_id, include='columns')
        
        # Find required columns
        dept_col = find_column_id(sheet.columns, ['dept #', 'dept#', 'department #'])
        work_req_col = find_column_id(sheet.columns, ['work request #', 'work request#', 'wr #'])
        job_col = find_column_id(sheet.columns, ['job #', 'job#', 'job number'])
        
        if not all([dept_col, work_req_col, job_col]):
            return None
        
        # Check for helper columns
        helper_dept_col = find_column_id(sheet.columns, ['helper dept #', 'helper dept#'])
        helper_job_col = find_column_id(sheet.columns, ['helper job [#]', 'helper job#', 'helper job'])
        has_helper = bool(helper_dept_col and helper_job_col)
        
        # Create cached sheet
        cached_sheet = CachedSheet(
            sheet_id=sheet_id,
            name=sheet_name,
            columns={
                'dept': dept_col,
                'work_request': work_req_col,
                'job': job_col,
                'helper_dept': helper_dept_col if helper_dept_col else None,
                'helper_job': helper_job_col if helper_job_col else None
            },
            has_helper_columns=has_helper,
            last_verified=datetime.now().isoformat(),
            row_count=sheet.total_row_count
        )
        
        # Cache the result
        sheet_cache.add_sheet(cached_sheet)
        
        logging.info(f"  ✅ Found all required columns in '{sheet_name}'")
        if has_helper:
            logging.info(f"  ✅ Also has helper columns")
        
        return cached_sheet
        
    except Exception as e:
        logging.error(f"Error checking sheet {sheet_id}: {e}")
        return None

def discover_sheets_parallel(client) -> List[CachedSheet]:
    """Discover sheets using parallel processing"""
    logging.info("🔍 Starting parallel sheet discovery...")
    
    # Get sheets from target folders
    workspace_sheet_ids = set()
    
    if TARGET_FOLDER_IDS:
        logging.info(f"Looking in folders: {TARGET_FOLDER_IDS}")
        for folder_id in TARGET_FOLDER_IDS:
            try:
                folder = make_api_call(client.Folders.get_folder, folder_id)
                if folder.sheets:
                    for sheet in folder.sheets:
                        workspace_sheet_ids.add(sheet.id)
                    logging.info(f"  Found {len(folder.sheets)} sheets in folder {folder_id}")
                
                # Check subfolders
                if folder.folders:
                    for subfolder in folder.folders:
                        try:
                            sub = make_api_call(client.Folders.get_folder, subfolder.id)
                            if sub.sheets:
                                for sheet in sub.sheets:
                                    workspace_sheet_ids.add(sheet.id)
                                logging.info(f"    Found {len(sub.sheets)} sheets in subfolder {subfolder.name}")
                        except:
                            pass
            except Exception as e:
                logging.warning(f"Could not access folder {folder_id}: {e}")
    
    # Get all sheets to match names
    sheets_response = make_api_call(client.Sheets.list_sheets, include_all=True)
    
    # Filter candidate sheets
    candidates = []
    for sheet_info in sheets_response.data:
        if sheet_info.id in workspace_sheet_ids and should_check_sheet(sheet_info.name):
            candidates.append((sheet_info.id, sheet_info.name))
    
    logging.info(f"Found {len(candidates)} candidate sheets to check")
    
    # Check cached sheets first
    qualified_sheets = []
    sheets_to_check = []
    
    for sheet_id, sheet_name in candidates:
        cached = sheet_cache.get_sheet(sheet_id)
        if cached:
            qualified_sheets.append(cached)
            logging.info(f"✨ Using cached: {sheet_name}")
        else:
            sheets_to_check.append((sheet_id, sheet_name))
    
    # Parallel check for new sheets
    if sheets_to_check:
        logging.info(f"📡 Checking {len(sheets_to_check)} new sheets in parallel...")
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(check_sheet_columns, client, sheet_id, sheet_name): (sheet_id, sheet_name)
                for sheet_id, sheet_name in sheets_to_check
            }
            
            for future in as_completed(futures):
                result = future.result()
                if result:
                    qualified_sheets.append(result)
    
    logging.info(f"✅ Found {len(qualified_sheets)} sheets with required columns")
    return qualified_sheets

def process_sheet_batch(client, sheet_info: CachedSheet, state_tracker: StateTracker, start_idx: int, batch_rows: List) -> Dict:
    """Process a batch of rows from a sheet"""
    stats = {
        "rows_processed": 0,
        "numbers_generated": 0,
        "rows_updated": []
    }
    
    for row_idx, row in enumerate(batch_rows):
        row_num = start_idx + row_idx + 1
        cells_to_update = []
        dept_val = None
        work_req_val = None
        job_val = None
        helper_dept_val = None
        helper_job_val = None
        
        # Extract values
        for cell in row.cells:
            if cell.column_id == sheet_info.columns['dept']:
                dept_val = cell.value
            elif cell.column_id == sheet_info.columns['work_request']:
                work_req_val = cell.value
            elif cell.column_id == sheet_info.columns['job']:
                job_val = cell.value
            elif sheet_info.has_helper_columns:
                if cell.column_id == sheet_info.columns.get('helper_dept'):
                    helper_dept_val = cell.value
                elif cell.column_id == sheet_info.columns.get('helper_job'):
                    helper_job_val = cell.value
        
        # Skip if excluded
        if should_exclude_value(dept_val) or should_exclude_value(work_req_val):
            continue
        
        stats["rows_processed"] += 1
        
        # Process main job number
        if dept_val and work_req_val and not job_val:
            job_number = state_tracker.get_or_create_main_number(str(dept_val), str(work_req_val))
            
            cells_to_update.append(
                client.models.Cell({
                    'column_id': sheet_info.columns['job'],
                    'value': job_number
                })
            )
            stats["numbers_generated"] += 1
        
        # Process helper job number (with CORRECTED logic and overwrite mismatches)
        if sheet_info.has_helper_columns and helper_dept_val and work_req_val:
            expected_helper_number = state_tracker.get_or_create_helper_number(str(helper_dept_val), str(work_req_val))
            
            # Update if empty OR if value doesn't match expected
            if not helper_job_val or str(helper_job_val) != expected_helper_number:
                cells_to_update.append(
                    client.models.Cell({
                        'column_id': sheet_info.columns['helper_job'],
                        'value': expected_helper_number
                    })
                )
                stats["numbers_generated"] += 1
        
        # Update row if needed
        if cells_to_update:
            row_update = client.models.Row({
                'id': row.id,
                'cells': cells_to_update
            })
            stats["rows_updated"].append(row_update)
    
    return stats

def process_sheet(client, sheet_info: CachedSheet, state_tracker: StateTracker) -> Tuple[int, int]:
    """Process a single sheet with batch processing"""
    try:
        logging.info(f"  📄 Processing: {sheet_info.name}")
        sheet = make_api_call(client.Sheets.get_sheet, sheet_info.sheet_id, include='rows')
        
        total_processed = 0
        total_generated = 0
        all_updates = []
        
        # Process in batches
        for i in range(0, len(sheet.rows), BATCH_SIZE):
            batch = sheet.rows[i:i + BATCH_SIZE]
            stats = process_sheet_batch(client, sheet_info, state_tracker, i, batch)
            
            total_processed += stats["rows_processed"]
            total_generated += stats["numbers_generated"]
            all_updates.extend(stats["rows_updated"])
            
            # Apply batch updates
            if len(all_updates) >= BATCH_SIZE:
                make_api_call(client.Sheets.update_rows, sheet_info.sheet_id, all_updates[:BATCH_SIZE])
                all_updates = all_updates[BATCH_SIZE:]
                logging.info(f"    ✅ Batch update: {BATCH_SIZE} rows")
        
        # Apply remaining updates
        if all_updates:
            make_api_call(client.Sheets.update_rows, sheet_info.sheet_id, all_updates)
            logging.info(f"    ✅ Final update: {len(all_updates)} rows")
        
        logging.info(f"    📊 Processed {total_processed} rows, generated {total_generated} numbers")
        return total_processed, total_generated
        
    except Exception as e:
        logging.error(f"Error processing sheet {sheet_info.name}: {e}")
        return 0, 0

class ProgressTracker:
    def __init__(self, total_sheets):
        self.total_sheets = total_sheets
        self.completed = 0
        self.start_time = time.time()
        self.lock = Lock()
    
    def update(self, sheet_name):
        with self.lock:
            self.completed += 1
            elapsed = time.time() - self.start_time
            avg_time = elapsed / self.completed
            remaining = self.total_sheets - self.completed
            eta = avg_time * remaining
            
            progress_pct = (self.completed / self.total_sheets) * 100
            eta_mins = eta / 60
            
            logging.info(f"📈 Progress: {self.completed}/{self.total_sheets} ({progress_pct:.1f}%) - ETA: {eta_mins:.1f} min - Completed: {sheet_name}")

def process_sheets_parallel(client, sheets: List[CachedSheet], state_tracker: StateTracker):
    """Process multiple sheets in parallel"""
    if not sheets:
        logging.warning("No sheets to process")
        return
    
    progress = ProgressTracker(len(sheets))
    total_rows = 0
    total_generated = 0
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_sheet, client, sheet, state_tracker): sheet
            for sheet in sheets
        }
        
        for future in as_completed(futures):
            sheet = futures[future]
            try:
                rows, generated = future.result()
                total_rows += rows
                total_generated += generated
                progress.update(sheet.name)
            except Exception as e:
                logging.error(f"Error processing {sheet.name}: {e}")
                progress.update(sheet.name)
    
    return total_rows, total_generated

def main():
    api_token = os.environ.get('SMARTSHEET_API_TOKEN')
    if not api_token:
        logging.error("❌ SMARTSHEET_API_TOKEN environment variable not set")
        sys.exit(1)
    
    client = smartsheet.Smartsheet(api_token)
    
    logging.info("="*60)
    logging.info("🚀 SMARTSHEET JOB NUMBER GENERATOR (FIXED HELPER LOGIC)")
    logging.info("="*60)
    
    # Initialize state tracker
    state_tracker = StateTracker(client)
    
    # Discover sheets
    start_time = time.time()
    qualified_sheets = discover_sheets_parallel(client)
    
    if not qualified_sheets:
        logging.warning("⚠️ No qualifying sheets found")
        return
    
    # Process sheets
    logging.info(f"\n📝 Processing {len(qualified_sheets)} sheets...")
    total_rows, total_generated = process_sheets_parallel(client, qualified_sheets, state_tracker)
    
    # Save any pending state
    state_tracker.save_all_pending()
    
    # Summary
    elapsed = time.time() - start_time
    logging.info("\n" + "="*60)
    logging.info("✅ PROCESSING COMPLETE")
    logging.info(f"  ⏱️ Time: {elapsed/60:.1f} minutes")
    logging.info(f"  📊 Rows processed: {total_rows}")
    logging.info(f"  🔢 Numbers generated: {total_generated}")
    logging.info(f"  📋 Sheets processed: {len(qualified_sheets)}")
    logging.info("="*60)

if __name__ == "__main__":
    main()