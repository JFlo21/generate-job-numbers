#!/usr/bin/env python3
"""
Optimized Smartsheet Job Number Generator with Caching and Parallel Processing
Processes Resiliency Promax Database and Intake Promax sheets
"""

import smartsheet
import logging
import time
import json
import os
from datetime import datetime, timedelta
from threading import Semaphore, Lock, RLock
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Set, Optional, Tuple
from dataclasses import dataclass, asdict
import random
from collections import deque
from ss_api_helpers import list_all_sheets, get_folder_children

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('job_generator_optimized.log')
    ]
)

# Configuration
API_TOKEN = os.getenv("SMARTSHEET_API_TOKEN")
if not API_TOKEN:
    raise ValueError("Please set SMARTSHEET_API_TOKEN environment variable")

# Target workspace and folders
TARGET_WORKSPACE_ID = 2763941144225668  # Linetec - Resiliency
TARGET_FOLDER_IDS = [
    1257051776149380,  # Parent folder
    7644752003786628   # Subfolder containing the sheets
]

# Sheet name patterns to process
SHEET_NAME_PATTERNS = [
    "resiliency promax database",
    "intake promax",
]

# State tracking sheet
STATE_SHEET_ID = 6534534683119492
STATE_COLUMN_NAMES = {
    'key': 'key',
    'generated_number': 'generated number'
}

# Cache configuration
CACHE_FILE = "sheet_discovery_cache.json"
CACHE_EXPIRY_DAYS = 7  # Re-verify sheets after 7 days

# Performance settings
MAX_WORKERS = 5  # Number of parallel workers
BATCH_SIZE = 500  # Rows to update in single API call
RATE_LIMIT_REQUESTS = 300  # Smartsheet limit
RATE_LIMIT_WINDOW = 60  # seconds
BURST_CAPACITY = 50  # Allow burst of requests

# Patterns to exclude
EXCLUDE_PATTERNS = ['#', '-', '(', ')', '"', '_']

@dataclass
class CachedSheet:
    """Cached sheet information"""
    sheet_id: int
    name: str
    columns: Dict[str, int]  # column_name -> column_id
    has_helper_columns: bool
    last_verified: str
    row_count: Optional[int] = None

class SheetCache:
    """Manages cached sheet discovery data"""
    
    def __init__(self, cache_file: str):
        self.cache_file = cache_file
        self.cache_lock = RLock()  # Use reentrant lock to avoid deadlock
        self.cache_data = self._load_cache()
    
    def _load_cache(self) -> Dict:
        """Load cache from JSON file"""
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r') as f:
                    data = json.load(f)
                logging.info(f"📚 Loaded cache with {len(data.get('sheets', {}))} sheets")
                return data
            except Exception as e:
                logging.warning(f"Could not load cache: {e}")
        return {"sheets": {}, "last_updated": None}
    
    def save_cache(self):
        """Save cache to JSON file"""
        with self.cache_lock:
            try:
                self.cache_data["last_updated"] = datetime.now().isoformat()
                with open(self.cache_file, 'w') as f:
                    json.dump(self.cache_data, f, indent=2, default=str)
                logging.info(f"💾 Saved cache with {len(self.cache_data['sheets'])} sheets")
            except Exception as e:
                logging.error(f"Could not save cache: {e}")
    
    def get_sheet(self, sheet_id: int) -> Optional[CachedSheet]:
        """Get cached sheet info if not expired"""
        sheet_str = str(sheet_id)
        if sheet_str in self.cache_data["sheets"]:
            sheet_data = self.cache_data["sheets"][sheet_str]
            last_verified = datetime.fromisoformat(sheet_data["last_verified"])
            if datetime.now() - last_verified < timedelta(days=CACHE_EXPIRY_DAYS):
                return CachedSheet(**sheet_data)
        return None
    
    def add_sheet(self, sheet: CachedSheet):
        """Add or update sheet in cache"""
        with self.cache_lock:
            self.cache_data["sheets"][str(sheet.sheet_id)] = asdict(sheet)
            self.save_cache()
    
    def get_all_valid_sheets(self) -> List[CachedSheet]:
        """Get all non-expired cached sheets"""
        valid_sheets = []
        for sheet_id, sheet_data in self.cache_data["sheets"].items():
            sheet = self.get_sheet(int(sheet_id))
            if sheet:
                valid_sheets.append(sheet)
        return valid_sheets

class EnhancedRateLimiter:
    """Rate limiter with burst capacity and request queuing"""
    
    def __init__(self, max_requests=RATE_LIMIT_REQUESTS, window=RATE_LIMIT_WINDOW, burst=BURST_CAPACITY):
        self.max_requests = max_requests
        self.window = window
        self.burst_capacity = burst
        self.request_times = deque()
        self.lock = Lock()
        self.total_requests = 0
        self.total_wait_time = 0
    
    def acquire(self, priority=1):
        """Acquire permission to make a request"""
        with self.lock:
            now = time.time()
            
            # Remove old requests outside the window
            while self.request_times and self.request_times[0] < now - self.window:
                self.request_times.popleft()
            
            # Check if we need to wait
            current_count = len(self.request_times)
            
            if current_count >= self.max_requests:
                # Calculate wait time
                wait_time = self.request_times[0] + self.window - now
                if wait_time > 0:
                    self.total_wait_time += wait_time
                    logging.debug(f"⏳ Rate limit: waiting {wait_time:.1f}s (req #{self.total_requests+1})")
                    time.sleep(wait_time)
                    now = time.time()
                    
                    # Clean again after waiting
                    while self.request_times and self.request_times[0] < now - self.window:
                        self.request_times.popleft()
            
            # Allow burst if under burst capacity
            elif current_count >= self.max_requests - self.burst_capacity:
                # Small delay to prevent hitting hard limit
                time.sleep(0.1)
            
            self.request_times.append(now)
            self.total_requests += 1
    
    def get_stats(self):
        """Get rate limiter statistics"""
        return {
            "total_requests": self.total_requests,
            "total_wait_time": self.total_wait_time,
            "current_rate": len(self.request_times)
        }

# Global instances
rate_limiter = EnhancedRateLimiter()
sheet_cache = SheetCache(CACHE_FILE)

def make_api_call(func, *args, **kwargs):
    """Enhanced API call with better error handling"""
    rate_limiter.acquire()
    max_retries = 10
    base_delay = 1
    
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
            
        except smartsheet.exceptions.ApiError as e:
            error_code = getattr(e.error.result, 'error_code', None)
            status_code = getattr(e.error.result, 'status_code', None)
            
            if error_code == 4003 or status_code == 429:
                wait_time = min(base_delay * (2 ** attempt), 60)
                if hasattr(e.error.result, 'headers'):
                    retry_after = e.error.result.headers.get('Retry-After')
                    if retry_after:
                        wait_time = int(retry_after)
                
                logging.warning(f"⏳ Rate limit (attempt {attempt+1}). Waiting {wait_time}s...")
                time.sleep(wait_time)
                continue
                
            elif status_code in [503, 408, 502, 504]:
                wait_time = min(base_delay * (2 ** attempt), 30)
                logging.warning(f"⚠️ Service issue. Waiting {wait_time}s...")
                time.sleep(wait_time)
                continue
            else:
                logging.error(f"API Error: {e}")
                raise
                
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(base_delay)
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
    
    # Get all sheets
    # Migrated from deprecated include_all=True — sunset June 3, 2026
    all_sheets = list_all_sheets(client, api_call_wrapper=make_api_call)
    total_sheets = len(all_sheets)
    logging.info(f"Found {total_sheets} total sheets")
    
    # Get sheets from target folders
    workspace_sheet_ids = set()
    
    if TARGET_FOLDER_IDS:
        logging.info(f"Looking in folders: {TARGET_FOLDER_IDS}")
        for folder_id in TARGET_FOLDER_IDS:
            try:
                # Migrated from deprecated get_folder SDK call — sunset June 3, 2026
                folder = get_folder_children(folder_id)
                if folder.sheets:
                    for sheet in folder.sheets:
                        workspace_sheet_ids.add(sheet.id)
                    logging.info(f"  Found {len(folder.sheets)} sheets in folder {folder_id}")
                
                # Check subfolders
                if folder.folders:
                    for subfolder in folder.folders:
                        try:
                            # Migrated from deprecated get_folder SDK call — sunset June 3, 2026
                            sub = get_folder_children(subfolder.id)
                            if sub.sheets:
                                for sheet in sub.sheets:
                                    workspace_sheet_ids.add(sheet.id)
                                logging.info(f"    Found {len(sub.sheets)} sheets in subfolder {subfolder.name}")
                        except:
                            pass
            except Exception as e:
                logging.warning(f"Could not access folder {folder_id}: {e}")
    
    # Filter candidate sheets
    candidates = []
    for sheet_info in all_sheets:
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
                sheet_id, sheet_name = futures[future]
                try:
                    result = future.result()
                    if result:
                        qualified_sheets.append(result)
                        logging.info(f"  ✅ Qualified: {sheet_name}")
                except Exception as e:
                    logging.error(f"Error checking {sheet_name}: {e}")
    
    logging.info(f"📊 Found {len(qualified_sheets)} qualifying sheets total")
    return qualified_sheets

def process_sheet_batch(client, sheet_info: CachedSheet, state_tracker) -> Dict:
    """Process a single sheet with batch operations"""
    stats = {"rows_processed": 0, "numbers_generated": 0, "errors": 0}
    
    try:
        logging.info(f"📄 Processing: {sheet_info.name}")
        
        # Get the full sheet data
        sheet = make_api_call(
            client.Sheets.get_sheet,
            sheet_info.sheet_id,
            include='rows'
        )
        
        if not sheet.rows:
            logging.info(f"  ⚠️ No rows in {sheet_info.name}")
            return stats
        
        # Prepare batch updates
        rows_to_update = []
        
        for row in sheet.rows:
            # Get cell values
            dept_val = None
            work_req_val = None
            job_val = None
            helper_dept_val = None
            helper_job_val = None
            
            for cell in row.cells:
                if cell.column_id == sheet_info.columns['dept']:
                    dept_val = cell.value
                elif cell.column_id == sheet_info.columns['work_request']:
                    work_req_val = cell.value
                elif cell.column_id == sheet_info.columns['job']:
                    job_val = cell.value
                elif sheet_info.columns.get('helper_dept') and cell.column_id == sheet_info.columns['helper_dept']:
                    helper_dept_val = cell.value
                elif sheet_info.columns.get('helper_job') and cell.column_id == sheet_info.columns['helper_job']:
                    helper_job_val = cell.value
            
            # Skip if excluded values
            if any(should_exclude_value(v) for v in [dept_val, work_req_val]):
                continue
            
            cells_to_update = []
            
            # Process main job number
            if dept_val and work_req_val and not job_val:
                key = f"{dept_val}|{work_req_val}"
                job_number = state_tracker.get_or_create_number(key, 'main')
                
                cells_to_update.append(
                    client.models.Cell({
                        'column_id': sheet_info.columns['job'],
                        'value': job_number
                    })
                )
                stats["numbers_generated"] += 1
            
            # Process helper columns if present
            if sheet_info.has_helper_columns and helper_dept_val and not helper_job_val:
                helper_key = f"HELPER|{helper_dept_val}"
                helper_number = state_tracker.get_or_create_number(helper_key, 'helper')
                
                cells_to_update.append(
                    client.models.Cell({
                        'column_id': sheet_info.columns['helper_job'],
                        'value': helper_number
                    })
                )
                stats["numbers_generated"] += 1
            
            # Add to batch if we have updates
            if cells_to_update:
                rows_to_update.append(
                    client.models.Row({
                        'id': row.id,
                        'cells': cells_to_update
                    })
                )
            
            stats["rows_processed"] += 1
            
            # Send batch when it reaches size limit
            if len(rows_to_update) >= BATCH_SIZE:
                try:
                    make_api_call(
                        client.Sheets.update_rows,
                        sheet_info.sheet_id,
                        rows_to_update
                    )
                    logging.info(f"  ✅ Batch updated {len(rows_to_update)} rows")
                    rows_to_update = []
                except Exception as e:
                    logging.error(f"Batch update failed: {e}")
                    stats["errors"] += len(rows_to_update)
                    rows_to_update = []
        
        # Send remaining updates
        if rows_to_update:
            try:
                make_api_call(
                    client.Sheets.update_rows,
                    sheet_info.sheet_id,
                    rows_to_update
                )
                logging.info(f"  ✅ Final batch updated {len(rows_to_update)} rows")
            except Exception as e:
                logging.error(f"Final batch update failed: {e}")
                stats["errors"] += len(rows_to_update)
        
        logging.info(f"  ✅ Completed: {stats['rows_processed']} rows, {stats['numbers_generated']} numbers generated")
        
    except Exception as e:
        logging.error(f"Error processing sheet {sheet_info.name}: {e}")
        stats["errors"] += 1
    
    return stats

class StateTracker:
    """Manages job number state and generation"""
    
    def __init__(self, client):
        self.client = client
        self.main_numbers = {}
        self.helper_numbers = {}
        self.lock = Lock()
        self.next_main_number = 1
        self.next_helper_number = 1
        self.column_ids = {}  # Cache column IDs to avoid repeated API calls
        self.load_state()
    
    def load_state(self):
        """Load existing numbers from state sheet"""
        try:
            sheet = make_api_call(self.client.Sheets.get_sheet, STATE_SHEET_ID, include='columns')
            
            # Cache column IDs to avoid repeated lookups
            for col in sheet.columns:
                self.column_ids[col.title.lower()] = col.id
            
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
                        self.helper_numbers[key] = number
                        try:
                            num_val = int(number.replace('H', ''))
                            self.next_helper_number = max(self.next_helper_number, num_val + 1)
                        except:
                            pass
                    else:
                        self.main_numbers[key] = number
                        try:
                            self.next_main_number = max(self.next_main_number, int(number) + 1)
                        except:
                            pass
            
            logging.info(f"📚 Loaded {len(self.main_numbers)} main numbers, {len(self.helper_numbers)} helper numbers")
            
        except Exception as e:
            logging.error(f"Could not load state: {e}")
    
    def get_or_create_number(self, key: str, number_type: str) -> str:
        """Get existing or create new job number"""
        with self.lock:
            if number_type == 'helper':
                if key in self.helper_numbers:
                    return self.helper_numbers[key]
                
                number = f"H{self.next_helper_number:05d}"
                self.helper_numbers[key] = number
                self.next_helper_number += 1
                self.save_number(key, number)
                return number
            else:
                if key in self.main_numbers:
                    return self.main_numbers[key]
                
                number = str(self.next_main_number)
                self.main_numbers[key] = number
                self.next_main_number += 1
                self.save_number(key, number)
                return number
    
    def save_number(self, key: str, number: str):
        """Save new number to state sheet"""
        try:
            new_row = self.client.models.Row()
            new_row.cells.append({
                'column_id': self.get_column_id('key'),
                'value': key
            })
            new_row.cells.append({
                'column_id': self.get_column_id('generated number'),
                'value': number
            })
            
            make_api_call(
                self.client.Sheets.add_rows,
                STATE_SHEET_ID,
                [new_row]
            )
            
        except Exception as e:
            logging.error(f"Could not save state: {e}")
    
    def get_column_id(self, col_name: str):
        """Get column ID from cached IDs"""
        col_id = self.column_ids.get(col_name.lower())
        if col_id:
            return col_id
        # Fall back to fetching if not cached (shouldn't happen normally)
        sheet = make_api_call(self.client.Sheets.get_sheet, STATE_SHEET_ID, include='columns')
        for col in sheet.columns:
            if col.title.lower() == col_name.lower():
                self.column_ids[col_name.lower()] = col.id
                return col.id
        raise ValueError(f"Column '{col_name}' not found in state sheet")

def format_time(seconds):
    """Format seconds into human-readable time"""
    if seconds < 60:
        return f"{seconds:.1f} seconds"
    elif seconds < 3600:
        return f"{seconds/60:.1f} minutes"
    else:
        return f"{seconds/3600:.1f} hours"

def main():
    """Main execution function"""
    start_time = time.time()
    
    logging.info("=" * 60)
    logging.info("🚀 OPTIMIZED JOB NUMBER GENERATOR - STARTING")
    logging.info("=" * 60)
    
    # Initialize Smartsheet client
    client = smartsheet.Smartsheet(API_TOKEN)
    client.errors_as_exceptions(True)
    
    # Load state tracker
    logging.info("\n📖 Step 1: Loading state...")
    state_tracker = StateTracker(client)
    
    # Discover sheets
    logging.info("\n🔍 Step 2: Discovering sheets...")
    qualified_sheets = discover_sheets_parallel(client)
    
    if not qualified_sheets:
        logging.warning("⚠️ No qualifying sheets found!")
        return
    
    # Process sheets in parallel
    logging.info(f"\n⚙️ Step 3: Processing {len(qualified_sheets)} sheets...")
    
    total_stats = {"rows_processed": 0, "numbers_generated": 0, "errors": 0}
    processing_start = time.time()
    
    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(qualified_sheets))) as executor:
        futures = {
            executor.submit(process_sheet_batch, client, sheet, state_tracker): sheet
            for sheet in qualified_sheets
        }
        
        completed = 0
        for future in as_completed(futures):
            sheet = futures[future]
            completed += 1
            
            try:
                stats = future.result()
                total_stats["rows_processed"] += stats["rows_processed"]
                total_stats["numbers_generated"] += stats["numbers_generated"]
                total_stats["errors"] += stats["errors"]
                
                # Calculate progress and time estimates
                elapsed = time.time() - processing_start
                avg_time_per_sheet = elapsed / completed
                remaining_sheets = len(qualified_sheets) - completed
                eta = avg_time_per_sheet * remaining_sheets
                
                progress_pct = (completed / len(qualified_sheets)) * 100
                
                logging.info(f"📊 Progress: [{completed}/{len(qualified_sheets)}] {progress_pct:.1f}% | "
                           f"ETA: {format_time(eta)} | Current: {sheet.name}")
                
            except Exception as e:
                logging.error(f"Failed processing {sheet.name}: {e}")
                total_stats["errors"] += 1
    
    # Final statistics
    elapsed = time.time() - start_time
    
    logging.info("\n" + "=" * 60)
    logging.info("✅ JOB NUMBER GENERATION COMPLETE!")
    logging.info("=" * 60)
    logging.info(f"📊 Final Statistics:")
    logging.info(f"  • Sheets processed: {len(qualified_sheets)}")
    logging.info(f"  • Rows processed: {total_stats['rows_processed']:,}")
    logging.info(f"  • Numbers generated: {total_stats['numbers_generated']:,}")
    logging.info(f"  • Errors: {total_stats['errors']}")
    logging.info(f"  • Time taken: {elapsed:.1f} seconds")
    logging.info(f"  • Processing speed: {total_stats['rows_processed']/elapsed:.1f} rows/second")
    
    # Rate limiter stats
    rl_stats = rate_limiter.get_stats()
    logging.info(f"\n📈 Rate Limiter Statistics:")
    logging.info(f"  • Total API calls: {rl_stats['total_requests']}")
    logging.info(f"  • Total wait time: {rl_stats['total_wait_time']:.1f} seconds")
    logging.info(f"  • Average throughput: {rl_stats['total_requests']/elapsed:.1f} requests/second")
    
    logging.info("\n✨ Cache saved for faster future runs!")

if __name__ == "__main__":
    main()