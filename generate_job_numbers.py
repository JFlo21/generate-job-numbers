import os
import smartsheet
import logging
import json
from collections import defaultdict
import time
from typing import Dict, List, Optional, Tuple

API_TOKEN = os.getenv("SMARTSHEET_API_TOKEN")

# Enhanced sheet configuration with chain support
SHEET_CONFIGS = [
    {
        "sheet_id": 3239244454645636,
        "columns": {
            "dept": 4959096660512644,
            "wr_num": 3620163004092292,
            "job_num": 2545575356223364,
        }
    },
    {
        "sheet_id": 2230129632694148,
        "columns": {
            "dept": 5714903412985732,
            "wr_num": 4026053552721796,
            "job_num": 3463103599300484,
        }
    },
    {
        "sheet_id": 1732945426468740,
        "columns": {
            "dept": 1524340369346436,
            "wr_num": 6590889950138244,
            "job_num": 6027939996716932,
        }
    },
    {
        "sheet_id": 4126460034895748,
        "columns": {
            "dept": 3541960614432644,
            "wr_num": 8608510195224452,
            "job_num": 804556024180314,
        }
    }
]

# Track sheet chains for each source sheet
SHEET_CHAINS = {
    "sheet_1": {
        "source_id": 3239244454645636,
        "duplicates": [],
        "template_config": SHEET_CONFIGS[0]
    },
    "sheet_2": {
        "source_id": 2230129632694148,
        "duplicates": [],
        "template_config": SHEET_CONFIGS[1]
    },
    "sheet_3": {
        "source_id": 1732945426468740,
        "duplicates": [],
        "template_config": SHEET_CONFIGS[2]
    },
    "sheet_4": {
        "source_id": 4126460034895748,
        "duplicates": [],
        "template_config": SHEET_CONFIGS[3]
    }
}

# Smartsheet API limits and error codes
MAX_ROWS_PER_SHEET = 20000  # Conservative limit
RETRY_DELAY = 2  # seconds
MAX_RETRIES = 3

STATE_SHEET_ID = 6534534683119492
STATE_COLUMN_MAP = {
    'key': 6556595015864196,
    'value': 4304795202178948
}
STATE_DATA_KEY = "StateData"
SHEET_CHAINS_KEY = "SheetChains"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class SmartsheetError(Exception):
    """Custom exception for Smartsheet-related errors"""
    pass

class SheetCapacityError(SmartsheetError):
    """Raised when sheet capacity is exceeded"""
    pass

def is_sheet_capacity_error(error) -> bool:
    """Check if error indicates sheet capacity issues"""
    if hasattr(error, 'error') and hasattr(error.error, 'result'):
        error_code = getattr(error.error.result, 'error_code', None)
        error_message = getattr(error.error.result, 'message', '').lower()
        
        # Check for specific capacity-related error codes and messages
        capacity_indicators = [
            'maximum number of rows',
            'row limit exceeded',
            'formula reference limit',
            'sheet size limit'
        ]
        
        return (error_code in [1036, 1074] or  # Common capacity error codes
                any(indicator in error_message for indicator in capacity_indicators))
    return False

def calculate_sheet_capacity(client, sheet_id: int) -> Tuple[int, int]:
    """Calculate current rows and available capacity for a sheet"""
    try:
        sheet = client.Sheets.get_sheet(sheet_id)
        current_rows = len(sheet.rows) if sheet.rows else 0
        available_capacity = MAX_ROWS_PER_SHEET - current_rows
        logging.info(f"Sheet {sheet_id}: {current_rows} rows used, {available_capacity} available")
        return current_rows, max(0, available_capacity)
    except Exception as e:
        logging.error(f"Error calculating capacity for sheet {sheet_id}: {e}")
        return 0, 0

def find_sheet_with_capacity(client, sheet_chain: List[int], required_capacity: int = 1) -> Optional[int]:
    """Find first sheet in chain with sufficient capacity"""
    for sheet_id in sheet_chain:
        _, available_capacity = calculate_sheet_capacity(client, sheet_id)
        if available_capacity >= required_capacity:
            logging.info(f"Found sheet {sheet_id} with capacity for {required_capacity} rows")
            return sheet_id
    return None

def create_duplicate_sheet(client, source_sheet_id: int, sheet_name: str) -> int:
    """Create a duplicate sheet from source template"""
    try:
        logging.info(f"Creating duplicate sheet from source {source_sheet_id}")
        
        # Create a copy of the source sheet
        copy_spec = smartsheet.models.ContainerDestination({
            'destination_type': 'home',
            'new_name': sheet_name
        })
        
        result = client.Sheets.copy_sheet(source_sheet_id, copy_spec, include=['data'])
        duplicate_sheet_id = result.result.id
        
        logging.info(f"Successfully created duplicate sheet {duplicate_sheet_id} with name '{sheet_name}'")
        return duplicate_sheet_id
        
    except Exception as e:
        logging.error(f"Failed to create duplicate sheet: {e}")
        raise SmartsheetError(f"Could not create duplicate sheet: {e}")

def load_sheet_chains(client) -> Dict:
    """Load sheet chain configuration from state"""
    try:
        state_sheet = client.Sheets.get_sheet(STATE_SHEET_ID)
        for row in state_sheet.rows:
            key_cell = next((cell for cell in row.cells if cell.column_id == STATE_COLUMN_MAP['key']), None)
            if key_cell and key_cell.value == SHEET_CHAINS_KEY:
                value_cell = next((cell for cell in row.cells if cell.column_id == STATE_COLUMN_MAP['value']), None)
                if value_cell and value_cell.value:
                    try:
                        chains = json.loads(value_cell.value)
                        logging.info(f"Loaded sheet chains configuration with {len(chains)} chains")
                        return chains
                    except (json.JSONDecodeError, TypeError):
                        logging.warning("Sheet chains data is malformed. Using default configuration.")
        
        logging.info("No existing sheet chains found. Using default configuration.")
        return SHEET_CHAINS.copy()
    except Exception as e:
        logging.warning(f"Could not load sheet chains: {e}. Using default configuration.")
        return SHEET_CHAINS.copy()

def save_sheet_chains(client, chains_data: Dict):
    """Save sheet chain configuration to state"""
    try:
        chains_json = json.dumps(chains_data, indent=2)
        state_sheet = client.Sheets.get_sheet(STATE_SHEET_ID, include=['rows'])
        
        chains_row_id = None
        for row in state_sheet.rows:
            key_cell = next((cell for cell in row.cells if cell.column_id == STATE_COLUMN_MAP['key']), None)
            if key_cell and key_cell.value == SHEET_CHAINS_KEY:
                chains_row_id = row.id
                break
        
        if chains_row_id:
            logging.info(f"Updating existing sheet chains row (ID: {chains_row_id})")
            update_row = smartsheet.models.Row()
            update_row.id = chains_row_id
            update_row.cells.append({'column_id': STATE_COLUMN_MAP['value'], 'value': chains_json})
            client.Sheets.update_rows(STATE_SHEET_ID, [update_row])
        else:
            logging.info("Creating new sheet chains row")
            new_row = smartsheet.models.Row()
            new_row.cells.append({'column_id': STATE_COLUMN_MAP['key'], 'value': SHEET_CHAINS_KEY})
            new_row.cells.append({'column_id': STATE_COLUMN_MAP['value'], 'value': chains_json})
            client.Sheets.add_rows(STATE_SHEET_ID, [new_row])
            
        logging.info("Successfully saved sheet chains configuration")
    except Exception as e:
        logging.error(f"Failed to save sheet chains: {e}")
        raise

def get_all_sheets_in_chain(chain_config: Dict) -> List[int]:
    """Get all sheet IDs in a chain (source + duplicates)"""
    sheets = [chain_config["source_id"]]
    sheets.extend(chain_config.get("duplicates", []))
    return sheets

def collect_duplicate_wr_numbers(client, sheet_chain: List[int], columns: Dict) -> set:
    """Collect all WR numbers across entire sheet chain for duplicate detection"""
    all_wr_numbers = set()
    
    for sheet_id in sheet_chain:
        try:
            sheet = client.Sheets.get_sheet(sheet_id)
            for row in sheet.rows:
                cell_map = {cell.column_id: cell for cell in row.cells}
                wr_num_cell = cell_map.get(columns["wr_num"])
                if wr_num_cell and wr_num_cell.display_value:
                    all_wr_numbers.add(wr_num_cell.display_value)
            logging.info(f"Collected {len(all_wr_numbers)} WR numbers from sheet {sheet_id}")
        except Exception as e:
            logging.error(f"Error collecting WR numbers from sheet {sheet_id}: {e}")
    
    return all_wr_numbers

def load_state(client):
    logging.info(f"Loading job number state from State Sheet ID: {STATE_SHEET_ID}")
    try:
        state_sheet = client.Sheets.get_sheet(STATE_SHEET_ID)
        for row in state_sheet.rows:
            key_cell = next((cell for cell in row.cells if cell.column_id == STATE_COLUMN_MAP['key']), None)
            if key_cell and key_cell.value == STATE_DATA_KEY:
                value_cell = next((cell for cell in row.cells if cell.column_id == STATE_COLUMN_MAP['value']), None)
                if value_cell and value_cell.value:
                    try:
                        state = json.loads(value_cell.value)
                        logging.info(f"Found existing job number state. Loaded {len(state)} records.")
                        return state
                    except (json.JSONDecodeError, TypeError):
                        logging.warning("State data is malformed. Starting fresh.")
                        return {}
        logging.info("No previous job number state found. Starting fresh.")
        return {}
    except smartsheet.exceptions.ApiError as e:
        if e.error.result.error_code == 1006:
            logging.warning("State Sheet not found. Cannot load state.")
            return {}
        raise

def save_state(client, state_data):
    logging.info(f"Saving new job number state to State Sheet ID: {STATE_SHEET_ID}")
    state_json = json.dumps(state_data, indent=2)
    try:
        state_sheet = client.Sheets.get_sheet(STATE_SHEET_ID, include=['rows'])
        state_row_id = None
        for row in state_sheet.rows:
            key_cell = next((cell for cell in row.cells if cell.column_id == STATE_COLUMN_MAP['key']), None)
            if key_cell and key_cell.value == STATE_DATA_KEY:
                state_row_id = row.id
                break
        if state_row_id:
            logging.info(f"Updating existing state row (ID: {state_row_id})...")
            update_row = smartsheet.models.Row()
            update_row.id = state_row_id
            update_row.cells.append({'column_id': STATE_COLUMN_MAP['value'], 'value': state_json})
            client.Sheets.update_rows(STATE_SHEET_ID, [update_row])
        else:
            logging.info("State row not found. Creating a new one...")
            new_row = smartsheet.models.Row()
            new_row.cells.append({'column_id': STATE_COLUMN_MAP['key'], 'value': STATE_DATA_KEY})
            new_row.cells.append({'column_id': STATE_COLUMN_MAP['value'], 'value': state_json})
            client.Sheets.add_rows(STATE_SHEET_ID, [new_row])
        logging.info("Successfully saved state.")
    except Exception as e:
        logging.error(f"Failed to save state: {e}")
        raise

def enhanced_update_rows_with_fallback(client, sheet_id: int, rows: List, chain_key: str, sheet_chains: Dict) -> bool:
    """Enhanced row update with intelligent fallback to duplicates on errors"""
    if not rows:
        return True
        
    try:
        # First attempt: update on primary sheet
        logging.info(f"Attempting to update {len(rows)} rows on sheet {sheet_id}")
        client.Sheets.update_rows(sheet_id, rows)
        logging.info(f"✅ Successfully updated {len(rows)} rows on sheet {sheet_id}")
        return True
        
    except smartsheet.exceptions.ApiError as e:
        if is_sheet_capacity_error(e):
            logging.warning(f"Sheet capacity error on {sheet_id}: {e.error.result.message}")
            
            # Get current chain
            chain_config = sheet_chains[chain_key]
            all_sheets = get_all_sheets_in_chain(chain_config)
            
            # Find sheet with capacity
            target_sheet = find_sheet_with_capacity(client, all_sheets, len(rows))
            
            if target_sheet and target_sheet != sheet_id:
                logging.info(f"Falling back to sheet {target_sheet} with available capacity")
                try:
                    client.Sheets.update_rows(target_sheet, rows)
                    logging.info(f"✅ Successfully updated {len(rows)} rows on fallback sheet {target_sheet}")
                    return True
                except Exception as fallback_error:
                    logging.error(f"Fallback update failed on sheet {target_sheet}: {fallback_error}")
            
            # Create new duplicate sheet as last resort
            logging.info(f"No existing sheets have capacity. Creating duplicate for {chain_key}")
            return create_duplicate_and_update(client, chain_key, sheet_chains, rows)
            
        else:
            # Non-capacity error, retry with exponential backoff
            for attempt in range(MAX_RETRIES):
                try:
                    time.sleep(RETRY_DELAY * (2 ** attempt))
                    client.Sheets.update_rows(sheet_id, rows)
                    logging.info(f"✅ Successfully updated {len(rows)} rows on sheet {sheet_id} (attempt {attempt + 2})")
                    return True
                except Exception as retry_error:
                    logging.warning(f"Retry {attempt + 1} failed for sheet {sheet_id}: {retry_error}")
            
            logging.error(f"All retry attempts failed for sheet {sheet_id}")
            raise
    
    except Exception as e:
        logging.error(f"Unexpected error updating sheet {sheet_id}: {e}")
        raise

def create_duplicate_and_update(client, chain_key: str, sheet_chains: Dict, rows: List) -> bool:
    """Create a new duplicate sheet and update rows there"""
    try:
        chain_config = sheet_chains[chain_key]
        source_sheet_id = chain_config["source_id"]
        
        # Generate unique name for duplicate
        duplicate_count = len(chain_config.get("duplicates", []))
        duplicate_name = f"Sheet {chain_key.split('_')[1]} Duplicate {duplicate_count + 1}"
        
        # Create duplicate sheet
        duplicate_sheet_id = create_duplicate_sheet(client, source_sheet_id, duplicate_name)
        
        # Add to chain configuration
        if "duplicates" not in chain_config:
            chain_config["duplicates"] = []
        chain_config["duplicates"].append(duplicate_sheet_id)
        
        # Save updated chain configuration
        save_sheet_chains(client, sheet_chains)
        
        # Update rows on new duplicate
        client.Sheets.update_rows(duplicate_sheet_id, rows)
        logging.info(f"✅ Successfully updated {len(rows)} rows on new duplicate sheet {duplicate_sheet_id}")
        
        return True
        
    except Exception as e:
        logging.error(f"Failed to create duplicate and update: {e}")
        raise SmartsheetError(f"Could not create duplicate and update: {e}")
def main():
    if not API_TOKEN:
        logging.error("FATAL: SMARTSHEET_API_TOKEN environment variable not set.")
        return

    client = smartsheet.Smartsheet(API_TOKEN)
    client.errors_as_exceptions(True)

    try:
        # Load both job number state and sheet chains
        wr_to_job_map = load_state(client)
        sheet_chains = load_sheet_chains(client)
        
        logging.info("Starting intelligent sheet management process")

        # Process each sheet chain
        for chain_key, chain_config in sheet_chains.items():
            logging.info(f"Processing chain: {chain_key}")
            
            # Get all sheets in this chain
            all_sheets_in_chain = get_all_sheets_in_chain(chain_config)
            columns = chain_config["template_config"]["columns"]
            
            # Collect all WR numbers across the entire chain for duplicate detection
            existing_wr_numbers = collect_duplicate_wr_numbers(client, all_sheets_in_chain, columns)
            
            # Gather all rows from all sheets in this chain
            all_rows = []
            for sheet_id in all_sheets_in_chain:
                logging.info(f"Fetching rows from sheet ID: {sheet_id}")
                try:
                    sheet = client.Sheets.get_sheet(sheet_id)
                    for row in sheet.rows:
                        cell_map = {cell.column_id: cell for cell in row.cells}
                        dept_cell = cell_map.get(columns["dept"])
                        wr_num_cell = cell_map.get(columns["wr_num"])
                        job_num_cell = cell_map.get(columns["job_num"])
                        
                        dept = dept_cell.display_value if dept_cell and dept_cell.display_value else None
                        wr_num = wr_num_cell.display_value if wr_num_cell and wr_num_cell.display_value else None
                        job_num = job_num_cell.display_value if job_num_cell else None
                        
                        if dept and wr_num:
                            all_rows.append({
                                "sheet_id": sheet_id,
                                "row_id": row.id,
                                "columns": columns,
                                "dept": dept,
                                "wr_num": wr_num,
                                "job_num": job_num,
                                "chain_key": chain_key
                            })
                except smartsheet.exceptions.ApiError as e:
                    logging.error(f"Could not access sheet ID {sheet_id}. Error: {e.error.result}")
                    # Continue processing other sheets
                    continue

            logging.info(f"Total rows fetched for chain {chain_key}: {len(all_rows)}")

            # Build a map of WR# to all row entries within this chain
            wr_row_map = defaultdict(list)
            for entry in all_rows:
                wr_row_map[entry["wr_num"]].append(entry)

            # Track department counters across all chains (global state)
            dept_counters = defaultdict(int)
            for jobnum in wr_to_job_map.values():
                try:
                    dept, num = jobnum.split('-')
                    dept_counters[dept] = max(dept_counters[dept], int(num))
                except Exception:
                    continue

            # Check for duplicates within this chain
            seen_sheets_per_wr = defaultdict(set)
            for entry in all_rows:
                seen_sheets_per_wr[entry["wr_num"]].add(entry["sheet_id"])

            for wr_num, sheets in seen_sheets_per_wr.items():
                if len(sheets) > 1:
                    logging.warning(f"Duplicate WR# '{wr_num}' found across multiple sheets in chain {chain_key}. Will assign same job number.")

            # Assign job numbers and prepare updates by sheet
            updates_by_sheet = defaultdict(list)
            for wr_num, entries in wr_row_map.items():
                # Assign job number if not already assigned in global state
                if wr_num not in wr_to_job_map:
                    dept = entries[0]["dept"]
                    dept_counters[dept] += 1
                    job_number = f"{dept}-{dept_counters[dept]}"
                    wr_to_job_map[wr_num] = job_number
                    logging.info(f"Assigned new job number {job_number} to WR# {wr_num}")
                else:
                    job_number = wr_to_job_map[wr_num]

                # Update all rows for this WR# in this chain
                for entry in entries:
                    if entry["job_num"] != job_number:
                        update_row = smartsheet.models.Row()
                        update_row.id = entry["row_id"]
                        update_row.cells.append({
                            'column_id': entry["columns"]["job_num"],
                            'value': job_number,
                            'strict': False
                        })
                        updates_by_sheet[entry["sheet_id"]].append({
                            "row": update_row,
                            "chain_key": entry["chain_key"]
                        })

            # Send updates with intelligent fallback handling
            for sheet_id, update_entries in updates_by_sheet.items():
                if update_entries:
                    rows_to_update = [entry["row"] for entry in update_entries]
                    chain_key = update_entries[0]["chain_key"]  # All entries should have same chain_key
                    
                    success = enhanced_update_rows_with_fallback(
                        client, sheet_id, rows_to_update, chain_key, sheet_chains
                    )
                    
                    if not success:
                        logging.error(f"Failed to update rows on sheet {sheet_id}")

        # Save updated job number state and sheet chains
        save_state(client, wr_to_job_map)
        save_sheet_chains(client, sheet_chains)
        
        logging.info("Intelligent sheet management process completed successfully.")

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
