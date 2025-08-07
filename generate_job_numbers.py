"""
Dynamic Job Number Generator for Smartsheet

This script automatically discovers Smartsheet sheets that contain the required columns
(dept, wr_num, job_num) and assigns job numbers based on department and work request numbers.

Key Features:
- Automatically discovers target sheets (no need to hardcode sheet IDs)
- Finds columns by name rather than hardcoded IDs
- Supports duplicate sheets without code changes
- Maintains state between runs to ensure consistent job numbering
- Automatically detects existing job number format from current data
- Preserves the naming convention used in original sheets

Required Environment Variables:
- SMARTSHEET_API_TOKEN: Your Smartsheet API token

Required Sheet Structure:
- Sheets must contain columns named: dept, wr_num, job_num
- State sheet for persistence (STATE_SHEET_ID)

The script will analyze existing job numbers in discovered sheets to determine the
correct naming convention and apply it to new job number assignments.
"""

import os
import smartsheet
import logging
import json
from collections import defaultdict

API_TOKEN = os.getenv("SMARTSHEET_API_TOKEN")

# Required column names for sheets to be processed
# You can modify these if your sheets use different column names
REQUIRED_COLUMNS = ["Dept #", "Work Request #", "Job #"]

# Optional: Set to True to enable debug logging for sheet discovery
DEBUG_SHEET_DISCOVERY = False

# Original hardcoded sheet IDs for reference (now automatically discovered)
# These were: 3239244454645636, 2230129632694148, 1732945426468740, 4126460034895748
ORIGINAL_SHEET_IDS = [3239244454645636, 2230129632694148, 1732945426468740, 4126460034895748]

STATE_SHEET_ID = 6534534683119492
# Column names in the state sheet - we'll discover the IDs dynamically
STATE_COLUMN_NAMES = {
    'key': 'key',        # Column name that stores the key
    'value': 'value'     # Column name that stores the JSON data
}
STATE_DATA_KEY = "StateData"

# Patterns to exclude from processing (case-insensitive)
EXCLUDE_PATTERNS = ["no match", "no match - 004", "not assigned"]

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def should_exclude_value(value):
    """
    Check if a value should be excluded from processing (contains 'no match', etc.)
    """
    if not value:
        return False
    
    value_str = str(value).strip().lower()
    for pattern in EXCLUDE_PATTERNS:
        if pattern.lower() in value_str:
            return True
    return False

def clean_job_number_for_display(job_num):
    """
    Clean job number for display - replace excluded patterns with 'Not Assigned'
    """
    if not job_num or should_exclude_value(job_num):
        return "Not Assigned"
    return str(job_num).strip()

def discover_target_sheets(client):
    """
    Automatically discover sheets that contain the required columns for job number processing.
    Returns a list of sheet configurations similar to the old SHEET_CONFIGS format.
    """
    logging.info("Discovering sheets with required columns...")
    discovered_sheets = []
    
    try:
        # Get list of all sheets the user has access to
        sheets_response = client.Sheets.list_sheets(include_all=True)
        logging.info(f"Found {len(sheets_response.data)} total sheets to check")
        
        for sheet_info in sheets_response.data:
            sheet_id = sheet_info.id
            sheet_name = sheet_info.name
            
            # Skip the state sheet
            if sheet_id == STATE_SHEET_ID:
                logging.info(f"Skipping state sheet: {sheet_name} (ID: {sheet_id})")
                continue
                
            try:
                # Get sheet details to check columns
                sheet = client.Sheets.get_sheet(sheet_id)
                
                # Build a map of column names to column IDs
                column_map = {}
                for column in sheet.columns:
                    if column.title:
                        column_map[column.title.lower()] = column.id
                
                # Check if all required columns are present
                required_columns_found = {}
                missing_columns = []
                
                for req_col in REQUIRED_COLUMNS:
                    if req_col.lower() in column_map:
                        # Map to standardized names for internal processing
                        if req_col == "Dept #":
                            required_columns_found["dept"] = column_map[req_col.lower()]
                        elif req_col == "Work Request #":
                            required_columns_found["wr_num"] = column_map[req_col.lower()]
                        elif req_col == "Job #":
                            required_columns_found["job_num"] = column_map[req_col.lower()]
                    else:
                        missing_columns.append(req_col)
                
                if not missing_columns:
                    # All required columns found - add this sheet to our config
                    sheet_config = {
                        "sheet_id": sheet_id,
                        "sheet_name": sheet_name,
                        "columns": required_columns_found
                    }
                    discovered_sheets.append(sheet_config)
                    
                    # Note if this was one of the original hardcoded sheets
                    if sheet_id in ORIGINAL_SHEET_IDS:
                        logging.info(f"✅ Found qualifying sheet: {sheet_name} (ID: {sheet_id}) [ORIGINAL]")
                    else:
                        logging.info(f"✅ Found qualifying sheet: {sheet_name} (ID: {sheet_id}) [NEW]")
                else:
                    if DEBUG_SHEET_DISCOVERY:
                        logging.debug(f"⏭️  Skipping sheet '{sheet_name}' - missing columns: {missing_columns}")
                    else:
                        logging.info(f"⏭️  Skipping sheet '{sheet_name}' - missing required columns")
                    
            except smartsheet.exceptions.ApiError as e:
                logging.warning(f"Could not access sheet '{sheet_name}' (ID: {sheet_id}). Error: {e.error.result}")
                continue
            except Exception as e:
                logging.warning(f"Error processing sheet '{sheet_name}' (ID: {sheet_id}): {e}")
                continue
                
    except Exception as e:
        logging.error(f"Failed to discover sheets: {e}")
        raise
    
    logging.info(f"Discovery complete. Found {len(discovered_sheets)} qualifying sheets.")
    return discovered_sheets

def analyze_existing_job_number_format(all_rows):
    """
    Analyze existing job numbers to determine the naming convention pattern.
    Returns a function that generates job numbers in the detected format.
    """
    existing_job_numbers = []
    dept_patterns = defaultdict(list)
    
    # Collect all existing job numbers (excluding patterns like "no match - 004")
    for entry in all_rows:
        if entry["job_num"] and str(entry["job_num"]).strip() and not should_exclude_value(entry["job_num"]):
            job_num = str(entry["job_num"]).strip()
            existing_job_numbers.append(job_num)
            dept_patterns[entry["dept"]].append(job_num)
    
    if not existing_job_numbers:
        logging.info("No existing job numbers found. Using default format: DEPT-###")
        return lambda dept, counter: f"{dept}-{counter:03d}"
    
    logging.info(f"Analyzing {len(existing_job_numbers)} existing job numbers to detect pattern...")
    
    # Log sample job numbers for debugging
    sample_jobs = existing_job_numbers[:5]
    logging.info(f"Sample existing job numbers: {sample_jobs}")
    
    # Analyze patterns more comprehensively
    pattern_analysis = {}
    for dept, job_nums in dept_patterns.items():
        if job_nums:
            # Analyze multiple samples if available
            samples = job_nums[:3]
            logging.info(f"Analyzing job numbers for department '{dept}': {samples}")
            
            for sample_job in samples:
                # Check for common patterns
                if sample_job.count('-') == 1:
                    # Simple DEPT-### pattern
                    parts = sample_job.split('-')
                    if len(parts) == 2 and parts[1].isdigit():
                        num_part = parts[1]
                        if len(num_part) >= 3:  # Padded numbers
                            pattern_analysis[dept] = f"DEPT-{num_part.zfill(len(num_part))}"
                        else:
                            pattern_analysis[dept] = "DEPT-NUM"
                        break
                elif sample_job.count('-') == 2:
                    # PREFIX-DEPT-### pattern
                    parts = sample_job.split('-')
                    if len(parts) == 3 and parts[2].isdigit():
                        num_part = parts[2]
                        prefix = parts[0]
                        pattern_analysis[dept] = f"{prefix}-DEPT-{num_part.zfill(len(num_part))}"
                        break
                elif sample_job.isdigit():
                    # Pure numeric
                    pattern_analysis[dept] = "NUMERIC"
                    break
                else:
                    # Try to find any number pattern
                    import re
                    numbers = re.findall(r'\d+', sample_job)
                    if numbers:
                        pattern_analysis[dept] = "CUSTOM"
                        break
    
    # Determine the formatting function based on analysis
    if pattern_analysis:
        # Use the most common pattern or first department's pattern
        first_dept = list(pattern_analysis.keys())[0]
        detected_pattern = pattern_analysis[first_dept]
        
        logging.info(f"Detected job number pattern: {detected_pattern}")
        
        if "DEPT-000" in detected_pattern or detected_pattern.endswith("001"):
            # Zero-padded 3-digit format
            logging.info("Using 3-digit zero-padded format")
            return lambda dept, counter: f"{dept}-{counter:03d}"
        elif detected_pattern.count('-') == 2:
            # PREFIX-DEPT-NUM format
            parts = list(pattern_analysis.values())[0].split('-')
            if len(parts) >= 3:
                prefix = parts[0]
                logging.info(f"Using format: {prefix}-DEPT-###")
                return lambda dept, counter: f"{prefix}-{dept}-{counter:03d}"
        elif detected_pattern == "NUMERIC":
            # Pure numeric
            logging.info("Using numeric-only format")
            return lambda dept, counter: f"{counter:03d}"
        elif detected_pattern == "DEPT-NUM":
            # Simple DEPT-NUM
            logging.info("Using simple DEPT-### format")
            return lambda dept, counter: f"{dept}-{counter}"
    
    # Fallback: analyze the actual format of existing numbers
    if existing_job_numbers:
        sample = existing_job_numbers[0]
        logging.info(f"Using pattern based on sample: {sample}")
        
        # Try to preserve the exact format
        import re
        if re.match(r'^[A-Z]+-\d{3}$', sample):
            return lambda dept, counter: f"{dept}-{counter:03d}"
        elif re.match(r'^[A-Z]+-\d+$', sample):
            return lambda dept, counter: f"{dept}-{counter}"
        
    # Final fallback
    logging.info("Using default format: DEPT-###")
    return lambda dept, counter: f"{dept}-{counter:03d}"

def get_state_sheet_columns(client):
    """
    Dynamically discover the column IDs in the state sheet based on column names.
    """
    try:
        state_sheet = client.Sheets.get_sheet(STATE_SHEET_ID)
        column_map = {}
        
        # Debug: Show all available columns for troubleshooting
        all_columns = []
        for column in state_sheet.columns:
            if column.title:
                all_columns.append(column.title)
                column_name = column.title.lower()
                if column_name == STATE_COLUMN_NAMES['key'].lower():
                    column_map['key'] = column.id
                elif column_name == STATE_COLUMN_NAMES['value'].lower():
                    column_map['value'] = column.id
        
        if 'key' not in column_map or 'value' not in column_map:
            missing = []
            if 'key' not in column_map:
                missing.append(STATE_COLUMN_NAMES['key'])
            if 'value' not in column_map:
                missing.append(STATE_COLUMN_NAMES['value'])
            logging.error(f"State sheet is missing required columns: {missing}")
            logging.info(f"Available columns in state sheet: {all_columns}")
            logging.info(f"Please ensure your state sheet has columns named: {list(STATE_COLUMN_NAMES.values())}")
            raise Exception(f"State sheet is missing required columns: {missing}")
        
        logging.info(f"✅ Found state sheet columns - key: {column_map['key']}, value: {column_map['value']}")
        return column_map
        
    except smartsheet.exceptions.ApiError as e:
        if e.error.result.error_code == 1006:
            raise Exception(f"State Sheet (ID: {STATE_SHEET_ID}) not found. Please check the STATE_SHEET_ID.")
        raise Exception(f"Could not access State Sheet: {e.error.result}")
    except Exception as e:
        if "State sheet is missing required columns" in str(e):
            raise  # Re-raise our custom error with good messaging
        raise Exception(f"Error discovering state sheet columns: {e}")

def load_state(client):
    logging.info(f"Loading job number state from State Sheet ID: {STATE_SHEET_ID}")
    try:
        # Dynamically discover state sheet column IDs
        state_column_map = get_state_sheet_columns(client)
        
        state_sheet = client.Sheets.get_sheet(STATE_SHEET_ID)
        for row in state_sheet.rows:
            key_cell = next((cell for cell in row.cells if cell.column_id == state_column_map['key']), None)
            if key_cell and key_cell.value == STATE_DATA_KEY:
                value_cell = next((cell for cell in row.cells if cell.column_id == state_column_map['value']), None)
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
        # Dynamically discover state sheet column IDs
        state_column_map = get_state_sheet_columns(client)
        
        state_sheet = client.Sheets.get_sheet(STATE_SHEET_ID, include=['rows'])
        state_row_id = None
        for row in state_sheet.rows:
            key_cell = next((cell for cell in row.cells if cell.column_id == state_column_map['key']), None)
            if key_cell and key_cell.value == STATE_DATA_KEY:
                state_row_id = row.id
                break
        if state_row_id:
            logging.info(f"Updating existing state row (ID: {state_row_id})...")
            update_row = smartsheet.models.Row()
            update_row.id = state_row_id
            update_row.cells.append({'column_id': state_column_map['value'], 'value': state_json})
            client.Sheets.update_rows(STATE_SHEET_ID, [update_row])
        else:
            logging.info("State row not found. Creating a new one...")
            new_row = smartsheet.models.Row()
            new_row.cells.append({'column_id': state_column_map['key'], 'value': STATE_DATA_KEY})
            new_row.cells.append({'column_id': state_column_map['value'], 'value': state_json})
            client.Sheets.add_rows(STATE_SHEET_ID, [new_row])
        logging.info("Successfully saved state.")
    except Exception as e:
        logging.error(f"Failed to save state: {e}")
        raise

def main():
    if not API_TOKEN:
        logging.error("FATAL: SMARTSHEET_API_TOKEN environment variable not set.")
        return

    logging.info(f"Excluded patterns for job number processing: {EXCLUDE_PATTERNS}")
    
    client = smartsheet.Smartsheet(API_TOKEN)
    client.errors_as_exceptions(True)

    try:
        # Load state
        wr_to_job_map = load_state(client)

        # Discover sheets that need job number processing
        sheet_configs = discover_target_sheets(client)
        
        if not sheet_configs:
            logging.warning("No qualifying sheets found. Nothing to process.")
            return
        
        # Log discovered sheets
        logging.info("Processing the following sheets:")
        for cfg in sheet_configs:
            logging.info(f"  - {cfg['sheet_name']} (ID: {cfg['sheet_id']})")

        # Gather all rows from discovered sheets
        all_rows = []
        for sheet_cfg in sheet_configs:
            sheet_id = sheet_cfg["sheet_id"]
            columns = sheet_cfg["columns"]
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
                    
                    # Filter out rows with excluded patterns in dept or wr_num
                    if dept and wr_num and not should_exclude_value(dept) and not should_exclude_value(wr_num):
                        all_rows.append({
                            "sheet_id": sheet_id,
                            "row_id": row.id,
                            "columns": columns,
                            "dept": dept,
                            "wr_num": wr_num,
                            "job_num": job_num,  # Keep original for comparison
                        })
                    elif dept and wr_num:
                        # Log excluded entries
                        logging.debug(f"Excluding row with dept='{dept}' or wr_num='{wr_num}' (contains excluded pattern)")
            except smartsheet.exceptions.ApiError as e:
                logging.error(f"Could not access sheet ID {sheet_id}. Skipping. Error: {e.error.result}")

        logging.info(f"Total rows fetched across {len(sheet_configs)} sheets: {len(all_rows)}")

        # Analyze existing job number format before processing
        job_number_formatter = analyze_existing_job_number_format(all_rows)

        # Build a map of WR# to all row entries (across both sheets)
        wr_row_map = defaultdict(list)
        for entry in all_rows:
            wr_row_map[entry["wr_num"]].append(entry)

        # Assign job numbers per department using detected format
        dept_counters = defaultdict(int)
        
        # Parse existing job numbers to get current counters for each department
        for jobnum in wr_to_job_map.values():
            try:
                # Try to extract department and number from existing job numbers
                if '-' in jobnum:
                    parts = jobnum.split('-')
                    if len(parts) >= 2 and parts[-1].isdigit():
                        # Last part is the number, second-to-last might be dept
                        dept = parts[-2] if len(parts) > 1 else parts[0]
                        num = int(parts[-1])
                        dept_counters[dept] = max(dept_counters[dept], num)
                    elif len(parts) == 2 and parts[1].isdigit():
                        # Simple DEPT-NUM format
                        dept, num = parts[0], int(parts[1])
                        dept_counters[dept] = max(dept_counters[dept], num)
            except (ValueError, IndexError):
                # Skip malformed job numbers
                continue

        # To keep log of duplicates across sheets
        seen_sheets_per_wr = defaultdict(set)
        for entry in all_rows:
            seen_sheets_per_wr[entry["wr_num"]].add(entry["sheet_id"])

        for wr_num, sheets in seen_sheets_per_wr.items():
            if len(sheets) > 1:
                logging.warning(f"Duplicate WR# '{wr_num}' found in multiple sheets. Will assign the same job number to all its occurrences.")

        # Assign job numbers and prepare updates
        updates_by_sheet = defaultdict(list)
        for wr_num, entries in wr_row_map.items():
            # Assign job number if not already assigned in state
            if wr_num not in wr_to_job_map:
                # Use department from first occurrence (could be any, but all should match for a given WR#)
                dept = entries[0]["dept"]
                dept_counters[dept] += 1
                job_number = job_number_formatter(dept, dept_counters[dept])
                wr_to_job_map[wr_num] = job_number
                logging.info(f"Assigned new job number: {job_number} for WR# {wr_num} (Dept: {dept})")
            else:
                job_number = wr_to_job_map[wr_num]

            # Now update all rows for this WR# - replace if different or if contains excluded patterns
            for entry in entries:
                current_job_num = entry["job_num"]
                needs_update = (current_job_num != job_number or 
                              should_exclude_value(current_job_num))
                
                if needs_update:
                    update_row = smartsheet.models.Row()
                    update_row.id = entry["row_id"]
                    update_row.cells.append({
                        'column_id': entry["columns"]["job_num"],
                        'value': job_number,
                        'strict': False
                    })
                    updates_by_sheet[entry["sheet_id"]].append(update_row)
                    
                    # Log the update reason
                    if should_exclude_value(current_job_num):
                        logging.info(f"Replacing excluded value '{current_job_num}' with '{job_number}' for WR# {wr_num}")
                    else:
                        logging.debug(f"Updating job number from '{current_job_num}' to '{job_number}' for WR# {wr_num}")

        # Send updates
        for sheet_id, rows in updates_by_sheet.items():
            if rows:
                logging.info(f"Updating {len(rows)} rows on sheet {sheet_id}")
                client.Sheets.update_rows(sheet_id, rows)
                logging.info(f"✅ Updated rows on sheet {sheet_id}")

        # Save new job number state
        save_state(client, wr_to_job_map)
        logging.info("Process complete.")

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)

if __name__ == "__main__":
    main()
