import os
import smartsheet
import logging
import json
from collections import defaultdict

API_TOKEN = os.getenv("SMARTSHEET_API_TOKEN")

# Sheet configurations using column names instead of hard-coded IDs
# Dynamic column ID lookup will be performed at runtime
SHEET_CONFIGS = [
    {
        "sheet_id": 3239244454645636,
        "required_columns": ["dept", "wr_num", "job_num"]
    },
    {
        "sheet_id": 2230129632694148,
        "required_columns": ["dept", "wr_num", "job_num"]
    },
    {
        "sheet_id": 1732945426468740,
        "required_columns": ["dept", "wr_num", "job_num"]
    },
    {
        "sheet_id": 4126460034895748,
        "required_columns": ["dept", "wr_num", "job_num"]
    }
]

# State sheet configuration - using column names instead of hard-coded IDs
STATE_SHEET_ID = 6534534683119492
STATE_REQUIRED_COLUMNS = ['key', 'value']  # Dynamic lookup will be performed at runtime
STATE_DATA_KEY = "StateData"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_live_col_map(client, sheet_id):
    """
    Fetch the live title-to-ID mapping from the sheet.
    
    Args:
        client: Smartsheet client instance
        sheet_id: ID of the sheet to fetch column mapping from
        
    Returns:
        dict: {column_title: column_id} mapping
    """
    try:
        sheet = client.Sheets.get_sheet(sheet_id)
        col_map = {col.title: col.id for col in sheet.columns}
        logging.info(f"Retrieved {len(col_map)} columns from sheet {sheet_id}: {list(col_map.keys())}")
        return col_map
    except Exception as e:
        logging.error(f"Failed to get column map for sheet {sheet_id}: {e}")
        raise

def assert_columns_exist(col_map, required_titles, sheet_id):
    """
    Check that all required columns are present in the sheet.
    
    Args:
        col_map: Dictionary of {column_title: column_id}
        required_titles: List of required column titles
        sheet_id: Sheet ID for error reporting
        
    Raises:
        ValueError: If any required columns are missing
    """
    missing_columns = [title for title in required_titles if title not in col_map]
    if missing_columns:
        error_msg = f"Missing required columns in sheet {sheet_id}: {missing_columns}. Available columns: {list(col_map.keys())}"
        logging.error(error_msg)
        raise ValueError(error_msg)
    logging.info(f"All required columns found in sheet {sheet_id}: {required_titles}")

def validate_update_payload(rows, live_col_map):
    """
    Validate that all column IDs in the update payload exist in the live column map.
    
    Args:
        rows: List of Row objects to be updated
        live_col_map: Dictionary of {column_title: column_id}
        
    Raises:
        ValueError: If any column IDs in the payload don't exist in the live map
    """
    valid_column_ids = set(live_col_map.values())
    
    for row in rows:
        for cell in row.cells:
            # Handle both cell objects and dictionaries
            if hasattr(cell, 'column_id'):
                column_id = cell.column_id
            elif isinstance(cell, dict) and 'column_id' in cell:
                column_id = cell['column_id']
            else:
                continue
                
            if column_id not in valid_column_ids:
                error_msg = f"Invalid column ID {column_id} in update payload. Valid IDs: {valid_column_ids}"
                logging.error(error_msg)
                raise ValueError(error_msg)
    
    logging.info(f"Update payload validated for {len(rows)} rows")

def load_state(client):
    """Load job number state from the state sheet using dynamic column lookups."""
    logging.info(f"Loading job number state from State Sheet ID: {STATE_SHEET_ID}")
    try:
        # Get live column mapping for state sheet
        state_col_map = get_live_col_map(client, STATE_SHEET_ID)
        assert_columns_exist(state_col_map, STATE_REQUIRED_COLUMNS, STATE_SHEET_ID)
        
        state_sheet = client.Sheets.get_sheet(STATE_SHEET_ID)
        for row in state_sheet.rows:
            key_cell = next((cell for cell in row.cells if cell.column_id == state_col_map['key']), None)
            if key_cell and key_cell.value == STATE_DATA_KEY:
                value_cell = next((cell for cell in row.cells if cell.column_id == state_col_map['value']), None)
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
    """Save job number state to the state sheet using dynamic column lookups."""
    logging.info(f"Saving new job number state to State Sheet ID: {STATE_SHEET_ID}")
    state_json = json.dumps(state_data, indent=2)
    try:
        # Get live column mapping for state sheet
        state_col_map = get_live_col_map(client, STATE_SHEET_ID)
        assert_columns_exist(state_col_map, STATE_REQUIRED_COLUMNS, STATE_SHEET_ID)
        
        state_sheet = client.Sheets.get_sheet(STATE_SHEET_ID, include=['rows'])
        state_row_id = None
        for row in state_sheet.rows:
            key_cell = next((cell for cell in row.cells if cell.column_id == state_col_map['key']), None)
            if key_cell and key_cell.value == STATE_DATA_KEY:
                state_row_id = row.id
                break
                
        if state_row_id:
            logging.info(f"Updating existing state row (ID: {state_row_id})...")
            update_row = smartsheet.models.Row()
            update_row.id = state_row_id
            update_row.cells.append({'column_id': state_col_map['value'], 'value': state_json})
            
            # Validate update payload before sending
            validate_update_payload([update_row], state_col_map)
            
            # Wrap update_rows call in try/except with full context logging
            try:
                client.Sheets.update_rows(STATE_SHEET_ID, [update_row])
                logging.info("Successfully updated state row")
            except Exception as update_error:
                logging.error(f"Failed to update state row in sheet {STATE_SHEET_ID}, row {state_row_id}: {update_error}")
                raise
        else:
            logging.info("State row not found. Creating a new one...")
            new_row = smartsheet.models.Row()
            new_row.cells.append({'column_id': state_col_map['key'], 'value': STATE_DATA_KEY})
            new_row.cells.append({'column_id': state_col_map['value'], 'value': state_json})
            
            # Wrap add_rows call in try/except with full context logging
            try:
                client.Sheets.add_rows(STATE_SHEET_ID, [new_row])
                logging.info("Successfully created new state row")
            except Exception as add_error:
                logging.error(f"Failed to add state row to sheet {STATE_SHEET_ID}: {add_error}")
                raise
                
        logging.info("Successfully saved state.")
    except Exception as e:
        logging.error(f"Failed to save state: {e}")
        raise

def main():
    if not API_TOKEN:
        logging.error("FATAL: SMARTSHEET_API_TOKEN environment variable not set.")
        return

    client = smartsheet.Smartsheet(API_TOKEN)
    client.errors_as_exceptions(True)

    try:
        # Load state using dynamic column lookups
        wr_to_job_map = load_state(client)

        # Gather all rows from sheets using dynamic column lookups
        all_rows = []
        for sheet_cfg in SHEET_CONFIGS:
            sheet_id = sheet_cfg["sheet_id"]
            required_columns = sheet_cfg["required_columns"]
            logging.info(f"Fetching rows from sheet ID: {sheet_id}")
            try:
                # Get live column mapping for this sheet
                col_map = get_live_col_map(client, sheet_id)
                assert_columns_exist(col_map, required_columns, sheet_id)
                
                # Create column lookup for this sheet
                columns = {name: col_map[name] for name in required_columns}
                logging.info(f"Using column mapping for sheet {sheet_id}: {columns}")
                
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
                        })
            except smartsheet.exceptions.ApiError as e:
                logging.error(f"Could not access sheet ID {sheet_id}. Skipping. Error: {e.error.result}")
            except ValueError as ve:
                logging.error(f"Column validation failed for sheet {sheet_id}: {ve}")
                continue

        logging.info(f"Total rows fetched across all sheets: {len(all_rows)}")

        # Build a map of WR# to all row entries (across all sheets)
        wr_row_map = defaultdict(list)
        for entry in all_rows:
            wr_row_map[entry["wr_num"]].append(entry)

        # Assign job numbers per department (incrementing across all sheets)
        dept_counters = defaultdict(int)
        for jobnum in wr_to_job_map.values():
            try:
                dept, num = jobnum.split('-')
                dept_counters[dept] = max(dept_counters[dept], int(num))
            except Exception:
                continue

        # Log duplicates across sheets for transparency
        seen_sheets_per_wr = defaultdict(set)
        for entry in all_rows:
            seen_sheets_per_wr[entry["wr_num"]].add(entry["sheet_id"])

        for wr_num, sheets in seen_sheets_per_wr.items():
            if len(sheets) > 1:
                logging.warning(f"Duplicate WR# '{wr_num}' found in multiple sheets. Will assign the same job number to all its occurrences.")

        # Assign job numbers and prepare updates with validation
        updates_by_sheet = defaultdict(list)
        for wr_num, entries in wr_row_map.items():
            # Assign job number if not already assigned in state
            if wr_num not in wr_to_job_map:
                # Use department from first occurrence (could be any, but all should match for a given WR#)
                dept = entries[0]["dept"]
                dept_counters[dept] += 1
                job_number = f"{dept}-{dept_counters[dept]}"
                wr_to_job_map[wr_num] = job_number
                logging.info(f"Assigned new job number {job_number} to WR# {wr_num}")
            else:
                job_number = wr_to_job_map[wr_num]

            # Update all rows for this WR# in all sheets
            for entry in entries:
                if entry["job_num"] != job_number:
                    update_row = smartsheet.models.Row()
                    update_row.id = entry["row_id"]
                    update_row.cells.append({
                        'column_id': entry["columns"]["job_num"],
                        'value': job_number,
                        'strict': False
                    })
                    updates_by_sheet[entry["sheet_id"]].append(update_row)

        # Send updates with enhanced error handling and validation
        for sheet_id, rows in updates_by_sheet.items():
            if rows:
                logging.info(f"Updating {len(rows)} rows on sheet {sheet_id}")
                try:
                    # Get fresh column mapping for validation
                    col_map = get_live_col_map(client, sheet_id)
                    
                    # Validate update payload
                    validate_update_payload(rows, col_map)
                    
                    # Perform the update with comprehensive error handling
                    client.Sheets.update_rows(sheet_id, rows)
                    logging.info(f"✅ Successfully updated {len(rows)} rows on sheet {sheet_id}")
                    
                except Exception as update_error:
                    logging.error(f"Failed to update rows in sheet {sheet_id}. Rows affected: {[row.id for row in rows]}. Error: {update_error}")
                    # Continue with other sheets even if one fails
                    continue

        # Save new job number state using dynamic column lookups
        save_state(client, wr_to_job_map)
        logging.info("Process complete.")

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)

if __name__ == "__main__":
    main()
