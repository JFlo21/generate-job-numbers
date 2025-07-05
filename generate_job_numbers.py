import os
import smartsheet
import logging
import json
from collections import defaultdict

# --- Configuration ---
# Set your Smartsheet API token as a secret in your GitHub repository
API_TOKEN = os.getenv("SMARTSHEET_API_TOKEN")

# --- IDs ---
# The script is driven by the State Sheet, which defines all other sheets.
STATE_SHEET_ID = 6534534683119492

# Column IDs from your STATE sheet ("StateKey" and "StateValue")
STATE_COLUMN_MAP = {
    'key': 6556595015864196,
    'value': 4304795202178948
}

# --- Constants ---
# Key for the row in the state sheet that holds all job counter data.
STATE_DATA_KEY = "StateData"
# Prefix for rows in the state sheet that define which sheets to process.
CONFIG_KEY_PREFIX = "CONFIG_SHEET_"

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_sheet_mapping(client):
    """
    Loads sheet configurations from the State Sheet. Each config's value
    should be a JSON object with 'sheet_id' and 'column_map'.
    """
    logging.info(f"Loading sheet configurations from State Sheet ID: {STATE_SHEET_ID}")
    sheet_configs = {}
    try:
        state_sheet = client.Sheets.get_sheet(STATE_SHEET_ID)
        for row in state_sheet.rows:
            key_cell = next((cell for cell in row.cells if cell.column_id == STATE_COLUMN_MAP['key']), None)
            
            if key_cell and key_cell.value and key_cell.value.startswith(CONFIG_KEY_PREFIX):
                value_cell = next((cell for cell in row.cells if cell.column_id == STATE_COLUMN_MAP['value']), None)
                config_key = key_cell.value
                
                if value_cell and value_cell.value:
                    try:
                        config_data = json.loads(value_cell.value)
                        if 'sheet_id' in config_data and 'column_map' in config_data:
                            sheet_configs[config_key] = config_data
                            logging.info(f"Loaded configuration for '{config_key}'.")
                        else:
                            logging.warning(f"Skipping malformed config for '{config_key}': missing 'sheet_id' or 'column_map'.")
                    except json.JSONDecodeError:
                        logging.warning(f"Skipping invalid JSON in config for '{config_key}'.")
                        
        if not sheet_configs:
            logging.warning("No sheet configurations found. Check your State Sheet setup.")
            
        return sheet_configs
    except smartsheet.exceptions.ApiError as e:
        logging.error(f"Failed to load sheet configurations from State Sheet: {e.error.result}")
        raise

def load_state(client):
    """
    Loads the state from the State Sheet.
    The state now contains two parts:
    - wr_state: Tracks each Work Request #.
    - dept_counters: Tracks the primary job number sequence for each department.
    """
    logging.info(f"Loading job number state from State Sheet ID: {STATE_SHEET_ID}")
    default_state = {'wr_state': {}, 'dept_counters': {}}
    try:
        state_sheet = client.Sheets.get_sheet(STATE_SHEET_ID)
        for row in state_sheet.rows:
            key_cell = next((cell for cell in row.cells if cell.column_id == STATE_COLUMN_MAP['key']), None)
            if key_cell and key_cell.value == STATE_DATA_KEY:
                value_cell = next((cell for cell in row.cells if cell.column_id == STATE_COLUMN_MAP['value']), None)
                if value_cell and value_cell.value:
                    try:
                        loaded_data = json.loads(value_cell.value)
                        # Ensure both keys exist for robustness on subsequent runs
                        state = {
                            'wr_state': loaded_data.get('wr_state', {}),
                            'dept_counters': loaded_data.get('dept_counters', {})
                        }
                        logging.info("Found existing job number state. Loading...")
                        return state
                    except (json.JSONDecodeError, TypeError):
                        logging.warning("State data is malformed. Starting fresh.")
                        return default_state
        
        logging.info("No previous job number state found. Starting fresh.")
        return default_state
    except smartsheet.exceptions.ApiError as e:
        if e.error.result.error_code == 1006:
            logging.warning("State Sheet not found. Cannot load state.")
            return default_state
        raise

def save_state(client, state_data):
    """Saves the new, combined state to the State Sheet."""
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

def main():
    """Main execution function with new three-pass logic."""
    if not API_TOKEN:
        logging.error("FATAL: SMARTSHEET_API_TOKEN environment variable not set.")
        return

    client = smartsheet.Smartsheet(API_TOKEN)
    client.errors_as_exceptions(True)

    try:
        # 1. Load configurations and the historical state
        sheet_configs = load_sheet_mapping(client)
        if not sheet_configs:
            logging.error("Execution halted: No valid sheet configurations were loaded.")
            return

        state = load_state(client)
        wr_state = state.get('wr_state', {})
        dept_counters = defaultdict(int, state.get('dept_counters', {}))

        # 2. PASS 1: Fetch ALL rows from ALL sheets into a single list
        logging.info("--- Pass 1: Fetching all data from all configured sheets ---")
        all_rows_to_process = []
        for config_key, config_data in sheet_configs.items():
            sheet_id = config_data['sheet_id']
            logging.info(f"Fetching rows from sheet '{config_key}' (ID: {sheet_id})")
            try:
                source_sheet = client.Sheets.get_sheet(sheet_id, include=['objectValue'])
                for row in source_sheet.rows:
                    all_rows_to_process.append({
                        'row_obj': row,
                        'sheet_id': sheet_id,
                        'column_map': config_data['column_map']
                    })
            except smartsheet.exceptions.ApiError as e:
                logging.error(f"Could not access sheet ID {sheet_id}. Skipping. Error: {e.error.result}")
        
        logging.info(f"Total rows fetched across all sheets: {len(all_rows_to_process)}")

        # 3. PASS 2: Calculate final job numbers for each unique Work Request #
        logging.info("--- Pass 2: Calculating job numbers for unique Work Requests ---")
        job_assignments_for_this_run = {}
        unique_wr_nums_processed_this_run = set()

        for item in all_rows_to_process:
            row = item['row_obj']
            column_map = item['column_map']
            cell_map = {cell.column_id: cell for cell in row.cells}

            dept_cell = cell_map.get(column_map['dept'])
            wr_num_cell = cell_map.get(column_map['wr_num'])
            
            dept = dept_cell.display_value if dept_cell and dept_cell.display_value else None
            wr_num = wr_num_cell.display_value if wr_num_cell and wr_num_cell.display_value else None

            if not dept or not wr_num or wr_num in unique_wr_nums_processed_this_run:
                continue
            
            # This is the first time we are seeing this WR# in this run.
            # Decide its job number based on historical state.
            if wr_num not in wr_state:
                # This is the FIRST time this WR# has ever been seen.
                dept_counters[dept] += 1
                base_job_num = f"{dept}-{dept_counters[dept]}"
                wr_state[wr_num] = {'base_job_num': base_job_num, 'count': 1}
                job_assignments_for_this_run[wr_num] = base_job_num
            else:
                # This is a DUPLICATE WR# from a previous run.
                wr_state[wr_num]['count'] += 1
                base_job_num = wr_state[wr_num]['base_job_num']
                new_job_number = f"{base_job_num}-{wr_state[wr_num]['count']}"
                job_assignments_for_this_run[wr_num] = new_job_number
                logging.info(f"Duplicate WR# '{wr_num}' from a previous run found. Original job: '{base_job_num}'. Assigning new job: '{new_job_number}'.")

            unique_wr_nums_processed_this_run.add(wr_num)

        # 4. PASS 3: Prepare batch updates for all rows
        logging.info("--- Pass 3: Preparing batch updates for all sheets ---")
        updates_by_sheet = defaultdict(list)
        for item in all_rows_to_process:
            row = item['row_obj']
            sheet_id = item['sheet_id']
            column_map = item['column_map']
            cell_map = {cell.column_id: cell for cell in row.cells}

            wr_num_cell = cell_map.get(column_map['wr_num'])
            job_num_cell = cell_map.get(column_map['job_num'])
            
            wr_num = wr_num_cell.display_value if wr_num_cell and wr_num_cell.display_value else None
            
            if not wr_num or wr_num not in job_assignments_for_this_run:
                continue

            # Assign the pre-calculated job number
            new_job_number = job_assignments_for_this_run[wr_num]
            current_job_number = job_num_cell.display_value if job_num_cell else None

            if new_job_number != current_job_number:
                update_row = smartsheet.models.Row()
                update_row.id = row.id
                update_row.cells.append({
                    'column_id': column_map['job_num'],
                    'value': new_job_number,
                    'strict': False
                })
                updates_by_sheet[sheet_id].append(update_row)

        # 5. Perform all batch updates
        logging.info("--- Final Step: Sending all batch updates ---")
        if not updates_by_sheet:
            logging.info("No changes detected across any sheets. Job numbers are up-to-date.")
        else:
            for sheet_id, rows_to_update in updates_by_sheet.items():
                logging.info(f"Found {len(rows_to_update)} rows to update in sheet {sheet_id}. Sending batch update...")
                client.Sheets.update_rows(sheet_id, rows_to_update)
                logging.info(f"✅ Batch update successful for sheet {sheet_id}.")

        # 6. Save the final state
        final_state = {'wr_state': wr_state, 'dept_counters': dict(dept_counters)}
        save_state(client, final_state)

        logging.info("--- 🎉 All Sheets Processed. Process Complete ---")

    except smartsheet.exceptions.ApiError as e:
        logging.error(f"A Smartsheet API Error occurred: {e.error.result}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)

if __name__ == "__main__":
    main()
