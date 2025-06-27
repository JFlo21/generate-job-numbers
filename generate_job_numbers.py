import os
import smartsheet
import logging
import json
from collections import defaultdict

# --- Configuration ---
# Set your Smartsheet API token as a secret in your GitHub repository
API_TOKEN = os.getenv("SMARTSHEET_API_TOKEN")

# --- IDs ---
SOURCE_SHEET_ID = 3239244454645636
STATE_SHEET_ID = 6534534683119492
# New sheet for looking up Dept # by Foreman
DEPT_MAPPING_SHEET_ID = 7060626703601540


# Column IDs from your SOURCE sheet (where data is pasted)
COLUMN_MAP = {
    'dept': 6997862724620164,      # This is still needed for context but will NOT be used for the logic
    'wr_num': 3620163004092292,
    'foreman': 5476104938409860,
    'job_num': 2545575356223364,
}

# Column IDs from your STATE sheet
STATE_COLUMN_MAP = {
    'key': 6556595015864196,
    'value': 4304795202178948
}

# Column IDs from your new DEPT MAPPING sheet
DEPT_MAPPING_COLUMN_MAP = {
    'dept': 8098055590727556,
    'foreman': 77970619625050
}


# --- Constants ---
JOB_NUMBER_PREFIX = "568-"
STATE_SHEET_KEY_CELL = "StateData" # A key to identify the state row

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def create_foreman_dept_map(client):
    """
    Fetches the Dept Mapping sheet and creates an efficient lookup dictionary.
    This map will provide the definitive Dept # for each Foreman.
    Returns a dictionary like: {'Foreman Name': 'Dept Number'}
    """
    logging.info(f"Creating Foreman to Dept # mapping from Sheet ID: {DEPT_MAPPING_SHEET_ID}")
    foreman_to_dept = {}
    
    try:
        mapping_sheet = client.Sheets.get_sheet(DEPT_MAPPING_SHEET_ID)
        
        for row in mapping_sheet.rows:
            row_cells = {cell.column_id: cell for cell in row.cells}
            
            foreman_cell = row_cells.get(DEPT_MAPPING_COLUMN_MAP['foreman'])
            dept_cell = row_cells.get(DEPT_MAPPING_COLUMN_MAP['dept'])

            # Use display_value for robustness
            foreman_name = foreman_cell.display_value if foreman_cell and foreman_cell.display_value else None
            dept_num = dept_cell.display_value if dept_cell and dept_cell.display_value else None

            if foreman_name and dept_num:
                foreman_to_dept[foreman_name] = dept_num
            else:
                logging.warning(f"Skipping row {row.row_number} in Dept Mapping Sheet due to missing Foreman or Dept #.")

        logging.info(f"✅ Successfully created map for {len(foreman_to_dept)} foremen.")
        return foreman_to_dept

    except Exception as e:
        logging.error(f"FATAL: Could not create Foreman to Dept # map. Error: {e}")
        raise # Stop execution if the mapping can't be created

def load_state(client):
    """
    Loads the last known state from the State Sheet.
    """
    logging.info(f"Loading state from State Sheet ID: {STATE_SHEET_ID}")
    try:
        state_sheet = client.Sheets.get_sheet(STATE_SHEET_ID)
        for row in state_sheet.rows:
            key_cell = next((cell for cell in row.cells if cell.column_id == STATE_COLUMN_MAP['key']), None)
            if key_cell and key_cell.value == STATE_SHEET_KEY_CELL:
                value_cell = next((cell for cell in row.cells if cell.column_id == STATE_COLUMN_MAP['value']), None)
                if value_cell and value_cell.value:
                    logging.info("Found existing state. Loading...")
                    return json.loads(value_cell.value)
        
        logging.info("No previous state found in State Sheet. Starting fresh.")
        return {}
    except smartsheet.exceptions.ApiError as e:
        if e.error.result.error_code == 1006: # Sheet not found
            logging.warning("State Sheet not found. Starting fresh.")
            return {}
        raise e

def save_state(client, state_data):
    """
    Saves the new state to the State Sheet, overwriting the old one.
    """
    logging.info(f"Saving new state to State Sheet ID: {STATE_SHEET_ID}")
    state_json = json.dumps(state_data, indent=2)

    try:
        state_sheet = client.Sheets.get_sheet(STATE_SHEET_ID, include=['rows'])
        state_row_id = None
        for row in state_sheet.rows:
            key_cell = next((cell for cell in row.cells if cell.column_id == STATE_COLUMN_MAP['key']), None)
            if key_cell and key_cell.value == STATE_SHEET_KEY_CELL:
                state_row_id = row.id
                break

        if state_row_id:
            logging.info(f"Updating existing state row (ID: {state_row_id})...")
            update_row = smartsheet.models.Row()
            update_row.id = state_row_id
            update_row.cells.append({'column_id': STATE_COLUMN_MAP['value'], 'value': state_json})
            client.Sheets.update_rows(STATE_SHEET_ID, [update_row])
            logging.info("Successfully updated state.")
        else:
            logging.info("State row not found. Creating a new one...")
            new_row = smartsheet.models.Row()
            new_row.to_top = True
            new_row.cells.append({'column_id': STATE_COLUMN_MAP['key'], 'value': STATE_SHEET_KEY_CELL})
            new_row.cells.append({'column_id': STATE_COLUMN_MAP['value'], 'value': state_json})
            client.Sheets.add_rows(STATE_SHEET_ID, [new_row])
            logging.info("Successfully created new state row.")

    except Exception as e:
        logging.error(f"Failed to save state: {e}")
        raise

def main():
    """Main execution function."""
    if not API_TOKEN:
        logging.error("FATAL: SMARTSHEET_API_TOKEN environment variable not set.")
        return

    client = smartsheet.Smartsheet(API_TOKEN)
    client.errors_as_exceptions(True)

    try:
        # 0. Create the Foreman to Dept # lookup map. This is our source of truth.
        foreman_dept_map = create_foreman_dept_map(client)

        # 1. Load the current state from the state sheet
        state = defaultdict(lambda: {'seen_wr': set(), 'count': 0})
        loaded_state = load_state(client)
        for key, value in loaded_state.items():
            state[key] = {'seen_wr': set(value['seen_wr']), 'count': value['count']}

        # 2. Fetch all rows from the source sheet
        logging.info(f"Fetching rows from Source Sheet ID: {SOURCE_SHEET_ID}")
        source_sheet = client.Sheets.get_sheet(SOURCE_SHEET_ID, include=['objectValue'])

        rows_to_update = []
        
        # 3. Process each row to generate job numbers
        logging.info(f"Processing {len(source_sheet.rows)} rows...")
        for row in source_sheet.rows:
            cell_map = {cell.column_id: cell for cell in row.cells}

            wr_num_cell = cell_map.get(COLUMN_MAP['wr_num'])
            foreman_cell = cell_map.get(COLUMN_MAP['foreman'])
            job_num_cell = cell_map.get(COLUMN_MAP['job_num'])

            # Get values from the main sheet
            wr_num = wr_num_cell.display_value if wr_num_cell and wr_num_cell.display_value else None
            foreman = foreman_cell.display_value if foreman_cell and foreman_cell.display_value else None
            
            # --- Core Logic ---
            if not wr_num or not foreman:
                continue

            # --- NEW LOGIC ---
            # Look up the Dept # from our map instead of reading it from the source row
            dept = foreman_dept_map.get(foreman)
            
            if not dept:
                logging.warning(f"Foreman '{foreman}' on row {row.row_number} not found in Dept Mapping Sheet. Skipping.")
                continue

            state_key = f"{dept}_{foreman}"
            
            if wr_num not in state[state_key]['seen_wr']:
                state[state_key]['count'] += 1
                state[state_key]['seen_wr'].add(wr_num)

            new_job_number = f"{JOB_NUMBER_PREFIX}{state[state_key]['count']}"
            
            current_job_number = job_num_cell.display_value if job_num_cell else None
            if new_job_number != current_job_number:
                update_row = smartsheet.models.Row()
                update_row.id = row.id
                update_row.cells.append({
                    'column_id': COLUMN_MAP['job_num'],
                    'value': new_job_number,
                    'strict': False
                })
                rows_to_update.append(update_row)

        # 4. Batch update the source sheet if there are changes
        if rows_to_update:
            logging.info(f"Found {len(rows_to_update)} rows to update. Sending batch update...")
            client.Sheets.update_rows(SOURCE_SHEET_ID, rows_to_update)
            logging.info("✅ Batch update successful.")
        else:
            logging.info("No changes detected. Job numbers are already up-to-date.")

        # 5. Save the final state back to the state sheet for the next run
        final_state_to_save = {}
        for key, value in state.items():
            final_state_to_save[key] = {'seen_wr': list(value['seen_wr']), 'count': value['count']}
        save_state(client, final_state_to_save)

        logging.info("--- 🎉 Process Complete ---")

    except smartsheet.exceptions.ApiError as e:
        logging.error(f"Smartsheet API Error: {e.error.result}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)

if __name__ == "__main__":
    main()
