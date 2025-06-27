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

# Column IDs from your SOURCE sheet (where data is pasted)
# Using the new, corrected column IDs as requested.
COLUMN_MAP = {
    'dept': 4959096660512644,
    'wr_num': 3620163004092292,
    'foreman': 8134988148723588,
    'job_num': 2545575356223364,
}

# Column IDs from your STATE sheet
STATE_COLUMN_MAP = {
    'key': 6556595015864196,
    'value': 4304795202178948
}

# --- Constants ---
# JOB_NUMBER_PREFIX has been removed as it is now dynamic.
STATE_SHEET_KEY_CELL = "StateData" # A key to identify the state row

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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

            # Get cells using the correct column IDs from COLUMN_MAP
            dept_cell = cell_map.get(COLUMN_MAP['dept'])
            wr_num_cell = cell_map.get(COLUMN_MAP['wr_num'])
            foreman_cell = cell_map.get(COLUMN_MAP['foreman'])
            job_num_cell = cell_map.get(COLUMN_MAP['job_num'])

            # Use .display_value to get the final calculated value the user sees
            dept = dept_cell.display_value if dept_cell and dept_cell.display_value else None
            wr_num = wr_num_cell.display_value if wr_num_cell and wr_num_cell.display_value else None
            foreman = foreman_cell.display_value if foreman_cell and foreman_cell.display_value else None
            
            # Skip any row that doesn't have the essential data points
            if not dept or not wr_num or not foreman:
                continue

            # Create a unique key for the Dept+Foreman combination. This is the core of the counter.
            state_key = f"{dept}_{foreman}"
            
            # If the WR# for this combo has NOT been seen before, it's a new job, so increment the count.
            if wr_num not in state[state_key]['seen_wr']:
                state[state_key]['count'] += 1
                state[state_key]['seen_wr'].add(wr_num)

            # --- NEW LOGIC ---
            # Generate the new job number using the dept number from the row as the dynamic prefix.
            new_job_number = f"{dept}-{state[state_key]['count']}"
            
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
