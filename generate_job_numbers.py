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
# These are confirmed to be correct.
STATE_COLUMN_MAP = {
    'key': 6556595015864196,
    'value': 4304795202178948
}

# --- Constants ---
# Key for the row in the state sheet that holds the job counter data.
STATE_DATA_KEY = "StateData" 
# Prefix for rows in the state sheet that define which sheets to process.
CONFIG_KEY_PREFIX = "CONFIG_SHEET_"

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_sheet_mapping(client):
    """
    Loads sheet configurations from the State Sheet.
    
    Each config row's value should be a JSON object containing the
    'sheet_id' and its specific 'column_map'.
    
    Returns:
        dict: A dictionary of sheet configurations.
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
                        # Parse the JSON from the value cell
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
    """Loads the last known job number state from the State Sheet."""
    logging.info(f"Loading job number state from State Sheet ID: {STATE_SHEET_ID}")
    try:
        state_sheet = client.Sheets.get_sheet(STATE_SHEET_ID)
        for row in state_sheet.rows:
            key_cell = next((cell for cell in row.cells if cell.column_id == STATE_COLUMN_MAP['key']), None)
            if key_cell and key_cell.value == STATE_DATA_KEY:
                value_cell = next((cell for cell in row.cells if cell.column_id == STATE_COLUMN_MAP['value']), None)
                if value_cell and value_cell.value:
                    logging.info("Found existing job number state. Loading...")
                    return json.loads(value_cell.value)
        
        logging.info("No previous job number state found. Starting fresh.")
        return {}
    except smartsheet.exceptions.ApiError as e:
        if e.error.result.error_code == 1006:
            logging.warning("State Sheet not found. Cannot load state.")
            return {}
        raise

def save_state(client, state_data):
    """Saves the new state to the State Sheet, overwriting the old one."""
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
            logging.info("Successfully updated state.")
        else:
            logging.info("State row not found. Creating a new one...")
            new_row = smartsheet.models.Row()
            new_row.to_top = True
            new_row.cells.append({'column_id': STATE_COLUMN_MAP['key'], 'value': STATE_DATA_KEY})
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
        # 1. Load the dynamic sheet configurations
        sheet_configs = load_sheet_mapping(client)
        if not sheet_configs:
            logging.error("Execution halted: No valid sheet configurations were loaded.")
            return

        # 2. Load the job number state
        state = defaultdict(lambda: {'seen_wr': set(), 'count': 0})
        loaded_state = load_state(client)
        for key, value in loaded_state.items():
            state[key] = {'seen_wr': set(value['seen_wr']), 'count': value['count']}

        # 3. Loop through each configured sheet and process it
        for config_key, config_data in sheet_configs.items():
            sheet_id = config_data['sheet_id']
            column_map = config_data['column_map']
            
            logging.info(f"--- Processing Sheet for '{config_key}' (ID: {sheet_id}) ---")
            
            try:
                source_sheet = client.Sheets.get_sheet(sheet_id, include=['objectValue'])
            except smartsheet.exceptions.ApiError as e:
                logging.error(f"Could not access or find sheet with ID {sheet_id}. Skipping. Error: {e.error.result}")
                continue

            rows_to_update = []
            
            logging.info(f"Processing {len(source_sheet.rows)} rows...")
            for row in source_sheet.rows:
                cell_map = {cell.column_id: cell for cell in row.cells}

                # Use the dynamic column_map for this specific sheet
                dept_cell = cell_map.get(column_map['dept'])
                wr_num_cell = cell_map.get(column_map['wr_num'])
                foreman_cell = cell_map.get(column_map['foreman'])
                job_num_cell = cell_map.get(column_map['job_num'])

                dept = dept_cell.display_value if dept_cell and dept_cell.display_value else None
                wr_num = wr_num_cell.display_value if wr_num_cell and wr_num_cell.display_value else None
                foreman = foreman_cell.display_value if foreman_cell and foreman_cell.display_value else None
                
                if not dept or not wr_num or not foreman:
                    continue

                state_key = f"{dept}_{foreman}"
                
                if wr_num not in state[state_key]['seen_wr']:
                    state[state_key]['count'] += 1
                    state[state_key]['seen_wr'].add(wr_num)

                new_job_number = f"{dept}-{state[state_key]['count']}"
                
                current_job_number = job_num_cell.display_value if job_num_cell else None
                if new_job_number != current_job_number:
                    update_row = smartsheet.models.Row()
                    update_row.id = row.id
                    update_row.cells.append({
                        'column_id': column_map['job_num'],
                        'value': new_job_number,
                        'strict': False
                    })
                    rows_to_update.append(update_row)

            # Batch update the current sheet
            if rows_to_update:
                logging.info(f"Found {len(rows_to_update)} rows to update. Sending batch update...")
                client.Sheets.update_rows(sheet_id, rows_to_update)
                logging.info(f"✅ Batch update successful.")
            else:
                logging.info(f"No changes detected. Job numbers are up-to-date.")

        # 4. Save the final state back to the state sheet
        final_state_to_save = {}
        for key, value in state.items():
            final_state_to_save[key] = {'seen_wr': list(value['seen_wr']), 'count': value['count']}
        save_state(client, final_state_to_save)

        logging.info("--- 🎉 All Sheets Processed. Process Complete ---")

    except smartsheet.exceptions.ApiError as e:
        logging.error(f"A Smartsheet API Error occurred: {e.error.result}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)

if __name__ == "__main__":
    main()
