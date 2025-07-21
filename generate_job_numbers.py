import os
import smartsheet
import logging
import json
from collections import defaultdict

API_TOKEN = os.getenv("SMARTSHEET_API_TOKEN")

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

STATE_SHEET_ID = 6534534683119492
STATE_COLUMN_MAP = {
    'key': 6556595015864196,
    'value': 4304795202178948
}
STATE_DATA_KEY = "StateData"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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

def main():
    if not API_TOKEN:
        logging.error("FATAL: SMARTSHEET_API_TOKEN environment variable not set.")
        return

    client = smartsheet.Smartsheet(API_TOKEN)
    client.errors_as_exceptions(True)

    try:
        # Load state
        wr_to_job_map = load_state(client)

        # Gather all rows from both sheets
        all_rows = []
        for sheet_cfg in SHEET_CONFIGS:
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

        logging.info(f"Total rows fetched across both sheets: {len(all_rows)}")

        # Build a map of WR# to all row entries (across both sheets)
        wr_row_map = defaultdict(list)
        for entry in all_rows:
            wr_row_map[entry["wr_num"]].append(entry)

        # Assign job numbers per department (incrementing across both sheets)
        dept_counters = defaultdict(int)
        for jobnum in wr_to_job_map.values():
            try:
                dept, num = jobnum.split('-')
                dept_counters[dept] = max(dept_counters[dept], int(num))
            except Exception:
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
                job_number = f"{dept}-{dept_counters[dept]}"
                wr_to_job_map[wr_num] = job_number
            else:
                job_number = wr_to_job_map[wr_num]

            # Now update all rows for this WR# in both sheets
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
