"""
Direct Job Number Generator - No Discovery, Maximum Speed
Processes only the 4 known sheets directly
Target runtime: Under 5 minutes
"""

import os
import smartsheet
import logging
import json
import time
from collections import defaultdict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s'
)

API_TOKEN = os.getenv("SMARTSHEET_API_TOKEN")
if not API_TOKEN:
    # For direct execution
    API_TOKEN = "rmMxBWoAniodcEE77JatH9qfXPRXSxPzCiizx"

# The 4 sheets we know need processing
KNOWN_SHEETS = [
    {"id": 3239244454645636, "name": "Resiliency Promax Database"},
    {"id": 2230129632694148, "name": "Resiliency Promax Database Backup 2"},
    {"id": 1732945426468740, "name": "Resiliency Promax Database Backup 3"},
    {"id": 4126460034895748, "name": "Resiliency Promax Database Backup 4"}
]

STATE_SHEET_ID = 6534534683119492
EXCLUDE_PATTERNS = ["no match", "no match - 004", "not assigned"]

def should_exclude(value):
    if not value:
        return False
    value_str = str(value).strip().lower()
    return any(pattern.lower() in value_str for pattern in EXCLUDE_PATTERNS)

def main():
    start_time = time.time()
    logging.info("=" * 60)
    logging.info("🚀 DIRECT JOB NUMBER GENERATOR - STARTING")
    logging.info("=" * 60)
    
    client = smartsheet.Smartsheet(API_TOKEN)
    client.errors_as_exceptions(True)
    
    try:
        # Load existing job numbers
        logging.info("📚 Loading existing job numbers...")
        state_sheet = client.Sheets.get_sheet(STATE_SHEET_ID)
        
        # Find state data
        wr_to_job = {}
        helper_wr_to_job = {}
        
        for row in state_sheet.rows:
            key_cell = None
            value_cell = None
            for cell in row.cells:
                if state_sheet.get_column(cell.column_id).title.lower() == 'key':
                    key_cell = cell
                elif state_sheet.get_column(cell.column_id).title.lower() == 'value':
                    value_cell = cell
            
            if key_cell and value_cell:
                if key_cell.value == "StateData" and value_cell.value:
                    try:
                        wr_to_job = json.loads(value_cell.value)
                        logging.info(f"  ✅ Loaded {len(wr_to_job)} main job numbers")
                    except:
                        pass
                elif key_cell.value == "HelperStateData" and value_cell.value:
                    try:
                        helper_wr_to_job = json.loads(value_cell.value)
                        logging.info(f"  ✅ Loaded {len(helper_wr_to_job)} helper job numbers")
                    except:
                        pass
        
        # Process each known sheet
        all_rows = []
        logging.info(f"\n📋 Processing {len(KNOWN_SHEETS)} known sheets...")
        
        for sheet_info in KNOWN_SHEETS:
            logging.info(f"\n  Processing: {sheet_info['name']}")
            
            try:
                sheet = client.Sheets.get_sheet(sheet_info['id'])
                
                # Find required columns
                columns = {}
                for col in sheet.columns:
                    title_lower = col.title.lower() if col.title else ""
                    if "dept #" in col.title.lower():
                        columns["dept"] = col.id
                    elif "work request #" in col.title.lower():
                        columns["wr_num"] = col.id
                    elif col.title == "Job #":
                        columns["job_num"] = col.id
                    elif "helper dept #" in col.title.lower():
                        columns["helper_dept"] = col.id
                    elif "helper job [#]" in col.title.lower():
                        columns["helper_job_num"] = col.id
                
                if "dept" not in columns or "wr_num" not in columns or "job_num" not in columns:
                    logging.warning(f"    ⚠️  Missing required columns, skipping")
                    continue
                
                # Extract rows
                row_count = 0
                for row in sheet.rows:
                    cells = {cell.column_id: cell for cell in row.cells}
                    
                    dept = cells.get(columns["dept"])
                    dept_val = dept.display_value if dept and dept.display_value else None
                    
                    wr = cells.get(columns["wr_num"])
                    wr_val = wr.display_value if wr and wr.display_value else None
                    
                    job = cells.get(columns["job_num"])
                    job_val = job.display_value if job else None
                    
                    if dept_val and wr_val and not should_exclude(dept_val) and not should_exclude(wr_val):
                        entry = {
                            "sheet_id": sheet_info['id'],
                            "row_id": row.id,
                            "columns": columns,
                            "dept": dept_val,
                            "wr_num": wr_val,
                            "job_num": job_val
                        }
                        
                        # Add helper fields if present
                        if "helper_dept" in columns:
                            h_dept = cells.get(columns["helper_dept"])
                            entry["helper_dept"] = h_dept.display_value if h_dept and h_dept.display_value else None
                        
                        if "helper_job_num" in columns:
                            h_job = cells.get(columns["helper_job_num"])
                            entry["helper_job_num"] = h_job.display_value if h_job else None
                        
                        all_rows.append(entry)
                        row_count += 1
                
                logging.info(f"    ✅ Found {row_count} rows to process")
                
            except Exception as e:
                logging.error(f"    ❌ Error: {e}")
        
        logging.info(f"\n📊 Total rows to process: {len(all_rows)}")
        
        # Process job numbers
        logging.info("\n⚙️ Assigning job numbers...")
        
        # Build WR map
        wr_map = defaultdict(list)
        for entry in all_rows:
            wr_map[entry["wr_num"]].append(entry)
        
        # Initialize counters
        dept_counters = defaultdict(int)
        
        # Parse existing to get max counters
        for job in wr_to_job.values():
            if '-' in job:
                parts = job.split('-')
                if len(parts) >= 2 and parts[-1].isdigit():
                    dept = parts[0] if len(parts) == 2 else parts[-2]
                    num = int(parts[-1])
                    dept_counters[dept] = max(dept_counters[dept], num)
        
        # Assign new job numbers
        updates_by_sheet = defaultdict(list)
        new_assignments = 0
        
        for wr_num, entries in wr_map.items():
            # Assign main job number
            if wr_num not in wr_to_job:
                dept = entries[0]["dept"]
                dept_counters[dept] += 1
                job_number = f"{dept}-{dept_counters[dept]:03d}"
                wr_to_job[wr_num] = job_number
                new_assignments += 1
            else:
                job_number = wr_to_job[wr_num]
            
            # Update all entries
            for entry in entries:
                if entry["job_num"] != job_number or should_exclude(entry["job_num"]):
                    update_row = smartsheet.models.Row()
                    update_row.id = entry["row_id"]
                    update_row.cells.append({
                        'column_id': entry["columns"]["job_num"],
                        'value': job_number,
                        'strict': False
                    })
                    updates_by_sheet[entry["sheet_id"]].append(update_row)
        
        logging.info(f"  ✨ {new_assignments} new job numbers assigned")
        
        # Send updates
        if updates_by_sheet:
            logging.info(f"\n📤 Updating sheets...")
            for sheet_id, rows in updates_by_sheet.items():
                if rows:
                    sheet_name = next(s['name'] for s in KNOWN_SHEETS if s['id'] == sheet_id)
                    logging.info(f"  Updating {len(rows)} rows in {sheet_name}")
                    
                    # Update in batches
                    batch_size = 500
                    for i in range(0, len(rows), batch_size):
                        batch = rows[i:i+batch_size]
                        client.Sheets.update_rows(sheet_id, batch)
        
        # Save state
        logging.info("\n💾 Saving state...")
        
        # Find or create state row
        state_row_id = None
        key_col_id = None
        value_col_id = None
        
        for col in state_sheet.columns:
            if col.title and col.title.lower() == 'key':
                key_col_id = col.id
            elif col.title and col.title.lower() == 'value':
                value_col_id = col.id
        
        for row in state_sheet.rows:
            for cell in row.cells:
                if cell.column_id == key_col_id and cell.value == "StateData":
                    state_row_id = row.id
                    break
        
        state_json = json.dumps(wr_to_job, indent=2)
        
        if state_row_id:
            update_row = smartsheet.models.Row()
            update_row.id = state_row_id
            update_row.cells.append({'column_id': value_col_id, 'value': state_json})
            client.Sheets.update_rows(STATE_SHEET_ID, [update_row])
        else:
            new_row = smartsheet.models.Row()
            new_row.cells.append({'column_id': key_col_id, 'value': 'StateData'})
            new_row.cells.append({'column_id': value_col_id, 'value': state_json})
            client.Sheets.add_rows(STATE_SHEET_ID, [new_row])
        
        # Done!
        elapsed = time.time() - start_time
        logging.info("\n" + "=" * 60)
        logging.info(f"✅ COMPLETE in {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)!")
        logging.info("=" * 60)
        
    except Exception as e:
        logging.error(f"Error: {e}")
        elapsed = time.time() - start_time
        logging.info(f"Failed after {elapsed:.1f} seconds")

if __name__ == "__main__":
    main()