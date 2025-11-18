#!/usr/bin/env python3
"""
Clean up incorrect helper job mappings from state sheet.
Removes entries with "H00001" style format that need to be regenerated.
"""

import smartsheet
import os
import logging

# Constants
STATE_SHEET_ID = 6534534683119492

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(message)s'
)

def main():
    api_token = os.environ.get('SMARTSHEET_API_TOKEN')
    if not api_token:
        logging.error("❌ SMARTSHEET_API_TOKEN environment variable not set")
        return
    
    client = smartsheet.Smartsheet(api_token)
    
    logging.info("🧹 Cleaning incorrect helper job mappings from state sheet...")
    
    # Get the state sheet
    sheet = client.Sheets.get_sheet(STATE_SHEET_ID, include='rows')
    
    # Find rows to delete (those with H-prefix values)
    rows_to_delete = []
    helper_count = 0
    
    for row in sheet.rows:
        key = None
        number = None
        
        for cell in row.cells:
            col_name = next((c.title for c in sheet.columns if c.id == cell.column_id), '')
            if col_name.lower() == 'key':
                key = cell.value
            elif col_name.lower() == 'generated number':
                number = cell.value
        
        # Delete rows with H-prefix numbers (incorrect format)
        if number and str(number).startswith('H'):
            rows_to_delete.append(row.id)
            helper_count += 1
            logging.info(f"  Found incorrect helper mapping: {key} = {number}")
    
    if rows_to_delete:
        logging.info(f"\n🗑️ Deleting {len(rows_to_delete)} incorrect helper mappings...")
        
        # Delete in batches
        batch_size = 100
        for i in range(0, len(rows_to_delete), batch_size):
            batch = rows_to_delete[i:i+batch_size]
            client.Sheets.delete_rows(STATE_SHEET_ID, batch)
            logging.info(f"  Deleted batch of {len(batch)} rows")
        
        logging.info(f"✅ Cleaned up {helper_count} incorrect helper mappings")
    else:
        logging.info("✅ No incorrect helper mappings found")

if __name__ == "__main__":
    main()