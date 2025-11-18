"""Check columns in the 4 known sheets"""
import os
import smartsheet
import json

API_TOKEN = os.getenv("SMARTSHEET_API_TOKEN")
client = smartsheet.Smartsheet(API_TOKEN)

# The 4 known sheet IDs that were working
KNOWN_SHEET_IDS = [
    3239244454645636,  # Original sheet 1
    2230129632694148,  # Original sheet 2 
    1732945426468740,  # Original sheet 3
    4126460034895748,  # Original sheet 4
]

print("=" * 60)
print("CHECKING COLUMNS IN KNOWN SHEETS")
print("=" * 60)

for sheet_id in KNOWN_SHEET_IDS:
    try:
        # Get sheet with columns
        sheet = client.Sheets.get_sheet(sheet_id, include='columns')
        
        print(f"\n📋 Sheet: {sheet.name}")
        print(f"   ID: {sheet_id}")
        print(f"   Columns found:")
        
        # List all columns
        for col in sheet.columns:
            if col.title:
                # Check if it matches any of our patterns
                marker = ""
                col_lower = col.title.lower()
                if "dept" in col_lower or "department" in col_lower:
                    marker = " ✅ (Dept match)"
                elif "work request" in col_lower or "wr" in col_lower:
                    marker = " ✅ (Work Request match)"
                elif "job" in col_lower:
                    marker = " ✅ (Job match)"
                    
                print(f"     - {col.title}{marker}")
                
    except Exception as e:
        print(f"\n❌ Error accessing sheet {sheet_id}: {e}")

print("\n" + "=" * 60)