"""Check what sheets are in the specified workspace"""
import os
import smartsheet

API_TOKEN = os.getenv("SMARTSHEET_API_TOKEN")
client = smartsheet.Smartsheet(API_TOKEN)

WORKSPACE_ID = 2763941144225668

print("=" * 60)
print(f"CHECKING WORKSPACE ID: {WORKSPACE_ID}")
print("=" * 60)

try:
    # Get workspace with sheets
    workspace = client.Workspaces.get_workspace(WORKSPACE_ID, load_all=True, include='sheets')
    
    print(f"\n📁 Workspace Name: {workspace.name}")
    print(f"📊 Number of sheets: {len(workspace.sheets) if workspace.sheets else 0}")
    
    if workspace.sheets:
        print("\n📋 Sheets in this workspace:")
        for sheet in workspace.sheets:
            print(f"   - {sheet.name} (ID: {sheet.id})")
            
        # Check if any have "resiliency" in the name
        resiliency_sheets = [s for s in workspace.sheets if 'resiliency' in s.name.lower()]
        if resiliency_sheets:
            print(f"\n✅ Found {len(resiliency_sheets)} sheets with 'resiliency' in the name")
        else:
            print("\n❌ No sheets with 'resiliency' in the name found in this workspace")
    else:
        print("\n❌ No sheets found in this workspace")
        
except Exception as e:
    print(f"\n❌ Error accessing workspace: {e}")
    
print("\n" + "=" * 60)
print("To find the Resiliency Promax Database sheets, we may need to check other workspaces.")
print("=" * 60)