"""Find which workspace contains the Resiliency Promax Database sheets"""
import os
import smartsheet
from ss_api_helpers import list_all_workspaces, get_workspace_children

API_TOKEN = os.getenv("SMARTSHEET_API_TOKEN")
client = smartsheet.Smartsheet(API_TOKEN)

print("=" * 60)
print("SEARCHING FOR RESILIENCY PROMAX DATABASE SHEETS")
print("=" * 60)

# Known Resiliency Promax Database sheet IDs from earlier
known_resiliency_sheet_ids = [
    3239244454645636,  # Resiliency Promax Database
    2230129632694148,  # Resiliency Promax Database Backup 2
    1732945426468740,  # Resiliency Promax Database Backup 3
    4126460034895748,  # Resiliency Promax Database Backup 4
]

try:
    print("\nChecking workspace for known Resiliency Promax Database sheets...")
    
    # Get all workspaces
    # Migrated from deprecated include_all=True — sunset June 3, 2026
    all_workspaces = list_all_workspaces(client)
    print(f"Found {len(all_workspaces)} workspaces total\n")
    
    workspace_found = None
    
    for workspace_info in all_workspaces:
        try:
            # Get workspace with sheets
            # Migrated from deprecated load_all=True — sunset June 3, 2026
            workspace = get_workspace_children(workspace_info.id, resource_types="sheets")
            
            if workspace.sheets:
                # Check if any known sheet is in this workspace
                sheet_ids_in_workspace = set(sheet.id for sheet in workspace.sheets)
                matching_sheets = [sid for sid in known_resiliency_sheet_ids if sid in sheet_ids_in_workspace]
                
                if matching_sheets:
                    workspace_found = workspace_info.id
                    print(f"✅ FOUND! Workspace: {workspace_info.name}")
                    print(f"   Workspace ID: {workspace_info.id}")
                    print(f"   Contains {len(matching_sheets)} known Resiliency Promax Database sheets")
                    
                    # List all Resiliency Promax Database sheets in this workspace
                    resiliency_sheets = [s for s in workspace.sheets 
                                       if 'resiliency promax database' in s.name.lower()]
                    if resiliency_sheets:
                        print(f"\n   Total Resiliency Promax Database sheets in workspace: {len(resiliency_sheets)}")
                        for sheet in resiliency_sheets[:10]:  # Show first 10
                            print(f"     - {sheet.name}")
                        if len(resiliency_sheets) > 10:
                            print(f"     ... and {len(resiliency_sheets) - 10} more")
                    break
                    
        except Exception as e:
            # Skip workspaces we can't access
            pass
    
    if not workspace_found:
        print("❌ Could not find the workspace containing Resiliency Promax Database sheets")
        print("   The sheets may be in your personal 'Sheets' folder or a workspace you don't have access to")
        
except Exception as e:
    print(f"Error: {e}")

print("\n" + "=" * 60)
