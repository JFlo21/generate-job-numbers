#!/usr/bin/env python
"""Quick test to verify Smartsheet API token is working"""
import smartsheet
import sys

# Directly use the provided token for this test
API_TOKEN = "rmMxBWoAniodcEE77JatH9qfXPRXSxPzCiizx"

try:
    client = smartsheet.Smartsheet(API_TOKEN)
    client.errors_as_exceptions(True)
    
    # Try to get the current user info
    user = client.Users.get_current_user()
    print(f"✅ API Token is valid!")
    print(f"Connected as: {user.email}")
    print(f"Account type: {user.account.account_type}")
    
    # Try to list sheets
    sheets = client.Sheets.list_sheets(include_all=True)
    print(f"\n📊 Found {len(sheets.data)} sheets in your account")
    
    # Show a few sheet names
    print("\nFirst 5 sheets:")
    for sheet in sheets.data[:5]:
        print(f"  - {sheet.name} (ID: {sheet.id})")
        
    print("\n✨ API token is working perfectly! The optimized script should run successfully.")
    
except smartsheet.exceptions.ApiError as e:
    print(f"❌ API Error: {e.error.result}")
    print("\nPossible issues:")
    print("1. Token might be invalid or expired")
    print("2. Token might not have required permissions")
    sys.exit(1)
except Exception as e:
    print(f"❌ Error: {e}")
    sys.exit(1)