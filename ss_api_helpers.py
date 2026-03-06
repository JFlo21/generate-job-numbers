"""
Smartsheet API helpers for endpoints not yet available in the Python SDK.

These functions replace deprecated `include_all`/`load_all` parameters that
Smartsheet is sunsetting on June 3, 2026.
"""

import os
import time
import requests

SMARTSHEET_API_BASE = "https://api.smartsheet.com/2.0"


class _DictObj:
    """Wrap a dict so attribute access works like the old SDK model objects."""

    def __init__(self, d):
        self.__dict__.update(d)


def _ss_api_get(endpoint, params=None, max_retries=3):
    """Direct Smartsheet API GET for new endpoints not yet in the SDK."""
    token = (
        os.getenv("SMARTSHEET_API_TOKEN")
        or os.getenv("SMARTSHEET_ACCESS_TOKEN")
        or os.getenv("SMARTSHEET_TOKEN")
    )
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    url = f"{SMARTSHEET_API_BASE}/{endpoint}"

    for attempt in range(max_retries):
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        if resp.status_code == 429:
            time.sleep(2 ** attempt * 5)
            continue
        resp.raise_for_status()
        return resp.json()
    raise Exception(f"Failed after {max_retries} retries: {endpoint}")


def get_workspace_children(workspace_id, resource_types="sheets,folders"):
    """Replacement for get_workspace(id, load_all=True).

    Returns a dict with 'sheets' and 'folders' lists whose items are
    _DictObj instances so that .id and .name attribute access still works.
    """
    # Migrated from deprecated load_all=True — sunset June 3, 2026
    raw = _ss_api_get(
        f"workspaces/{workspace_id}/children",
        params={"childrenResourceTypes": resource_types},
    )
    sheets = [_DictObj(s) for s in raw.get("sheets", [])]
    folders = [_DictObj(f) for f in raw.get("folders", [])]
    result = _DictObj({"sheets": sheets, "folders": folders})
    return result


def get_folder_children(folder_id, resource_types="sheets,folders"):
    """Replacement for Folders.get_folder(id).

    Returns a _DictObj with .sheets and .folders lists whose items are
    _DictObj instances so that .id and .name attribute access still works.
    """
    # Migrated from deprecated get_folder SDK call — sunset June 3, 2026
    raw = _ss_api_get(
        f"folders/{folder_id}/children",
        params={"childrenResourceTypes": resource_types},
    )
    sheets = [_DictObj(s) for s in raw.get("sheets", [])]
    folders = [_DictObj(f) for f in raw.get("folders", [])]
    result = _DictObj({"sheets": sheets, "folders": folders})
    return result


def get_workspace_metadata(workspace_id):
    """Get workspace name/metadata only (no children).

    Returns a _DictObj so attribute access (e.g. .name) works consistently.
    """
    return _DictObj(_ss_api_get(f"workspaces/{workspace_id}/metadata"))


def list_all_sheets(client):
    """Replacement for client.Sheets.list_sheets(include_all=True).

    Uses page-based pagination and returns a plain list of sheet objects.
    Migrated from deprecated include_all=True — sunset June 3, 2026.
    """
    all_sheets = []
    page_number = 1
    while True:
        response = client.Sheets.list_sheets(page_size=100, page=page_number)
        if not response.data:
            break
        all_sheets.extend(response.data)
        if len(response.data) < 100:
            break
        page_number += 1
    return all_sheets


def list_all_workspaces(client):
    """Replacement for client.Workspaces.list_workspaces(include_all=True).

    Uses page-based pagination and returns a plain list of workspace objects.
    Migrated from deprecated include_all=True — sunset June 3, 2026.
    """
    all_workspaces = []
    page_number = 1
    while True:
        response = client.Workspaces.list_workspaces(page_size=100, page=page_number)
        if not response.data:
            break
        all_workspaces.extend(response.data)
        if len(response.data) < 100:
            break
        page_number += 1
    return all_workspaces
