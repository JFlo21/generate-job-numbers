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


def _ss_api_get(endpoint, params=None, max_retries=5):
    """Direct Smartsheet API GET for new endpoints not yet in the SDK.

    Includes retry/backoff for rate-limit (429) and transient errors
    (502/503/504) to match the resilience of the existing make_api_call
    wrappers used throughout the codebase.
    """
    token = (
        os.getenv("SMARTSHEET_API_TOKEN")
        or os.getenv("SMARTSHEET_ACCESS_TOKEN")
        or os.getenv("SMARTSHEET_TOKEN")
    )
    if not token or not str(token).strip():
        raise RuntimeError(
            "Smartsheet API token not found. Set one of "
            "SMARTSHEET_API_TOKEN, SMARTSHEET_ACCESS_TOKEN, or SMARTSHEET_TOKEN "
            "in the environment."
        )
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    url = f"{SMARTSHEET_API_BASE}/{endpoint}"
    base_delay = 2

    for attempt in range(max_retries):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=30)
        except (ConnectionError, TimeoutError, requests.exceptions.RequestException):
            if attempt < max_retries - 1:
                time.sleep(min(base_delay * (2 ** attempt), 30))
                continue
            raise

        if resp.status_code == 429:
            # Honor Retry-After header when present
            retry_after = resp.headers.get("Retry-After")
            try:
                wait_time = int(retry_after) if retry_after else min(base_delay * (2 ** attempt), 60)
            except (ValueError, TypeError):
                wait_time = min(base_delay * (2 ** attempt), 60)
            time.sleep(wait_time)
            continue

        if resp.status_code in (502, 503, 504):
            if attempt < max_retries - 1:
                time.sleep(min(base_delay * (2 ** attempt), 30))
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


def list_all_sheets(client, api_call_wrapper=None):
    """Replacement for client.Sheets.list_sheets(include_all=True).

    Uses page-based pagination and returns a plain list of sheet objects.
    Migrated from deprecated include_all=True — sunset June 3, 2026.

    Args:
        client: Smartsheet client instance.
        api_call_wrapper: Optional callable wrapping each SDK call with
            rate-limiting/retry logic (e.g. a script's ``make_api_call``).
            Signature: ``wrapper(func, *args, **kwargs)``.
    """
    call = api_call_wrapper if api_call_wrapper else lambda fn, *a, **kw: fn(*a, **kw)
    all_sheets = []
    page_number = 1
    while True:
        response = call(client.Sheets.list_sheets, page_size=100, page=page_number)
        if not response.data:
            break
        all_sheets.extend(response.data)
        if len(response.data) < 100:
            break
        page_number += 1
    return all_sheets


def list_all_workspaces(client, api_call_wrapper=None):
    """Replacement for client.Workspaces.list_workspaces(include_all=True).

    Uses page-based pagination and returns a plain list of workspace objects.
    Migrated from deprecated include_all=True — sunset June 3, 2026.

    Args:
        client: Smartsheet client instance.
        api_call_wrapper: Optional callable wrapping each SDK call with
            rate-limiting/retry logic (e.g. a script's ``make_api_call``).
            Signature: ``wrapper(func, *args, **kwargs)``.
    """
    call = api_call_wrapper if api_call_wrapper else lambda fn, *a, **kw: fn(*a, **kw)
    all_workspaces = []
    page_number = 1
    while True:
        response = call(client.Workspaces.list_workspaces, page_size=100, page=page_number)
        if not response.data:
            break
        all_workspaces.extend(response.data)
        if len(response.data) < 100:
            break
        page_number += 1
    return all_workspaces
