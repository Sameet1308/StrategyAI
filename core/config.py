"""
Configuration loader — reads Strategy server settings from .env file.
Supports dual-server setup: PROD (source) and DEV (target).
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from project root
load_dotenv(Path(__file__).resolve().parent.parent / ".env")


def get_prod_config():
    """Return PROD Strategy connection config (source for lineage extraction)."""
    base_url = os.getenv("MSTR_PROD_BASE_URL")
    if not base_url:
        raise ValueError("MSTR_PROD_BASE_URL not set in .env file.")
    return {
        "base_url": base_url.rstrip("/"),
        "username": os.getenv("MSTR_USERNAME"),
        "password": os.getenv("MSTR_PASSWORD"),
        "verify_ssl": False,
    }


def get_dev_config():
    """Return DEV Strategy connection config (target for cube publishing)."""
    base_url = os.getenv("MSTR_DEV_BASE_URL")
    if not base_url:
        raise ValueError("MSTR_DEV_BASE_URL not set in .env file.")
    return {
        "base_url": base_url.rstrip("/"),
        "username": os.getenv("MSTR_USERNAME"),
        "password": os.getenv("MSTR_PASSWORD"),
        "project_id": os.getenv("MSTR_DEV_PROJECT_ID", ""),
        "folder_id": os.getenv("MSTR_DEV_FOLDER_ID", ""),
        "verify_ssl": False,
    }


def get_prod_project_ids():
    """Return list of PROD project IDs to process."""
    raw = os.getenv("MSTR_PROD_PROJECT_IDS", "")
    return [pid.strip() for pid in raw.split(",") if pid.strip()]


# Keep backward compat for simple single-server scripts
def get_config():
    """Return single-server config (legacy). Falls back to PROD config."""
    try:
        return get_prod_config()
    except ValueError:
        pass
    base_url = os.getenv("MSTR_BASE_URL")
    if not base_url:
        raise ValueError("MSTR_BASE_URL (or MSTR_PROD_BASE_URL) not set in .env file.")
    return {
        "base_url": base_url.rstrip("/"),
        "username": os.getenv("MSTR_USERNAME"),
        "password": os.getenv("MSTR_PASSWORD"),
        "project_id": os.getenv("MSTR_PROJECT_ID", ""),
        "verify_ssl": os.getenv("MSTR_VERIFY_SSL", "true").lower() == "true",
    }
