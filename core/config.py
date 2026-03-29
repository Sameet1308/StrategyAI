"""
Configuration loader — reads Strategy server settings from .env file.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from project root
load_dotenv(Path(__file__).resolve().parent.parent / ".env")


def get_config():
    """Return Strategy connection config from environment variables."""
    base_url = os.getenv("MSTR_BASE_URL")
    if not base_url:
        raise ValueError("MSTR_BASE_URL not set in .env file. Copy .env.example to .env and fill in your values.")

    return {
        "base_url": base_url.rstrip("/"),
        "username": os.getenv("MSTR_USERNAME"),
        "password": os.getenv("MSTR_PASSWORD"),
        "project_id": os.getenv("MSTR_PROJECT_ID", ""),
        "verify_ssl": os.getenv("MSTR_VERIFY_SSL", "true").lower() == "true",
    }
