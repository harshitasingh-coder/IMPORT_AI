"""Project configuration: paths, environment variables and data folders."""

import os
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
LOGS_DIR = DATA_DIR / "logs"
PROFILE_PATH = DATA_DIR / "product_profile.json"

load_dotenv(ROOT / ".env")

UN_COMTRADE_API_KEY = os.getenv("UN_COMTRADE_API_KEY", "").strip()


def ensure_data_dirs():
    """Create the data folders if they do not exist yet."""
    for folder in (RAW_DIR, PROCESSED_DIR, LOGS_DIR):
        folder.mkdir(parents=True, exist_ok=True)


def has_comtrade_key():
    return bool(UN_COMTRADE_API_KEY)
