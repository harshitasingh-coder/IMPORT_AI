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

_PLACEHOLDERS = {"", "none", "null", "n/a", "na", "no", "false", "changeme", "your_key", "your-key", "todo", "xxx"}


def _key(name):
    """An API key, or '' when it is unset or a placeholder such as 'none'."""
    value = os.getenv(name, "").strip()
    return "" if value.lower() in _PLACEHOLDERS else value


UN_COMTRADE_API_KEY = _key("UN_COMTRADE_API_KEY")
# Optional: official Brave Search API key for automated buyer discovery.
BRAVE_SEARCH_API_KEY = _key("BRAVE_SEARCH_API_KEY")  # needs a card even on its free tier
TAVILY_API_KEY = _key("TAVILY_API_KEY")              # free: 1,000 searches/month, no card (tavily.com)
SERPER_API_KEY = _key("SERPER_API_KEY")              # free: 2,500 searches once, no card (serper.dev)


def ensure_data_dirs():
    """Create the data folders if they do not exist yet."""
    for folder in (RAW_DIR, PROCESSED_DIR, LOGS_DIR):
        folder.mkdir(parents=True, exist_ok=True)


def has_comtrade_key():
    return bool(UN_COMTRADE_API_KEY)
