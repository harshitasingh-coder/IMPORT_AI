"""Create the empty company-level dataset files with their headers.

Run:  python -m src.init_buyer_dataset

Existing files are never overwritten.
"""

import csv

from src.config import PROCESSED_DIR, ensure_data_dirs
from src.schemas import DATASETS


def main():
    ensure_data_dirs()
    for filename, fields in DATASETS.items():
        path = PROCESSED_DIR / filename
        if path.exists():
            print(f"Kept existing {path.name}")
            continue
        with path.open("w", newline="", encoding="utf-8") as file:
            csv.writer(file).writerow(fields)
        print(f"Created {path.name} ({len(fields)} columns)")


if __name__ == "__main__":
    main()
