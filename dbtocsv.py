#!/usr/bin/env python3
"""
dbtocsv.py

Exports rows from the `tn` table in the SQLite database to daily CSV files
both locally and to a USB-mounted directory.  If anything goes wrong
(e.g. DB locked, USB unmounted, permission error), it logs the error,
waits, and retries until it succeeds.
"""

import sqlite3
import csv
import time
import os
import logging
from datetime import datetime, timedelta

# Configuration
DB_PATH       = "/home/gap900/tndb900.db"
CSV_DIR       = "/home/gap900/csv_exports"
USB_CSV_DIR   = "/media/usbdrive/csv_exports"
LOG_FILE      = "dbtocsv.log"
RETRY_DELAY   = 60   # seconds between retry attempts

# Configure logging
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def log_and_print(level: str, message: str) -> None:
    """Log at the given level and also print to stdout."""
    getattr(logging, level)(message)
    print(message)

def write_csv(path: str, rows: list[tuple]) -> None:
    """
    Write the given rows to a CSV file at `path`,
    creating parent directories if needed.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, mode="w", newline="") as f:
        writer = csv.writer(f, delimiter="\t")
        # Header
        writer.writerow([
            "id", "date", "finished_serial",
            "component_serial1", "component_serial2", "status"
        ])
        # Data
        writer.writerows(rows)
    log_and_print("info", f"CSV file written to {path}")

def export_csv() -> None:
    """
    Connect to the SQLite DB, pull all rows from `tn` for yesterday,
    and write them both locally and to the USB directory.
    Raises on any error so the caller can retry.
    """
    conn = sqlite3.connect(DB_PATH)
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, date, finished_serial, component_serial1, component_serial2, status "
            "FROM tn"
        )
        rows = cursor.fetchall()

        if not rows:
            log_and_print("info", "No entries to export.")
            return

        # Build filename for yesterday
        yesterday = datetime.now() - timedelta(days=1)
        filename  = yesterday.strftime("%Y-%m-%d") + ".csv"

        # Local export
        local_path = os.path.join(CSV_DIR, filename)
        write_csv(local_path, rows)

        # USB export
        usb_path = os.path.join(USB_CSV_DIR, filename)
        write_csv(usb_path, rows)

        log_and_print(
            "info",
            f"Exported {len(rows)} entries to "
            f"{local_path} and {usb_path}"
        )

    finally:
        conn.close()

def main() -> None:
    """
    Keep trying export_csv() until it completes without error.
    """
    while True:
        try:
            export_csv()
            log_and_print("info", "Export completed successfully.")
            break
        except Exception as e:
            logging.exception("Export failed, will retry")
            time.sleep(RETRY_DELAY)

if __name__ == "__main__":
    main()
