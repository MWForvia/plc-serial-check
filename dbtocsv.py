#!/usr/bin/env python3
"""
dbtocsv.py

Exports rows from the `tn` table in the SQLite database to daily CSV files
both locally and to a USB-mounted directory. If anything goes wrong
(e.g. DB locked, USB unmounted, permission error), it logs the error,
waits, retries until it succeeds, and then backs up the DB file.
Saved files-
    CSV Exports (File name has yesterday's date appended)
        /home/gap900/csv_exports/YYYY-MM-DD.csv
        /media/usbdrive/csv_exports/YYYY-MM-DD.csv
    DB Backups (Current and dated)
        /home/gap900/db_backup/tndb900.db
        /media/usbdrive/db_backup/tndb900.db
        /home/gap900/db_backup/tndb900_YYYY-MM-DD.db
        /media/usbdrive/db_backup/tndb900_YYYY-MM-DD.db
"""

import sqlite3
import csv
import time
import os
import shutil
import logging
from datetime import datetime, timedelta

# Configuration
DB_PATH        = "/home/gap900/tndb900.db"
CSV_DIR        = "/home/gap900/csv_exports"
USB_CSV_DIR    = "/media/usbdrive/csv_exports"
DB_BACKUP_DIRS = ["/media/usbdrive/db_backup", "/home/gap900/db_backup"]
LOG_FILE       = "dbtocsv.log"
RETRY_DELAY    = 60   # seconds between retry attempts

# Configure logging
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def log_and_print(level: str, message: str) -> None:
    """
    Log at the given level and also print to stdout.
    """
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
    Connect to the SQLite DB, pull all rows from `tn`,
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
        filename = yesterday.strftime("%Y-%m-%d") + ".csv"

        # Local export
        local_path = os.path.join(CSV_DIR, filename)
        write_csv(local_path, rows)

        # USB export
        usb_path = os.path.join(USB_CSV_DIR, filename)
        write_csv(usb_path, rows)

        log_and_print(
            "info",
            f"Exported {len(rows)} entries to {local_path} and {usb_path}"
        )

    finally:
        conn.close()


def backup_db() -> None:
    """
    Copy the live DB file into each backup directory.
    Only overwrite the 'current' backup if the live DB has at least as many rows.
    Always create a date-stamped backup with today's date appended.
    """
    # Get row count from the live DB
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cur_rows = conn.execute("SELECT COUNT(*) FROM tn").fetchone()[0]
    except Exception as e:
        log_and_print("error", f"Failed to count rows in live DB: {e}")
        return

    # Prepare dated backup filename
    date_str = datetime.now().strftime("%Y-%m-%d")
    base = os.path.basename(DB_PATH)
    dated_name = base.replace(".db", f"_{date_str}.db")

    for d in DB_BACKUP_DIRS:
        try:
            os.makedirs(d, exist_ok=True)
            dst_current = os.path.join(d, base)
            dst_dated = os.path.join(d, dated_name)

            # Determine existing backup row count
            if os.path.exists(dst_current):
                with sqlite3.connect(dst_current) as bconn:
                    backup_rows = bconn.execute("SELECT COUNT(*) FROM tn").fetchone()[0]
            else:
                backup_rows = -1

            # Only overwrite current if live has >= rows
            if cur_rows >= backup_rows:
                shutil.copy2(DB_PATH, dst_current)
                log_and_print("info", f"DB current backup succeeded: {dst_current}")
                shutil.copy2(DB_PATH, dst_dated)
                log_and_print("info", f"DB dated backup succeeded: {dst_dated}")
            else:
                log_and_print(
                    "warning",
                    f"Live DB ({cur_rows} rows) has fewer rows than existing backup ({backup_rows}); skipping overwrite of {dst_current}"
                )
        except Exception as e:
            log_and_print("error", f"DB backup to {d} failed: {e}")


def main() -> None:
    """
    Keep trying export_csv() until it completes without error,
    then back up the DB before exiting.
    """
    while True:
        try:
            export_csv()
            log_and_print("info", "Export completed successfully.")
            backup_db()
            break
        except Exception:
            logging.exception("Export failed, will retry")
            time.sleep(RETRY_DELAY)

if __name__ == "__main__":
    main()
