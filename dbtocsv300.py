#!/usr/bin/env python3
"""
dbtocsv300.py

Exports rows from the `tn` table in the SQLite database to daily CSV files
both locally and to a USB-mounted directory. If anything goes wrong
(e.g. DB locked, USB unmounted, permission error), it logs the error,
waits, retries until it succeeds, and then backs up the DB file.

Daily logs are kept in ~/dbtocsv_logs300/ with date suffix; active log lives at ~/dbtocsv300.log.

Saved files-
    CSV Exports (File name has yesterday's date appended)
        /home/gap300/csv_exports300/YYYY-MM-DD.csv
        /media/usbdrive/csv_exports300/YYYY-MM-DD.csv
    DB Backups (Current and dated)
        /home/gap300/db_backup300/tndb300.db
        /media/usbdrive/db_backup300/tndb300.db
        /home/gap300/db_backup300/tndb300_YYYY-MM-DD.db
        /media/usbdrive/db_backup300/tndb300_YYYY-MM-DD.db
"""

import sqlite3
import csv
import time
import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path
import logging
from logging.handlers import TimedRotatingFileHandler
import traceback  # keep for exception logging
 
# Helper to detect real mount (walk parents and check mount points)
def is_mounted(path: str) -> bool:
    p = str(path)
    # if path itself is a mount point, return True
    if os.path.ismount(p):
        return True
    # walk up parents until root
    while p and p != os.path.sep:
        if os.path.ismount(p):
            return True
        p = os.path.dirname(p)
    return False

# Configuration
DB_PATH        = "/home/gap300/tndb300.db"
CSV_DIR        = "/home/gap300/csv_exports300"
USB_CSV_DIR    = "/media/usbdrive/csv_exports300"
USB2_CSV_DIR   = "/media/usbdrive2/csv_exports300"
DB_BACKUP_DIRS = ["/media/usbdrive/db_backup300", "/media/usbdrive2/db_backup300", "/home/gap300/db_backup300"]
RETRY_DELAY    = 60   # seconds between retry attempts

# directory where rotated logs live
LOG_BACKUP_DIR = Path.home() / "dbtocsv_logs300"
LOG_BACKUP_DIR.mkdir(parents=True, exist_ok=True)

# Setup logging: active log at ~/dbtocsv300.log, rotated daily into ~/dbtocsv_logs300
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

# INFO handler: only file‐save successes
info_log     = Path.home() / "dbtocsv300.log"
info_handler = TimedRotatingFileHandler(
    filename=str(info_log),
    when="midnight",
    interval=1,
    backupCount=0
)
info_handler.suffix    = "%Y-%m-%d"
info_handler.namer     = lambda name: str(LOG_BACKUP_DIR / Path(name).name)
info_handler.setLevel  (logging.INFO)
info_handler.setFormatter(formatter)

# DEBUG handler: all other statuses, retries, errors
debug_log     = Path.home() / "dbtocsv_debug300.log"
debug_handler = TimedRotatingFileHandler(
    filename=str(debug_log),
    when="midnight",
    interval=1,
    backupCount=0
)
debug_handler.suffix    = "%Y-%m-%d"
debug_handler.namer     = lambda name: str(LOG_BACKUP_DIR / Path(name).name)
debug_handler.setLevel  (logging.DEBUG)
debug_handler.setFormatter(formatter)

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.addHandler(info_handler)
logger.addHandler(debug_handler)


def export_csv() -> None:
    """
    Export the entire `tn` table to a dated CSV file and write it to local
    and mounted USB targets. The CSV filename uses yesterday's date
    (YYYY-MM-DD.csv) but contains the full table contents each run.
    """
    yesterday = datetime.now() - timedelta(days=1)
    filename = yesterday.strftime("%Y-%m-%d") + ".csv"

    # Read full table using the tnpy300 schema
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id, date, tla1, tla1_date, conv1, conv1_date, tla2, tla2_date, conv2, conv2_date, status FROM tn"
            )
            rows = cursor.fetchall()
    except Exception:
        logger.exception("export_csv DB read failed")
        return

    if not rows:
        logger.debug("No entries to export.")
        return

    header = [
        "id", "date",
        "tla1", "tla1_date", "conv1", "conv1_date",
        "tla2", "tla2_date", "conv2", "conv2_date",
        "status",
    ]

    targets = (CSV_DIR, USB_CSV_DIR, USB2_CSV_DIR)
    for target_dir in targets:
        # skip unmounted USB directories
        if target_dir.startswith("/media") and not is_mounted(target_dir):
            logger.debug(f"Skipping CSV export to unmounted {target_dir}")
            continue
        path = Path(target_dir) / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(path, mode="w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(header)
                writer.writerows(rows)
            logger.info(f"CSV file written to {path}")
        except Exception:
            logger.exception(f"Failed to write CSV to {path}")


def backup_db() -> None:
    """
    Incrementally append new rows from the live DB to each backup DB
    instead of copying the entire file each time.
    """
    try:
        with sqlite3.connect(DB_PATH) as conn_live:
            total_rows = conn_live.execute("SELECT COUNT(*) FROM tn").fetchone()[0]
    except Exception:
        logger.exception("Failed to count rows in live DB")
        return

    base = os.path.basename(DB_PATH)
    for d in DB_BACKUP_DIRS:
        try:
            # skip unmounted usb dirs
            if d.startswith("/media") and not os.path.ismount(d):
                logger.debug(f"Skipping DB backup to unmounted {d}")
                continue
            os.makedirs(d, exist_ok=True)
            backup_path = Path(d) / base

            # If no backup exists, copy entire DB
            if not backup_path.exists():
                shutil.copy2(DB_PATH, str(backup_path))
                logger.info(f"Initial DB backup created: {backup_path}")
                continue

            # Open backup DB and append only new entries
            with sqlite3.connect(str(backup_path)) as bconn:
                bcursor = bconn.cursor()
                # Ensure table exists
                bcursor.execute(
                    "CREATE TABLE IF NOT EXISTS tn ("
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                    "date TEXT, "
                    "tla1 TEXT, tla1_date TEXT, conv1 TEXT, conv1_date TEXT, "
                    "tla2 TEXT, tla2_date TEXT, conv2 TEXT, conv2_date TEXT, "
                    "status TEXT"
                    ")"
                )
                # Find max ID in backup
                bcursor.execute("SELECT MAX(id) FROM tn")
                max_id = bcursor.fetchone()[0] or 0

                # Attach live DB and count new rows
                bconn.execute("ATTACH DATABASE ? AS src", (DB_PATH,))
                cur = bconn.execute(
                    "SELECT COUNT(*) FROM src.tn WHERE id > ?", (max_id,)
                )
                new_count = cur.fetchone()[0]
                if new_count > 0:
                    bconn.execute(
                        "INSERT INTO tn(date, tla1, tla1_date, conv1, conv1_date, tla2, tla2_date, conv2, conv2_date, status) "
                        "SELECT date, tla1, tla1_date, conv1, conv1_date, tla2, tla2_date, conv2, conv2_date, status "
                        "FROM src.tn WHERE id > ?", (max_id,)
                    )
                bconn.execute("DETACH DATABASE src")
                bconn.commit()
                logger.info(f"Appended {new_count} new rows to backup DB: {backup_path}")

        except Exception:
            logger.exception(f"Incremental DB backup to {d} failed")


def main() -> None:
    while True:
        try:
            export_csv()
            # individual file‐save messages are logged in write_csv/export_csv
            backup_db()
            # final success message
            logger.info("Export completed successfully.")
            break
        except Exception:
            logger.info(f"Export failed, will retry: {traceback.format_exc()}")
            time.sleep(RETRY_DELAY)

if __name__ == "__main__":
    main()
