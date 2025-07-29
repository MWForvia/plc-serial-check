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
    Export all rows from 'tn' table to daily CSV files in local and USB directories.
    """
    yesterday = datetime.now() - timedelta(days=1)
    filename = yesterday.strftime("%Y-%m-%d") + ".csv"

    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            # export the entire table each day using new 17-column schema
            cursor.execute(
                "SELECT id, date, tla1_pn, tla1_tn, tla1_vpps, tla1_duns, "
                "conv1_pn, conv1_tn, conv1_vpps, conv1_duns, "
                "tla2_pn, tla2_tn, tla2_vpps, tla2_duns, "
                "conv2_pn, conv2_tn, conv2_vpps, conv2_duns "
                "FROM tn"
            )

            rows = cursor.fetchall()
        if not rows:
            logger.debug("No entries to export.")
            return

        for target_dir in (CSV_DIR, USB_CSV_DIR, USB2_CSV_DIR):
            path = Path(target_dir) / filename
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, mode="w", newline="") as f:
                writer = csv.writer(f, delimiter="\t")
                # write header for 17 columns
                writer.writerow([
                    "id", "date",
                    "tla1_pn", "tla1_tn", "tla1_vpps", "tla1_duns",
                    "conv1_pn", "conv1_tn", "conv1_vpps", "conv1_duns",
                    "tla2_pn", "tla2_tn", "tla2_vpps", "tla2_duns",
                    "conv2_pn", "conv2_tn", "conv2_vpps", "conv2_duns"
                ])
                writer.writerows(rows)
            logger.info(f"CSV file written to {path}")

        logger.info(f"Exported {len(rows)} entries to both CSV locations")
    except Exception:
        logger.exception("export_csv failed")


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
                    "tla1_pn TEXT, tla1_tn TEXT, tla1_vpps TEXT, tla1_duns TEXT, "
                    "conv1_pn TEXT, conv1_tn TEXT, conv1_vpps TEXT, conv1_duns TEXT, "
                    "tla2_pn TEXT, tla2_tn TEXT, tla2_vpps TEXT, tla2_duns TEXT, "
                    "conv2_pn TEXT, conv2_tn TEXT, conv2_vpps TEXT, conv2_duns TEXT"
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
                        "INSERT INTO tn(date, tla1_pn, tla1_tn, tla1_vpps, tla1_duns, "
                        "conv1_pn, conv1_tn, conv1_vpps, conv1_duns, "
                        "tla2_pn, tla2_tn, tla2_vpps, tla2_duns, "
                        "conv2_pn, conv2_tn, conv2_vpps, conv2_duns) "
                        "SELECT date, tla1_pn, tla1_tn, tla1_vpps, tla1_duns, "
                        "conv1_pn, conv1_tn, conv1_vpps, conv1_duns, "
                        "tla2_pn, tla2_tn, tla2_vpps, tla2_duns, "
                        "conv2_pn, conv2_tn, conv2_vpps, conv2_duns "
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
