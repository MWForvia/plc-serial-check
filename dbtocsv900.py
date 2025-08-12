#!/usr/bin/env python3
"""
dbtocsv.py

Exports rows from the `tn` table in the SQLite database to daily CSV files
both locally and to a USB-mounted directory. If anything goes wrong
(e.g. DB locked, USB unmounted, permission error), it logs the error,
waits, retries until it succeeds, and then backs up the DB file.

Daily logs are kept in ~/dbtocsv_logs/ with date suffix; active log lives at ~/dbtocsv.log.

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
from datetime import datetime, timedelta
from pathlib import Path
import logging
from logging.handlers import TimedRotatingFileHandler
import traceback  # keep for exception logging
import sys
import errno

# Helper to detect a real mount
def is_mounted(path: str) -> bool:
    return os.path.ismount(os.path.dirname(path))

# Configuration
DB_PATH        = "/home/gap900/tndb900.db"
CSV_DIR        = "/home/gap900/csv_exports900"
USB_CSV_DIR    = "/media/usbdrive/csv_exports900"
USB2_CSV_DIR   = "/media/usbdrive2/csv_exports900"
DB_BACKUP_DIRS = ["/media/usbdrive/db_backup900", "/media/usbdrive2/db_backup900", "/home/gap900/db_backup900"]
RETRY_DELAY    = 60   # seconds between retry attempts

# directory where rotated logs live
LOG_BACKUP_DIR = Path.home() / "dbtocsv_logs900"
LOG_BACKUP_DIR.mkdir(parents=True, exist_ok=True)

# Setup logging: active log at ~/dbtocsv.log, rotated daily into ~/dbtocsv_logs
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

# INFO handler: only file‐save successes
info_log     = Path.home() / "dbtocsv900.log"
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
debug_log     = Path.home() / "dbtocsv_debug900.log"
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


# Ensure database and 'tn' table exist
def ensure_db_schema(db_path: str) -> None:
    """
    Create the database file and the 'tn' table if they do not exist.
    """
    parent = os.path.dirname(db_path) or '.'
    try:
        os.makedirs(parent, exist_ok=True)
    except Exception as e:
        logger.error("Failed to create directory for DB %s: %s", parent, e)
        sys.exit(1)
    try:
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS tn (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT,
                    finished_serial TEXT,
                    component_serial1 TEXT,
                    component_serial1_date TEXT,
                    component_serial2 TEXT,
                    component_serial2_date TEXT,
                    status TEXT
                )
                """
            )
            conn.commit()
            logger.debug("Ensured schema for database %s", db_path)
    except Exception as e:
        logger.error("Failed to create database schema on %s: %s", db_path, e)
        sys.exit(1)


def export_csv() -> None:
    """
    Export all rows from 'tn' table to daily CSV files in local and USB directories.
    """
    try:
        yesterday = datetime.now() - timedelta(days=1)
        filename = yesterday.strftime("%Y-%m-%d") + ".csv"

        # fetch rows
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id, date, finished_serial, component_serial1, component_serial1_date, component_serial2, component_serial2_date, status "
                "FROM tn"
            )
            rows = cursor.fetchall()
        if not rows:
            logger.debug("No entries to export.")
            return

        # write yesterday's CSV locally
        local_path = Path(CSV_DIR) / filename
        local_path.parent.mkdir(parents=True, exist_ok=True)
        with open(local_path, mode="w", newline="") as f:
            writer = csv.writer(f, delimiter="\t")
            writer.writerow([
                "id", "date", "finished_serial",
                "component_serial1", "component_serial1_date", "component_serial2", "component_serial2_date", "status"
            ])
            writer.writerows(rows)
        logger.info(f"CSV file written to {local_path}")

        # backfill any missing CSVs to mounted USB drives
        for csv_file in Path(CSV_DIR).glob("*.csv"):
            for target_dir in (USB_CSV_DIR, USB2_CSV_DIR):
                if not is_mounted(target_dir):
                    continue
                dest = Path(target_dir) / csv_file.name
                dest.parent.mkdir(parents=True, exist_ok=True)
                if not dest.exists():
                    shutil.copy2(str(csv_file), str(dest))
                    logger.info(f"Backfilled {csv_file.name} to {dest}")
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
        # skip unmounted USB backup locations (check parent mountpoint)
        if d.startswith("/media") and not os.path.ismount(os.path.dirname(d)):
            logger.debug(f"Skipping DB backup to unmounted {d}")
            continue
        try:
            os.makedirs(d, exist_ok=True)
            backup_path = Path(d) / base

            # If no backup exists, copy entire DB
            if not backup_path.exists():
                shutil.copy2(DB_PATH, str(backup_path))
                logger.info(f"Initial DB backup created: {backup_path}")
                continue
            # Check integrity of existing backup, replace if corrupted
            try:
                with sqlite3.connect(str(backup_path)) as chk_conn:
                    result = chk_conn.execute("PRAGMA integrity_check;").fetchone()[0]
                if result != "ok":
                    logger.warning(f"Integrity check failed for {backup_path}: {result}, replacing with live DB")
                    shutil.copy2(DB_PATH, str(backup_path))
                    logger.info(f"Replaced corrupted backup DB at {backup_path}")
                    continue
            except Exception:
                logger.exception(f"Integrity check exception for backup DB {backup_path}")
                try:
                    shutil.copy2(DB_PATH, str(backup_path))
                    logger.info(f"Replaced backup DB after exception at {backup_path}")
                    continue
                except Exception:
                    logger.exception(f"Failed to replace backup DB {backup_path} after integrity exception")

            # Open backup DB and append only new entries
            with sqlite3.connect(str(backup_path)) as bconn:
                bcursor = bconn.cursor()
                # Ensure table exists
                bcursor.execute(
                    "CREATE TABLE IF NOT EXISTS tn ("
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                    "date TEXT, finished_serial TEXT, "
                    "component_serial1 TEXT, component_serial1_date TEXT, "
                    "component_serial2 TEXT, component_serial2_date TEXT, "
                    "status TEXT)"
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
                        "INSERT INTO tn(date, finished_serial, component_serial1, component_serial1_date, component_serial2, component_serial2_date, status) "
                        "SELECT date, finished_serial, component_serial1, component_serial1_date, component_serial2, component_serial2_date, status "
                        "FROM src.tn WHERE id > ?", (max_id,)
                    )
                bconn.execute("DETACH DATABASE src")
                bconn.commit()
                logger.info(f"Appended {new_count} new rows to backup DB: {backup_path}")

        except Exception:
            logger.exception(f"Incremental DB backup to {d} failed")


def main() -> None:
    # create DB file and schema if missing, and ensure CSV/backup dirs exist
    ensure_db_schema(DB_PATH)
    # ensure CSV export directory exists
    os.makedirs(CSV_DIR, exist_ok=True)
    # ensure DB backup directories exist (skip unmounted USB paths)
    for d in DB_BACKUP_DIRS:
        # local paths always created; skip USB if unmounted
        if d.startswith("/media") and not is_mounted(d):
            logger.debug(f"Skipping directory creation for unmounted path: {d}")
            continue
        try:
            os.makedirs(d, exist_ok=True)
        except OSError as e:
            # suppress common 'no such device' / mount errors
            if e.errno in (errno.ENODEV, errno.ENOENT):
                logger.debug(f"Mount disappeared before creating {d}; skipping")
            else:
                logger.error(f"Failed to create directory {d}: {e}")
    while True:
        try:
            export_csv()
            # individual file‐save messages are logged in write_csv/export_csv
            backup_db()
            # Weekly optimize on Sunday
            if datetime.now().weekday() == 6:
                try:
                    with sqlite3.connect(DB_PATH) as conn:
                        conn.execute("PRAGMA optimize;")
                    logger.info("Weekly PRAGMA optimize run on database")
                except Exception:
                    logger.exception("Weekly PRAGMA optimize failed")
            # final success message
            logger.info("Export completed successfully.")
            break
        except Exception:
            logger.info(f"Export failed, will retry: {traceback.format_exc()}")
            time.sleep(RETRY_DELAY)

if __name__ == "__main__":
    main()
