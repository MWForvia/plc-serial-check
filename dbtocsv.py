#!/usr/bin/env python3
"""
dbtocsv.py

Exports rows from the `tn` table in the SQLite database to daily CSV files
both locally and to a USB-mounted directory. If anything goes wrong
(e.g. DB locked, USB unmounted, permission error), it logs the error,
waits, retries until it succeeds, and then backs up the DB file.

Daily logs are kept in ~/dbtocsv_logs/ with date suffix; active log lives at ~/dbtocsv.log.
"""

import sqlite3
import csv
import time
import os
import shutil
import sys
from datetime import datetime, timedelta
from pathlib import Path
import logging
from logging.handlers import TimedRotatingFileHandler

# Configuration
DB_PATH        = "/home/gap900/tndb900.db"
CSV_DIR        = "/home/gap900/csv_exports"
USB_CSV_DIR    = "/media/usbdrive/csv_exports"
DB_BACKUP_DIRS = ["/media/usbdrive/db_backup", "/home/gap900/db_backup"]
RETRY_DELAY    = 60   # seconds between retry attempts

# Setup logging: active log at ~/dbtocsv.log, rotated daily into ~/dbtocsv_logs
active_log = Path.home() / "dbtocsv.log"
backup_dir = Path.home() / "dbtocsv_logs"
backup_dir.mkdir(parents=True, exist_ok=True)
handler = TimedRotatingFileHandler(
    filename=str(active_log),
    when="midnight",
    interval=1,
    backupCount=0    # keep all logs indefinitely
)
handler.suffix = "%Y-%m-%d"
handler.namer = lambda name: str(backup_dir / Path(name).name)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)


def log_and_print(level: str, message: str) -> None:
    """
    Log at the given level and also print to stdout.
    """
    # Use numeric level lookup and log via logging.log()
    level_num = getattr(logging, level.upper(), logging.INFO)
    logging.log(level_num, message)
    print(message)


def write_csv(path: str, rows: list[tuple]) -> None:
    """
    Write the given rows to a CSV file at `path`,
    creating parent directories if needed.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, mode="w", newline="") as f:
        writer = csv.writer(f, delimiter="\t")
        writer.writerow(["id", "date", "finished_serial", "component_serial1", "component_serial2", "status"])
        writer.writerows(rows)
    log_and_print("info", f"CSV file written to {path}")


def export_csv() -> None:
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
        yesterday = datetime.now() - timedelta(days=1)
        filename = yesterday.strftime("%Y-%m-%d") + ".csv"
        local_path = os.path.join(CSV_DIR, filename)
        write_csv(local_path, rows)
        usb_path = os.path.join(USB_CSV_DIR, filename)
        write_csv(usb_path, rows)
        log_and_print("info", f"Exported {len(rows)} entries to {local_path} and {usb_path}")
    finally:
        conn.close()


def backup_db() -> None:
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cur_rows = conn.execute("SELECT COUNT(*) FROM tn").fetchone()[0]
    except Exception as e:
        log_and_print("error", f"Failed to count rows in live DB: {e}")
        return
    date_str = datetime.now().strftime("%Y-%m-%d")
    base = os.path.basename(DB_PATH)
    dated = base.replace(".db", f"_{date_str}.db")
    for d in DB_BACKUP_DIRS:
        try:
            os.makedirs(d, exist_ok=True)
            dst_cur = os.path.join(d, base)
            dst_dat = os.path.join(d, dated)
            if os.path.exists(dst_cur):
                with sqlite3.connect(dst_cur) as bconn:
                    prev_rows = bconn.execute("SELECT COUNT(*) FROM tn").fetchone()[0]
            else:
                prev_rows = -1
            if cur_rows >= prev_rows:
                shutil.copy2(DB_PATH, dst_cur)
                log_and_print("info", f"DB current backup succeeded: {dst_cur}")
                shutil.copy2(DB_PATH, dst_dat)
                log_and_print("info", f"DB dated backup succeeded: {dst_dat}")
            else:
                log_and_print("warning", f"Live DB ({cur_rows}) < backup ({prev_rows}); skipped {dst_cur}")
        except Exception as e:
            log_and_print("error", f"DB backup to {d} failed: {e}")


def main() -> None:
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
