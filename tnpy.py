#!/usr/bin/env python3
"""
tnpy.py

This script pulls the serial numbers scanned from the LH and RH converter and compares them to a historical database.
It returns if they are a repeat or not, then adds the data to the db.

Database: tndb900.db
Table: tn
Schema:
    id integer primary key autoincrement,
    date text,
    finished_serial text,
    component_serial1 text,
    component_serial2 text,
    status TEXT
"""

import argparse
import logging
import sys
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
import sqlite3
import time
import os
import shutil
from typing import Any

# Configure active log in home + daily rotated backups in ~/tnpy_logs
active_log = Path.home() / "tnpy.log"
backup_dir = Path.home() / "tnpy_logs"
backup_dir.mkdir(parents=True, exist_ok=True)
handler = TimedRotatingFileHandler(
    filename=str(active_log),
    when="midnight",
    interval=1,
    backupCount=0  # keep all rotated logs indefinitely
)
handler.suffix = "%Y-%m-%d"
# Place rotated files into backup_dir
handler.namer = lambda name: str(backup_dir / Path(name).name)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)

# Attempt to import PLC driver and error type
try:
    from pycomm3 import LogixDriver, CommError
except ImportError as e:
    logging.error(f"Required module pycomm3 not found: {e}")
    print(f"Required module pycomm3 not found: {e}", file=sys.stderr)
    sys.exit(1)

# Local and USB backup database paths
default_local_db = "/home/gap900/tndb900.db"
USB_DB_BACKUP  = "/media/usbdrive/db_backup/tndb900.db"

# PLC tags grouped for easy lookup
PLC_TAGS = {
    'LH_CONV': 'FIX_513D.Conv_Barcode.EXTRACT[2]',
    'RH_CONV': 'FIX_513D.Conv_Barcode_R.EXTRACT[2]',
    'DATASTORE': 'FIX_513D.Seq.Data_Store',
    'UNCLAMP': 'FIX_513D.Main.Unclamp_Part',
    'SEQ_STEP': 'SEQUENCE_STEP',
    'FINISHED_SERIAL': 'ZEBRA.Working_String[20]',
    'SCAN_COMPLETE': 'FIX_513D.Seq.Conv_Barcode_Passed',
    'TN_CHECK_PASS': 'TN_Check_Pass',
    'TN_CHECK_FAIL': 'TN_Check_Fail',
    'TN_DB_ERROR': 'TN_DB_Error'
}

# SQL statements
SQL_STATEMENTS = {
    'insert_tn': (
        "INSERT INTO tn "
        "(date, finished_serial, component_serial1, component_serial2, status) "
        "VALUES (?, ?, ?, ?, ?)"
    )
}

# Timing configuration
POLL_INTERVAL = 0.5   # seconds while online (inside connection)
RETRY_DELAY   = 10     # seconds between reconnect attempts when offline


def log_and_print(level: str, message: str) -> None:
    logging.log(getattr(logging, level.upper()), message)
    print(message)


def sync_db_from_backup(local_db: str) -> None:
    if not os.path.exists(USB_DB_BACKUP):
        return
    try:
        with sqlite3.connect(USB_DB_BACKUP) as usb_conn:
            usb_rows = usb_conn.execute("SELECT COUNT(*) FROM tn").fetchone()[0]
    except Exception:
        return
    local_rows = 0
    if os.path.exists(local_db):
        try:
            with sqlite3.connect(local_db) as loc_conn:
                local_rows = loc_conn.execute("SELECT COUNT(*) FROM tn").fetchone()[0]
        except Exception:
            return
    if usb_rows > local_rows:
        try:
            shutil.copy2(USB_DB_BACKUP, local_db)
            log_and_print('info', f"Synced local DB ({local_rows} rows) from USB ({usb_rows} rows)")
        except Exception as e:
            log_and_print('error', f"DB sync failed: {e}")


def wait_for_tag(plc: LogixDriver, tag_key: str) -> None:
    tag_name = PLC_TAGS[tag_key]
    while True:
        r = plc.read(tag_name)
        if r and not r.value:
            break
        time.sleep(POLL_INTERVAL)
    while True:
        r = plc.read(tag_name)
        if r and r.value:
            return
        time.sleep(POLL_INTERVAL)


def wait_for_datastore_or_reset(plc: LogixDriver) -> bool:
    while True:
        ds = plc.read(PLC_TAGS['DATASTORE'])
        rs = plc.read(PLC_TAGS['SEQ_STEP'])
        if rs and rs.value == 0:
            return False
        if ds and ds.value:
            return True
        time.sleep(POLL_INTERVAL)


def wait_for_fail_or_reset(plc: LogixDriver) -> bool:
    while True:
        fl = plc.read(PLC_TAGS['UNCLAMP'])
        rs = plc.read(PLC_TAGS['SEQ_STEP'])
        if rs and rs.value == 0:
            return False
        if fl and fl.value:
            return True
        time.sleep(POLL_INTERVAL)


def check_converter_sn(cursor: sqlite3.Cursor, column: str, sn: Any, label: str) -> bool:
    cursor.execute(f"SELECT 1 FROM tn WHERE {column} = ?", (sn,))
    if cursor.fetchone():
        log_and_print('warning', f"{label} Converter SN Failed: {sn}")
        return False
    log_and_print('info', f"{label} Converter SN Passed: {sn}")
    return True


def handle_fail(lh_pass: bool, rh_pass: bool, plc: LogixDriver,
                cursor: sqlite3.Cursor, lhconv: Any, rhconv: Any) -> None:
    if not lh_pass and not rh_pass:
        status = "LH & RH Converter SN Duplicated - Failed"
    elif not lh_pass:
        status = "LH Converter SN Duplicated - Failed"
    else:
        status = "RH Converter SN Duplicated - Failed"
    log_and_print('error', status)
    if wait_for_fail_or_reset(plc):
        ts = time.strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute(SQL_STATEMENTS['insert_tn'], (ts, 'N/A', lhconv, rhconv, status))
        cursor.connection.commit()
        log_and_print('error', "Failed TN Check - Data stored in database")


def monitor_and_update(plc_ip_address: str, db_file: str) -> None:
    while True:
        sync_db_from_backup(db_file)
        try:
            log_and_print('info', f"Attempting connection to PLC at {plc_ip_address}")
            with LogixDriver(plc_ip_address) as plc:
                log_and_print('info', "PLC connection established.")
                while True:
                    wait_for_tag(plc, 'SCAN_COMPLETE')
                    with sqlite3.connect(db_file) as conn:
                        cursor = conn.cursor()
                        try:
                            lhconv = plc.read(PLC_TAGS['LH_CONV']).value
                            rhconv = plc.read(PLC_TAGS['RH_CONV']).value
                            lh_pass = check_converter_sn(cursor, 'component_serial1', lhconv, 'LH')
                            rh_pass = check_converter_sn(cursor, 'component_serial2', rhconv, 'RH')

                            if lh_pass and rh_pass:
                                res = plc.write((PLC_TAGS['TN_CHECK_PASS'], True))
                                if not res or getattr(res, 'error', False):
                                    log_and_print('error', f"Write TN_CHECK_PASS failed: {res}")
                                res = plc.write((PLC_TAGS['TN_CHECK_FAIL'], False))
                                if not res or getattr(res, 'error', False):
                                    log_and_print('error', f"Write TN_CHECK_FAIL failed: {res}")
                                log_and_print('info', "TN Check Passed")

                                if wait_for_datastore_or_reset(plc):
                                    finished_serial = plc.read(PLC_TAGS['FINISHED_SERIAL']).value
                                    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
                                    cursor.execute(
                                        SQL_STATEMENTS['insert_tn'],
                                        (timestamp, finished_serial, lhconv, rhconv, 'Passed')
                                    )
                                    conn.commit()
                                    log_and_print('info', "Data stored in database")
                            else:
                                plc.write((PLC_TAGS['TN_CHECK_PASS'], False))
                                plc.write((PLC_TAGS['TN_CHECK_FAIL'], True))
                                handle_fail(lh_pass, rh_pass, plc, cursor, lhconv, rhconv)
                        except CommError as e_comm_inner:
                            log_and_print('error', f"Lost PLC connection during processing: {e_comm_inner}")
                            break
                        except Exception as e_inner:
                            import traceback
                            log_and_print('error', f"Error during PLC processing: {e_inner}\n{traceback.format_exc()}")
                            plc.write((PLC_TAGS['TN_DB_ERROR'], True))
        except KeyboardInterrupt:
            log_and_print('info', "Interrupted by user, exiting.")
            sys.exit(0)
        except CommError as e_comm:
            log_and_print('error', f"CommError connecting to PLC ({plc_ip_address}): {e_comm}. Retrying in {RETRY_DELAY}s.")
        except Exception as e:
            import traceback
            log_and_print('error', f"Unexpected error in monitor_and_update: {e}\n{traceback.format_exc()}\nRetrying in {RETRY_DELAY}s.")
        time.sleep(RETRY_DELAY)


def main() -> None:
    parser = argparse.ArgumentParser(description="TN barcode converter serial checker")
    parser.add_argument("--plc", default="10.131.201.60", help="IP address of the Allen-Bradley PLC")
    parser.add_argument("--db", default=default_local_db, help="Path to the SQLite database file")
    args = parser.parse_args()
    db_file = os.path.expanduser(args.db)
    log_and_print('info', f"Starting tnpy: PLC={args.plc}, DB={db_file}")
    sync_db_from_backup(db_file)
    monitor_and_update(args.plc, db_file)

if __name__ == "__main__":
    main()
