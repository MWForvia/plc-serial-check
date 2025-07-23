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
import threading
import sqlite3
import time
import os
import shutil
from pathlib import Path
from typing import Any

# --- Logging setup ---
log_dir = Path.home() / "tnpy_logs"
log_dir.mkdir(parents=True, exist_ok=True)

# INFO handler
info_log = Path.home() / "tnpy.log"
info_handler = TimedRotatingFileHandler(
    filename=str(info_log), when="midnight", interval=1, backupCount=0
)
info_handler.suffix = "%Y-%m-%d"
info_handler.namer = lambda name: str(log_dir / Path(name).name)
info_handler.setLevel(logging.INFO)
info_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

# DEBUG handler
debug_log = Path.home() / "tnpy_debug.log"
debug_handler = TimedRotatingFileHandler(
    filename=str(debug_log), when="midnight", interval=1, backupCount=0
)
debug_handler.suffix = "%Y-%m-%d"
debug_handler.namer = lambda name: str(log_dir / Path(name).name)
debug_handler.setLevel(logging.DEBUG)
debug_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(info_handler)
logger.addHandler(debug_handler)

# --- PLC driver import ---
try:
    from pycomm3 import LogixDriver, CommError
except ImportError as e:
    logger.error("Required module pycomm3 not found: %s", e)
    sys.exit(1)

# --- Constants ---
default_local_db = "/home/gap900/tndb900.db"
USB_DB_BACKUP   = "/media/usbdrive/db_backup/tndb900.db"

PLC_TAGS = {
    'PI_HEARTBEAT':    'PI_Heartbeat',
    'LH_CONV':         'FIX_513D.Conv_Barcode.EXTRACT[2]',
    'RH_CONV':         'FIX_513D.Conv_Barcode_R.EXTRACT[2]',
    'DATASTORE':       'FIX_513D.Seq.Data_Store',
    'SEQ_STEP':        'SEQUENCE_STEP',
    'FINISHED_SERIAL': 'ZEBRA.Working_String[20]',
    'SCAN_COMPLETE':   'FIX_513D.Seq.Conv_Barcode_Passed',
    'TN_CHECK_PASS':   'TN_Check_Pass',
    'TN_CHECK_FAIL':   'TN_Check_Fail',
    'TN_DB_ERROR':     'TN_DB_Error',
    'PART_FAIL':       'FIX_513D.Seq.Part_Failed[0]',
    'LEAK_TEST_FAIL':  'FIX_513D.Seq.Leak_Test_Failed',
    'FIRST_PIECE_CHECK':'FIRST_PIECE_CHECK'
}

SQL_STATEMENTS = {
    'insert_tn': (
        "INSERT INTO tn (date, finished_serial, component_serial1, component_serial2, status) "
        "VALUES (?, ?, ?, ?, ?)"
    )
}

POLL_INTERVAL      = 0.5   # general polling interval
FAST_POLL_INTERVAL = 0.1   # fast polling for fail/datastore
RETRY_DELAY        = 10    # seconds between retries

# --- Helper functions ---

def sync_db_from_backup(local_db: str) -> None:
    """
    If USB backup exists and is ahead of local_db, overwrite local_db from USB.
    """
    if not os.path.exists(USB_DB_BACKUP):
        logger.debug("No USB DB backup present at %s", USB_DB_BACKUP)
        return

    try:
        with sqlite3.connect(USB_DB_BACKUP) as usb_conn:
            usb_rows = usb_conn.execute("SELECT COUNT(*) FROM tn").fetchone()[0]
    except Exception:
        logger.exception("Unable to read USB backup DB at %s", USB_DB_BACKUP)
        return

    local_rows = -1
    if os.path.exists(local_db):
        try:
            with sqlite3.connect(local_db) as loc_conn:
                local_rows = loc_conn.execute("SELECT COUNT(*) FROM tn").fetchone()[0]
        except Exception:
            logger.exception("Unable to read local DB at %s", local_db)

    if not os.path.exists(local_db) or usb_rows > local_rows:
        try:
            os.makedirs(os.path.dirname(local_db) or ".", exist_ok=True)
            shutil.copy2(USB_DB_BACKUP, local_db)
            logger.info(
                "Restored local DB from USB backup: %s (local rows=%d, usb rows=%d)",
                local_db, local_rows, usb_rows
            )
        except Exception:
            logger.exception("Failed to copy USB DB %s to local %s", USB_DB_BACKUP, local_db)


def wait_for_tag(plc: LogixDriver, tag_key: str) -> None:
    name = PLC_TAGS[tag_key]
    # wait for false→true transition
    while True:
        val = plc.read(name)
        if val and not val.value:
            break
        time.sleep(POLL_INTERVAL)
    # then true
    while True:
        val = plc.read(name)
        if val and val.value:
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
        time.sleep(FAST_POLL_INTERVAL)


def wait_for_fail_or_reset(plc: LogixDriver) -> bool:
    while True:
        fl = plc.read(PLC_TAGS['PART_FAIL'])
        if fl and fl.value:
            return True
        time.sleep(FAST_POLL_INTERVAL)


def check_converter_sn(cursor: sqlite3.Cursor, column: str, sn: Any, label: str) -> bool:
    cursor.execute(f"SELECT 1 FROM tn WHERE {column} = ?", (sn,))
    if cursor.fetchone():
        logger.warning("%s Converter SN Failed: %s", label, sn)
        return False
    logger.info("%s Converter SN Passed: %s", label, sn)
    return True


def is_leaktest_rerun_allowed(cursor: sqlite3.Cursor, lhconv: Any, rhconv: Any) -> bool:
    cursor.execute(
        "SELECT COUNT(*) FROM tn WHERE component_serial1=? AND component_serial2=? AND status='Part Failed Leaktest'",
        (lhconv, rhconv)
    )
    if cursor.fetchone()[0] != 1:
        return False
    cursor.execute(
        "SELECT COUNT(*) FROM tn WHERE (component_serial1=? OR component_serial2=?) AND status!='Part Failed Leaktest'",
        (lhconv, rhconv)
    )
    return cursor.fetchone()[0] == 0


def set_pass(plc: LogixDriver, passed: bool) -> None:
    plc.write((PLC_TAGS['TN_CHECK_PASS'], passed))
    plc.write((PLC_TAGS['TN_CHECK_FAIL'], not passed))


def insert_tn_record(db_path: str, timestamp: str, finished_serial: Any,
                     lhconv: Any, rhconv: Any, status: str) -> None:
    try:
        with sqlite3.connect(db_path) as conn2:
            cur2 = conn2.cursor()
            cur2.execute(SQL_STATEMENTS['insert_tn'],
                         (timestamp, finished_serial, lhconv, rhconv, status))
            conn2.commit()
            logger.info("Data stored in USB backup database: %s", db_path)
    except Exception as e:
        logger.error("Failed to write TN record to USB backup DB %s: %s", db_path, e)


def handle_fail(lh_pass: bool, rh_pass: bool, plc: LogixDriver,
                cursor: sqlite3.Cursor, lhconv: Any, rhconv: Any) -> None:
    # 1) Leak-test failure branch
    leak = plc.read(PLC_TAGS['LEAK_TEST_FAIL'])
    if leak and leak.value:
        status = "Part Failed Leaktest"
        logger.error(status)
        if wait_for_fail_or_reset(plc):
            ts = time.strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute(SQL_STATEMENTS['insert_tn'],
                           (ts, 'N/A', lhconv, rhconv, status))
            cursor.connection.commit()
            logger.error("Leak Test Failed - Data stored in database")
            insert_tn_record(USB_DB_BACKUP, ts, 'N/A', lhconv, rhconv, status)
        return

    # 2) Duplicate-SN logic
    if not lh_pass and not rh_pass:
        base_status = "LH & RH SN Dupe - Failed"
        cursor.execute(
            "SELECT finished_serial FROM tn "
            "WHERE component_serial1=? OR component_serial2=? "
            "ORDER BY id ASC LIMIT 1",
            (lhconv, rhconv)
        )
    elif not lh_pass:
        base_status = "LH SN Dupe - Failed"
        cursor.execute(
            "SELECT finished_serial FROM tn "
            "WHERE component_serial1=? "
            "ORDER BY id ASC LIMIT 1",
            (lhconv,)
        )
    else:
        base_status = "RH SN Dupe - Failed"
        cursor.execute(
            "SELECT finished_serial FROM tn "
            "WHERE component_serial2=? "
            "ORDER BY id ASC LIMIT 1",
            (rhconv,)
        )

    row = cursor.fetchone()
    first_tla = row[0] if row and row[0] else "Unknown"
    status = f"{base_status} (first TLA: {first_tla})"
    logger.error(status)
    if wait_for_fail_or_reset(plc):
        ts = time.strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute(SQL_STATEMENTS['insert_tn'],
                       (ts, 'N/A', lhconv, rhconv, status))
        cursor.connection.commit()
        logger.error("Failed TN Check - Data stored in database")
        insert_tn_record(USB_DB_BACKUP, ts, 'N/A', lhconv, rhconv, status)


def monitor_and_update(plc_ip_address: str, db_file: str) -> None:
    def heartbeat_loop(plc: LogixDriver) -> None:
        state = False
        while True:
            try:
                plc.write((PLC_TAGS['PI_HEARTBEAT'], state))
                logger.debug("Heartbeat sent: %s", state)
            except Exception as e:
                logger.debug("Heartbeat error: %s", e)
                break
            state = not state
            time.sleep(1)

    while True:
        sync_db_from_backup(db_file)
        try:
            logger.debug("Attempting connection to PLC at %s", plc_ip_address)
            with LogixDriver(plc_ip_address) as plc:
                threading.Thread(target=heartbeat_loop, args=(plc,), daemon=True).start()
                logger.info("PLC connection established.")

                while True:
                    wait_for_tag(plc, 'SCAN_COMPLETE')
                    with sqlite3.connect(db_file) as conn:
                        cursor = conn.cursor()
                        lhconv = plc.read(PLC_TAGS['LH_CONV']).value
                        rhconv = plc.read(PLC_TAGS['RH_CONV']).value

                        # 0) First-piece check
                        fpc = plc.read(PLC_TAGS['FIRST_PIECE_CHECK'])
                        if fpc and fpc.value:
                            logger.info("First Piece Check - test part detected")
                            set_pass(plc, True)
                            if wait_for_datastore_or_reset(plc):
                                finished_serial = plc.read(PLC_TAGS['FINISHED_SERIAL']).value
                                ts = time.strftime('%Y-%m-%d %H:%M:%S')
                                cursor.execute(SQL_STATEMENTS['insert_tn'],
                                               (ts, finished_serial, lhconv, rhconv,
                                                'First Piece Check'))
                                conn.commit()
                                logger.info("Data stored in local database (First Piece Check)")
                                insert_tn_record(USB_DB_BACKUP, ts, finished_serial,
                                                 lhconv, rhconv, 'First Piece Check')
                            continue

                        # 1) Leak-test rerun logic
                        if is_leaktest_rerun_allowed(cursor, lhconv, rhconv):
                            logger.info(
                                "Rerun allowed for LH=%s, RH=%s due to previous leak test failure.",
                                lhconv, rhconv
                            )
                            set_pass(plc, True)
                            if wait_for_datastore_or_reset(plc):
                                finished_serial = plc.read(PLC_TAGS['FINISHED_SERIAL']).value
                                ts = time.strftime('%Y-%m-%d %H:%M:%S')
                                cursor.execute(SQL_STATEMENTS['insert_tn'],
                                               (ts, finished_serial, lhconv, rhconv,
                                                'Passed - Previously failed leak test.'))
                                conn.commit()
                                logger.info("Data stored in local database (Leak Test Rerun)")
                                insert_tn_record(USB_DB_BACKUP, ts, finished_serial,
                                                 lhconv, rhconv,
                                                 'Passed - Previously failed leak test.')
                            continue

                        # 2) Normal pass / fail
                        lh_pass = check_converter_sn(cursor, 'component_serial1', lhconv, 'LH')
                        rh_pass = check_converter_sn(cursor, 'component_serial2', rhconv, 'RH')

                        if lh_pass and rh_pass:
                            set_pass(plc, True)
                            if wait_for_datastore_or_reset(plc):
                                finished_serial = plc.read(PLC_TAGS['FINISHED_SERIAL']).value
                                ts = time.strftime('%Y-%m-%d %H:%M:%S')
                                cursor.execute(SQL_STATEMENTS['insert_tn'],
                                               (ts, finished_serial, lhconv, rhconv, 'Passed'))
                                conn.commit()
                                logger.info("Data stored in local database")
                                insert_tn_record(USB_DB_BACKUP, ts, finished_serial,
                                                 lhconv, rhconv, 'Passed')
                        else:
                            set_pass(plc, False)
                            handle_fail(lh_pass, rh_pass, plc, cursor, lhconv, rhconv)

        except KeyboardInterrupt:
            logger.info("Interrupted by user, exiting.")
            sys.exit(0)

        except CommError as e_comm:
            logger.error(
                "CommError connecting to PLC %s: %s – retrying in %ds",
                plc_ip_address, e_comm, RETRY_DELAY
            )

        except Exception:
            logger.exception(
                "Unexpected error in monitor_and_update – retrying in %ds",
                RETRY_DELAY
            )

        time.sleep(RETRY_DELAY)


def main() -> None:
    parser = argparse.ArgumentParser(description="TN barcode converter serial checker")
    parser.add_argument("--plc", default="10.131.201.60", help="PLC IP address")
    parser.add_argument("--db", default=default_local_db, help="Path to SQLite DB file")
    args = parser.parse_args()

    db_file = os.path.expanduser(args.db)
    logger.info("Starting tnpy: PLC=%s, DB=%s", args.plc, db_file)
    sync_db_from_backup(db_file)
    monitor_and_update(args.plc, db_file)


if __name__ == "__main__":
    main()