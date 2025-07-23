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
import sqlite3
import time
import os
import shutil
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Any

# Setup log directories and handlers
log_dir = Path.home() / "tnpy_logs"
log_dir.mkdir(parents=True, exist_ok=True)

# INFO handler: logs INFO and above to ~/tnpy.log
info_log = Path.home() / "tnpy.log"
info_handler = TimedRotatingFileHandler(
    filename=str(info_log),
    when="midnight",
    interval=1,
    backupCount=0
)
info_handler.suffix = "%Y-%m-%d"
info_handler.namer = lambda name: str(log_dir / Path(name).name)
info_handler.setLevel(logging.INFO)
info_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
)

# DEBUG handler: logs DEBUG and above to ~/tnpy_debug.log
debug_log = Path.home() / "tnpy_debug.log"
debug_handler = TimedRotatingFileHandler(
    filename=str(debug_log),
    when="midnight",
    interval=1,
    backupCount=0
)
debug_handler.suffix = "%Y-%m-%d"
debug_handler.namer = lambda name: str(log_dir / Path(name).name)
debug_handler.setLevel(logging.DEBUG)
debug_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
)

# Configure root logger
tool_logger = logging.getLogger()
tool_logger.setLevel(logging.DEBUG)
tool_logger.addHandler(info_handler)
tool_logger.addHandler(debug_handler)

# Convenience wrapper
def log_and_print(level: str, message: str) -> None:
    lvl = getattr(logging, level.upper(), logging.INFO)
    logging.log(lvl, message)
    print(message)

# Attempt to import PLC driver and error type
try:
    from pycomm3 import LogixDriver, CommError
except ImportError as e:
    log_and_print('error', f"Required module pycomm3 not found: {e}")
    sys.exit(1)

# Paths for local and USB backup DBs
default_local_db = "/home/gap900/tndb900.db"
USB_DB_BACKUP = "/media/usbdrive/db_backup/tndb900.db"

# PLC tags
PLC_TAGS = {
    'PI_HEARTBEAT': 'PI_Heartbeat',
    'LH_CONV': 'FIX_513D.Conv_Barcode.EXTRACT[2]',
    'RH_CONV': 'FIX_513D.Conv_Barcode_R.EXTRACT[2]',
    'DATASTORE': 'FIX_513D.Seq.Data_Store',
    'UNCLAMP': 'FIX_513D.Main.Unclamp_Part',
    'SEQ_STEP': 'SEQUENCE_STEP',
    'FINISHED_SERIAL': 'ZEBRA.Working_String[20]',
    'SCAN_COMPLETE': 'FIX_513D.Seq.Conv_Barcode_Passed',
    'TN_CHECK_PASS': 'TN_Check_Pass',
    'TN_CHECK_FAIL': 'TN_Check_Fail',
    'TN_DB_ERROR': 'TN_DB_Error',
    'PART_FAIL': 'FIX_513D.Seq.Part_Failed[0]',
    'LEAK_TEST_FAIL': 'FIX_513D.Seq.Leak_Test_Failed',
    'FIRST_PIECE_CHECK': 'FIRST_PIECE_CHECK'
}

# SQL statements
SQL_STATEMENTS = {
    'insert_tn': (
        "INSERT INTO tn (date, finished_serial, component_serial1, component_serial2, status) "
        "VALUES (?, ?, ?, ?, ?)"
    )
}

# Timing
POLL_INTERVAL = 0.5  # seconds for general polling
FAST_POLL_INTERVAL = 0.1  # seconds for datastore/fail polling
RETRY_DELAY = 10     # seconds


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
    name = PLC_TAGS[tag_key]
    while True:
        val = plc.read(name)
        if val and not val.value:
            break
        time.sleep(POLL_INTERVAL)
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
        log_and_print('warning', f"{label} Converter SN Failed: {sn}")
        return False
    log_and_print('info', f"{label} Converter SN Passed: {sn}")
    return True


# Add leak-test rerun check
def is_leaktest_rerun_allowed(cursor: sqlite3.Cursor, lhconv: Any, rhconv: Any) -> bool:
    # exactly one prior leak-test-failed entry for these serials
    cursor.execute(
        "SELECT COUNT(*) FROM tn WHERE component_serial1=? AND component_serial2=? AND status='Part Failed Leaktest'",
        (lhconv, rhconv)
    )
    if cursor.fetchone()[0] != 1:
        return False
    # and no other entries for those serials
    cursor.execute(
        "SELECT COUNT(*) FROM tn WHERE (component_serial1=? OR component_serial2=?) AND status!='Part Failed Leaktest'",
        (lhconv, rhconv)
    )
    return cursor.fetchone()[0] == 0


def handle_fail(lh_pass: bool, rh_pass: bool, plc: LogixDriver,
                cursor: sqlite3.Cursor, lhconv: Any, rhconv: Any) -> None:
    """
    Handle the fail case for LH/RH converter serial number duplication or leak test failure.
    """

    # 1) Leak-test failure branch
    leak = plc.read(PLC_TAGS['LEAK_TEST_FAIL'])
    if leak and leak.value:
        status = "Part Failed Leaktest"
        log_and_print('error', status)
        if wait_for_fail_or_reset(plc):
            ts = time.strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute(SQL_STATEMENTS['insert_tn'],
                           (ts, 'N/A', lhconv, rhconv, status))
            cursor.connection.commit()
            log_and_print('error', "Leak Test Failed - Data stored in database")
        return

    # 2) Duplicate-SN logic with first-instance TLA lookup
    # Determine base status message
    if not lh_pass and not rh_pass:
        base_status = "LH & RH SN Dupe - Failed"
        # Find first TLA for either converter
        cursor.execute(
            "SELECT tla_serial FROM tn "
            "WHERE component_serial1=? OR component_serial2=? "
            "ORDER BY id ASC LIMIT 1",
            (lhconv, rhconv)
        )
    elif not lh_pass:
        base_status = "LH SN Dupe - Failed"
        cursor.execute(
            "SELECT tla_serial FROM tn "
            "WHERE component_serial1=? "
            "ORDER BY id ASC LIMIT 1",
            (lhconv,)
        )
    else:
        base_status = "RH SN Dupe - Failed"
        cursor.execute(
            "SELECT tla_serial FROM tn "
            "WHERE component_serial2=? "
            "ORDER BY id ASC LIMIT 1",
            (rhconv,)
        )

    row = cursor.fetchone()
    first_tla = row[0] if row and row[0] else "Unknown"
    status = f"{base_status} (first TLA: {first_tla})"

    # Log and wait for part-fail/reset
    log_and_print('error', status)
    if wait_for_fail_or_reset(plc):
        ts = time.strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute(SQL_STATEMENTS['insert_tn'],
                       (ts, 'N/A', lhconv, rhconv, status))
        cursor.connection.commit()
        log_and_print('error', "Failed TN Check - Data stored in database")


def monitor_and_update(plc_ip_address: str, db_file: str) -> None:
    import threading

    def heartbeat_loop(plc: LogixDriver) -> None:
        state = False
        while True:
            try:
                plc.write((PLC_TAGS['PI_HEARTBEAT'], state))
                logging.debug(f"Heartbeat sent: {state}")
            except Exception as e:
                logging.debug(f"Heartbeat error: {e}")
                break
            state = not state
            time.sleep(1)

    while True:
        sync_db_from_backup(db_file)
        try:
            log_and_print('debug', f"Attempting connection to PLC at {plc_ip_address}")
            with LogixDriver(plc_ip_address) as plc:
                # start heartbeat thread
                threading.Thread(target=heartbeat_loop, args=(plc,), daemon=True).start()
                log_and_print('info', "PLC connection established.")
                while True:
                    wait_for_tag(plc, 'SCAN_COMPLETE')
                    with sqlite3.connect(db_file) as conn:
                        cursor = conn.cursor()
                        try:
                            lhconv = plc.read(PLC_TAGS['LH_CONV']).value
                            rhconv = plc.read(PLC_TAGS['RH_CONV']).value

                            # 0) First‐piece (test part) check
                            fpc = plc.read(PLC_TAGS['FIRST_PIECE_CHECK'])
                            if fpc and fpc.value:
                                log_and_print('info', "First Piece Check - test part detected")
                                plc.write((PLC_TAGS['TN_CHECK_PASS'], True))
                                plc.write((PLC_TAGS['TN_CHECK_FAIL'], False))
                                if wait_for_datastore_or_reset(plc):
                                    finished_serial = plc.read(PLC_TAGS['FINISHED_SERIAL']).value
                                    ts = time.strftime('%Y-%m-%d %H:%M:%S')
                                    cursor.execute(
                                        SQL_STATEMENTS['insert_tn'],
                                        (ts, finished_serial, lhconv, rhconv, 'First Piece Check')
                                    )
                                    conn.commit()
                                    log_and_print('info', "Data stored in database (First Piece Check)")
                                continue  # skip normal pass/fail logic

                            # 1) Leak-test rerun logic
                            if is_leaktest_rerun_allowed(cursor, lhconv, rhconv):
                                log_and_print(
                                    'info',
                                    f"Rerun allowed for LH={lhconv}, RH={rhconv} due to previous leak test failure."
                                )
                                plc.write((PLC_TAGS['TN_CHECK_PASS'], True))
                                plc.write((PLC_TAGS['TN_CHECK_FAIL'], False))
                                # wait for datastore or reset, then insert rerun entry
                                if wait_for_datastore_or_reset(plc):
                                    finished_serial = plc.read(PLC_TAGS['FINISHED_SERIAL']).value
                                    ts = time.strftime('%Y-%m-%d %H:%M:%S')
                                    cursor.execute(
                                        SQL_STATEMENTS['insert_tn'],
                                        (ts, finished_serial, lhconv, rhconv,
                                         'Passed - Previously failed leak test.')
                                    )
                                    conn.commit()
                                    log_and_print('info', "Data stored in database (Leak Test Rerun)")
                                continue  # skip normal pass/fail

                            # 1) Normal duplicate-SN check
                            lh_pass = check_converter_sn(
                                cursor, 'component_serial1', lhconv, 'LH'
                            )
                            rh_pass = check_converter_sn(
                                cursor, 'component_serial2', rhconv, 'RH'
                            )

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
