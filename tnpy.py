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
from typing import Any

# Configure logging as early as possible
logging.basicConfig(
    filename='tnpy.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Attempt to import PLC driver and error type
try:
    from pycomm3 import LogixDriver, CommError
except ImportError as e:
    error_msg = f"Required module pycomm3 not found: {e}"
    logging.error(error_msg)
    print(error_msg, file=sys.stderr)
    sys.exit(1)

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
RETRY_DELAY = 5       # seconds between reconnect attempts when offline

def log_and_print(level: str, message: str) -> None:
    logging.log(getattr(logging, level.upper()), message)
    print(message)

def wait_for_tag(plc: LogixDriver, tag_key: str) -> None:
    """
    Block until the specified PLC tag transitions from False to True.
    """
    tag_name = PLC_TAGS[tag_key]
    # wait for it to go False first
    while True:
        result = plc.read(tag_name)
        if result and not result.value:
            break
        time.sleep(POLL_INTERVAL)
    # now wait for rising edge
    while True:
        result = plc.read(tag_name)
        if result and result.value:
            return
        time.sleep(POLL_INTERVAL)

def wait_for_datastore_or_reset(plc: LogixDriver) -> bool:
    while True:
        datastore = plc.read(PLC_TAGS['DATASTORE'])
        reset = plc.read(PLC_TAGS['SEQ_STEP'])
        if reset and reset.value == 0:
            return False
        if datastore and datastore.value:
            return True
        time.sleep(POLL_INTERVAL)

def wait_for_fail_or_reset(plc: LogixDriver) -> bool:
    while True:
        failed = plc.read(PLC_TAGS['UNCLAMP'])
        reset = plc.read(PLC_TAGS['SEQ_STEP'])
        if reset and reset.value == 0:
            return False
        if failed and failed.value:
            return True
        time.sleep(POLL_INTERVAL)

def check_converter_sn(cursor: sqlite3.Cursor, column_name: str, sn_value: Any, label: str) -> bool:
    cursor.execute(f"SELECT 1 FROM tn WHERE {column_name} = ?", (sn_value,))
    exists = cursor.fetchone() is not None
    if exists:
        log_and_print('warning', f"{label} Converter SN Failed: {sn_value}")
        return False
    log_and_print('info', f"{label} Converter SN Passed: {sn_value}")
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
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute(
            SQL_STATEMENTS['insert_tn'],
            (timestamp, 'N/A', lhconv, rhconv, status)
        )
        cursor.connection.commit()
        log_and_print('error', "Failed TN Check - Data stored in database")

def monitor_and_update(plc_ip_address: str, db_file: str) -> None:
    """
    Main loop: keep trying to connect to the PLC forever.
    Once connected, process scans continuously.
    """
    while True:
        try:
            log_and_print('info', f"Attempting connection to PLC at {plc_ip_address}")
            with LogixDriver(plc_ip_address) as plc:
                log_and_print('info', "PLC connection established.")
                # Inner loop: process scans
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
                                res = plc.write((PLC_TAGS['TN_CHECK_PASS'], False))
                                if not res or getattr(res, 'error', False):
                                    log_and_print('error', f"Write TN_CHECK_PASS failed: {res}")
                                res = plc.write((PLC_TAGS['TN_CHECK_FAIL'], True))
                                if not res or getattr(res, 'error', False):
                                    log_and_print('error', f"Write TN_CHECK_FAIL failed: {res}")
                                handle_fail(lh_pass, rh_pass, plc, cursor, lhconv, rhconv)

                        except CommError as e_comm_inner:
                            log_and_print('error', f"Lost PLC connection during processing: {e_comm_inner}")
                            break  # break inner loop to reconnect
                        except Exception as e_inner:
                            import traceback
                            log_and_print(
                                'error',
                                f"Error during PLC processing: {e_inner}\n{traceback.format_exc()}"
                            )
                            plc.write((PLC_TAGS['TN_DB_ERROR'], True))

        except KeyboardInterrupt:
            log_and_print('info', "Interrupted by user, exiting.")
            sys.exit(0)
        except CommError as e_comm:
            log_and_print(
                'error',
                f"CommError connecting to PLC ({plc_ip_address}): {e_comm}. "
                f"Retrying in {RETRY_DELAY}s."
            )
        except Exception as e:
            import traceback
            log_and_print(
                'error',
                f"Unexpected error in monitor_and_update: {e}\n"
                f"{traceback.format_exc()}\nRetrying in {RETRY_DELAY}s."
            )

        time.sleep(RETRY_DELAY)

def main() -> None:
    """
    Parse command-line arguments and start the monitoring loop.
    """
    parser = argparse.ArgumentParser(
        description="TN barcode converter serial checker"
    )
    parser.add_argument(
        "--plc", default="10.131.201.60",
        help="IP address of the Allen-Bradley PLC"
    )
    parser.add_argument(
        "--db", default="~/tndb900.db",
        help="Path to the SQLite database file"
    )
    args = parser.parse_args()

    db_file = os.path.expanduser(args.db)
    log_and_print('info', f"Starting tnpy: PLC={args.plc}, DB={db_file}")
    monitor_and_update(args.plc, db_file)

if __name__ == "__main__":
    main()
