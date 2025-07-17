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

import logging
from pycomm3 import LogixDriver
import sqlite3
import time
from typing import Any

# Configure logging
logging.basicConfig(
    filename='app.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Constants for tag names and SQL
TAG_LH_CONV = 'FIX_513D.Conv_Barcode.EXTRACT[2]'
TAG_RH_CONV = 'FIX_513D.Conv_Barcode_R.EXTRACT[2]'
TAG_DATASTORE = 'FIX_513D.Seq.Data_Store'
TAG_UNCLAMP = 'FIX_513D.Main.Unclamp_Part'
TAG_SEQ_STEP = 'SEQUENCE_STEP'
TAG_TN_CHECK_READY = 'FIX_513D.Seq.Conv_Barcode_Passed'
TAG_TLA_SERIAL = 'ZEBRA.Working_String[20]'
TAG_TN_CHECK_PASS = 'TN_Check_Pass'
TAG_TN_CHECK_FAIL = 'TN_Check_Fail'
TAG_TN_DB_ERROR = 'TN_DB_Error'
SQL_INSERT = (
    "INSERT INTO tn (date, tla_serial, component_serial1, component_serial2, status) "
    "VALUES (?, ?, ?, ?, ?)"
)
POLL_INTERVAL = 0.25  # seconds

def log_and_print(level: str, message: str) -> None:
    """
    Log a message at the specified level and print it to the console.
    """
    getattr(logging, level)(message)
    print(message)

def check_converter_sn(
    cursor: sqlite3.Cursor,
    column_name: str,
    sn_value: Any,
    label: str
) -> bool:
    """
    Check if the converter serial number exists in the database.
    Returns True if not found (pass), False if found (fail).
    """
    cursor.execute(f"SELECT * FROM tn WHERE {column_name} = ?", (sn_value,))
    db_value = cursor.fetchone()
    sn_pass = db_value is None
    msg = f"{label} Converter SN {'Passed' if sn_pass else 'Failed'}: {sn_value}"
    log_and_print('info' if sn_pass else 'warning', msg)
    return sn_pass

def handle_fail(
    lhconv_pass: bool,
    rhconv_pass: bool,
    plc: LogixDriver,
    cursor: sqlite3.Cursor,
    lhconv: Any,
    rhconv: Any
) -> None:
    """
    Handle the fail case for LH and/or RH converter serial number duplication.
    Logs the error and stores the failed check in the database when appropriate.
    """
    if not lhconv_pass and not rhconv_pass:
        message = "LH & RH Converter SN Duplicated - Failed"
    elif not lhconv_pass:
        message = "LH Converter SN Duplicated - Failed"
    elif not rhconv_pass:
        message = "RH Converter SN Duplicated - Failed"
    else:
        return

    status = tla_serial = log_msg = message
    log_and_print('error', log_msg)

    while not lhconv_pass or not rhconv_pass:
        failedpart = plc.read_tag(TAG_UNCLAMP)
        reset = plc.read_tag(TAG_SEQ_STEP)
        if reset == 0:
            break
        if failedpart:
            timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute(SQL_INSERT, (timestamp, tla_serial, lhconv, rhconv, status))
            cursor.connection.commit()
            log_and_print('error', "Failed TN Check - Data stored in database")
            break
        else:
            time.sleep(POLL_INTERVAL)

def monitor_and_update(plc_ip_address: str, db_file: str, TN_Check_Ready: str) -> None:
    """
    Main loop: Monitors PLC for TN check readiness, validates converter serials,
    and logs results to the database.
    """
    try:
        with LogixDriver(plc_ip_address) as plc:
            while True:
                # Wait for TN check ready signal from PLC
                tn_check_ready = plc.read_tag(TAG_TN_CHECK_READY)
                if tn_check_ready:
                    with sqlite3.connect(db_file) as conn:
                        cursor = conn.cursor()
                        try:
                            # Read serial numbers from PLC
                            lhconv = plc.read_tag(TAG_LH_CONV)
                            rhconv = plc.read_tag(TAG_RH_CONV)
                            # Check both converters for duplicates
                            converters = [
                                {"column": "component_serial1", "value": lhconv, "label": "LH"},
                                {"column": "component_serial2", "value": rhconv, "label": "RH"}
                            ]
                            results = [check_converter_sn(cursor, c["column"], c["value"], c["label"]) for c in converters]
                            lhconv_pass, rhconv_pass = results

                            if lhconv_pass and rhconv_pass:
                                # Both converters passed, log and store data
                                plc.write_tag(TAG_TN_CHECK_PASS, True)
                                plc.write_tag(TAG_TN_CHECK_FAIL, False)
                                status = "Passed"
                                log_and_print('info', "TN Check Passed")
                                while True:
                                    # Wait for data store signal or reset
                                    datastore = plc.read_tag(TAG_DATASTORE)
                                    reset = plc.read_tag(TAG_SEQ_STEP)
                                    if reset == 0:
                                        break
                                    if datastore:
                                        tla_serial = plc.read_tag(TAG_TLA_SERIAL)
                                        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
                                        cursor.execute(SQL_INSERT, (timestamp, tla_serial, lhconv, rhconv, status))
                                        conn.commit()
                                        log_and_print('info', "Data stored in database")
                                        break
                                    else:
                                        time.sleep(POLL_INTERVAL)
                            else:
                                # One or both converters failed, handle failure
                                plc.write_tag(TAG_TN_CHECK_PASS, False)
                                plc.write_tag(TAG_TN_CHECK_FAIL, True)
                                handle_fail(lhconv_pass, rhconv_pass, plc, cursor, lhconv, rhconv)
                        except Exception as e:
                            import traceback
                            log_and_print('error', f"Error processing PLC data: {e}\n{traceback.format_exc()}")
                            plc.write_tag(TAG_TN_DB_ERROR, True)
                else:
                    # Polling interval when not ready
                    time.sleep(POLL_INTERVAL)
    except Exception as e:
        import traceback
        log_and_print('error', f"Error connecting to PLC: {e}\n{traceback.format_exc()}")
        try:
            with LogixDriver(plc_ip_address) as plc:
                plc.write_tag(TAG_TN_DB_ERROR, True)
        except Exception:
            pass