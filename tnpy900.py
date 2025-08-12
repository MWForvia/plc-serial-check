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
from pathlib import Path
from typing import Any
from datetime import datetime, timedelta
import errno
import json
import socket
socket.setdefaulttimeout(None)

# helper to open SQLite in WAL mode with relaxed sync
def get_db_connection(path: str, timeout: float = 1.0) -> sqlite3.Connection:
    """
    Open a SQLite connection in WAL mode with NORMAL synchronous to reduce lock contention.
    """
    conn = sqlite3.connect(path, timeout=timeout)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    # performance PRAGMAs: larger cache in RAM, temp tables in memory, less frequent WAL checkpoints
    conn.execute("PRAGMA cache_size=-5000;")         # roughly 5MB page cache
    conn.execute("PRAGMA temp_store=MEMORY;")       # store temp tables in memory
    conn.execute("PRAGMA wal_autocheckpoint=100;") # checkpoint after 100 WAL pages
    return conn

 # Helper to detect real mount
# import os  # duplicate import removed

def is_mounted(path: str) -> bool:
    """
    Return True if the given path or any of its parent directories is a mount point.
    """
    p = path
    # climb up until before root (ignore root mount)
    while p and p != os.path.sep:
        if os.path.ismount(p):
            return True
        p = os.path.dirname(p)
    return False

# --- Logging setup ---
log_dir = Path.home() / "tnpy_logs900"
log_dir.mkdir(parents=True, exist_ok=True)

# INFO handler
info_log = Path.home() / "tnpy900.log"
info_handler = TimedRotatingFileHandler(
    filename=str(info_log), when="midnight", interval=1, backupCount=0
)
info_handler.suffix = "%Y-%m-%d"
info_handler.namer = lambda name: str(log_dir / Path(name).name)
info_handler.setLevel(logging.INFO)
info_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

# DEBUG handler
debug_log = Path.home() / "tnpy_debug900.log"
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
# Metrics logging setup
metrics_log = Path.home() / "tnpy_metrics900.log"
metrics_handler = TimedRotatingFileHandler(
    filename=str(metrics_log), when="midnight", interval=1, backupCount=0
)
metrics_handler.suffix = "%Y-%m-%d"
metrics_handler.namer = lambda name: str(log_dir / Path(name).name)
metrics_handler.setLevel(logging.INFO)
metrics_handler.setFormatter(logging.Formatter("%(message)s"))
metrics_logger = logging.getLogger("metrics")
metrics_logger.setLevel(logging.INFO)
metrics_logger.addHandler(metrics_handler)
# Global error counters for metrics
error_comm_count = 0
error_unexpected_count = 0

# --- PLC driver import ---
try:
    from pycomm3 import LogixDriver, CommError
except ImportError as e:
    logger.error("Required module pycomm3 not found: %s", e)
    sys.exit(1)

# --- Constants ---
default_local_db   = "/home/gap900/tndb900.db"
USB_DB_BACKUP      = "/media/usbdrive/db_backup900/tndb900.db"
USB_DB_BACKUP2     = "/media/usbdrive2/db_backup900/tndb900.db"
USB_DB_BACKUPS     = [USB_DB_BACKUP, USB_DB_BACKUP2]

PLC_TAGS = {
    # New tags (project)
    # Control flow
    'TN_CHECK_PASS':         'TN.CHECK_PASS',
    'TN_CHECK_FAIL':         'TN.CHECK_FAIL',
    'SCAN_COMPLETE':         'TN.DB_CHECK_TRIGGER',

    # Error & diagnostic
    'TN_DB_ERROR':           'TN.DB_ERROR',
    'DB_ERROR_INFO':         'TN.DB_ERROR_INFO',
    'TN_MESSAGE':            'TN.Message',

    # Processing flow
    'FIRST_PIECE_CHECK':     'TN.RABBIT_MODE',
    'REWORK_MODE':           'TN.REWORK_MODE',
    'REWORK_LABEL_FINISHED': 'TN.RW_LABEL_FINISHED',
    'TN_MANUAL_ENTRY':       'TN.RW_MANUAL_ENTRY',

    # TLA duplicate handling
    'TLA_SN_PASS':           'TN.TLA_SN_PASS',
    'TN_TLA_SN_CHECK_PASS':  'TN.TLA_SN_CHECK_PASS',

    # Existing tags (from the PLC)
    'SERIAL_HOLDER':         'ZEBRA.Working_String[20]',    
    'LH_CONV':               'FIX_513D.Conv_Barcode.EXTRACT[2]',
    'RH_CONV':               'FIX_513D.Conv_Barcode_R.EXTRACT[2]',
    'DATASTORE':             'FIX_513D.Seq.Data_Store',
    'SEQ_STEP':              'SEQUENCE_STEP',
    'FINISHED_SERIAL':       'ZEBRA.Working_String[20]',
    'PART_FAIL':             'FIX_513D.Seq.Part_Failed[0]',
    'LEAK_TEST_FAIL':        'FIX_513D.Seq.Leak_Test_Failed',
    # Label scanning and manual entry tags
    'PART_SELECT':           'FIX_513D.Part_Select',
    'SERIAL_NUMBER':         'FIX_513D.Serial_Number',
}

SQL_STATEMENTS = {
    # include finished_serial_date and component_serial?_date for faster lookups
    'insert_tn': (
        "INSERT INTO tn (date, finished_serial, finished_serial_date, component_serial1, component_serial1_date, "
        "component_serial2, component_serial2_date, status) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    )
}

def extract_julian(serial: Any) -> str:
    """
    Extract 5-digit Julian date from serial (chars 2-6), or empty string if invalid.
    """
    s = str(serial or "")
    return s[2:7] if len(s) >= 7 else ""

# Detailed DB error info codes for TN.DB_ERROR_INFO
DB_ERROR_INFO_CODES = {
    'SCHEMA_ERROR':        1,  # failed to create or migrate schema
    'WRITE_ERROR':         2,  # failed to write record to DB
    'REWORK_LOOKUP_ERROR': 3,  # failed during rework DB lookup
}
# helper to write a message string back to PLC TN.Message tag
def write_plc_message(plc: LogixDriver, message: str) -> None:
    """Write a message to the TN.Message PLC tag."""
    try:
        # Format the message for a STRING tag
        plc.write({
            PLC_TAGS['TN_MESSAGE']: {
                "LEN": len(message),
                "DATA": message
            }
        })
    except Exception:
        logger.exception("Failed to write PLC message")

# Ensure local DB file exists and has the required schema
def ensure_db_schema(db_path: str) -> None:
    """
    Ensure the database file, 'tn' table, and schema are correct before attempting creation.
    """
    parent = os.path.dirname(db_path) or '.'
    try:
        os.makedirs(parent, exist_ok=True)
    except Exception as e:
        logger.error("Failed to create directory for DB %s: %s", parent, e)
        try:
            plc = globals().get('plc')
            if plc:
                plc.write((PLC_TAGS['TN_DB_ERROR'], True))
                plc.write((PLC_TAGS['DB_ERROR_INFO'], DB_ERROR_INFO_CODES['SCHEMA_ERROR']))
        except Exception:
            pass
        sys.exit(1)

    expected_columns = {
        'id': 'INTEGER',
        'date': 'TEXT',
        'finished_serial': 'TEXT',
        'finished_serial_date': 'TEXT',
        'component_serial1': 'TEXT',
        'component_serial1_date': 'TEXT',
        'component_serial2': 'TEXT',
        'component_serial2_date': 'TEXT',
        'status': 'TEXT'
    }

    try:
        with get_db_connection(db_path) as conn:
            # Check if the 'tn' table exists
            table_exists = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='tn';"
            ).fetchone()

            if not table_exists:
                # Create the table if it does not exist
                conn.execute(
                    """
                    CREATE TABLE tn (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        date TEXT,
                        finished_serial TEXT,
                        finished_serial_date TEXT,
                        component_serial1 TEXT,
                        component_serial1_date TEXT,
                        component_serial2 TEXT,
                        component_serial2_date TEXT,
                        status TEXT
                    )
                    """
                )
                conn.commit()
                return

            # Validate the schema of the 'tn' table
            actual_columns = {
                row[1]: row[2] for row in conn.execute("PRAGMA table_info('tn');")
            }

            for col, col_type in expected_columns.items():
                if col not in actual_columns or actual_columns[col] != col_type:
                    logger.error("Schema mismatch for column '%s': expected '%s', found '%s'", 
                                 col, col_type, actual_columns.get(col))
                    raise ValueError("Schema validation failed for table 'tn'")

    except Exception as e:
        logger.error("Failed to validate or create schema for DB %s: %s", db_path, e)
        try:
            plc = globals().get('plc')
            if plc:
                plc.write((PLC_TAGS['TN_DB_ERROR'], True))
                plc.write((PLC_TAGS['DB_ERROR_INFO'], DB_ERROR_INFO_CODES['SCHEMA_ERROR']))
        except Exception:
            pass
        sys.exit(1)

POLL_INTERVAL      = 0.5   # general polling interval
FAST_POLL_INTERVAL = 0.25   # fast polling for fail/datastore
RETRY_DELAY        = 1    # seconds to wait before first retry
MAX_RETRY_DELAY    = 5    # maximum seconds to back off on repeated errors
RESET_BACKOFF_TIMEOUT = 60  # seconds of stability to reset retry delay

# --- USB health & formatting helpers ---

# --- Helper functions ---

def sync_db_from_backup(local_db: str) -> None:
    """
    Incremental tri-directional sync: pick the DB with most rows then append missing rows to the others.
    """
    # show all DB paths being used for syncing for diagnostics
    logger.debug("Sync targets: local=%s, usb1=%s, usb2=%s", local_db, USB_DB_BACKUP, USB_DB_BACKUP2)
    logger.debug("Entered sync_db_from_backup")
    paths = {
        'local': local_db,
        'usb1':  USB_DB_BACKUP,
        'usb2':  USB_DB_BACKUP2,
    }
    logger.debug("Entering sync_db_from_backup with local_db=%s", local_db)
    # compute max id in each accessible DB
    row_ids = {}
    for name, path in paths.items():
        logger.debug("Scanning DB %s at %s", name, path)
        # for USB targets, require parent dir and mount before proceeding
        if path.startswith("/media"):
            # determine the actual mount root, e.g. '/media/usbdrive'
            mount_root = os.path.dirname(os.path.dirname(path))
            # skip entire USB if not mounted
            if not is_mounted(mount_root):
                logger.debug("Skipping read for %s: mount %s not present", name, mount_root)
                row_ids[name] = -1
                continue
            # ensure backup directory exists under the mounted device
            parent = os.path.dirname(path)
            try:
                os.makedirs(parent, exist_ok=True)
            except OSError as e:
                if e.errno in (errno.ENODEV, errno.ENOENT):
                    logger.debug("Skipping read for %s: cannot create directory %s (%s)", name, parent, e)
                    row_ids[name] = -1
                    continue
                else:
                    logger.exception("Error creating directory %s for %s", parent, name)
                    row_ids[name] = -1
                    continue
            # if DB file is missing, initialize new DB with schema
            if not os.path.exists(path):
                try:
                    with get_db_connection(path, timeout=1) as init_conn:
                        init_conn.execute(
                            """
                            CREATE TABLE IF NOT EXISTS tn (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                date TEXT,
                                finished_serial TEXT,
                                finished_serial_date TEXT,
                                component_serial1 TEXT,
                                component_serial1_date TEXT,
                                component_serial2 TEXT,
                                component_serial2_date TEXT,
                                status TEXT
                            )
                            """
                        )
                        init_conn.commit()
                        # create indexes on new backup DB
                        init_conn.execute("CREATE INDEX IF NOT EXISTS idx_tn_finished ON tn(finished_serial);")
                        init_conn.execute("CREATE INDEX IF NOT EXISTS idx_tn_finished_date ON tn(finished_serial_date);")
                        init_conn.commit()
                        logger.debug("Created and initialized new backup DB at %s", path)
                except Exception:
                    row_ids[name] = -1
                    logger.exception("Failed to initialize DB at %s", path)
                    continue
        # attempt to read max id
        try:
            with get_db_connection(path, timeout=1) as conn:
                row_ids[name] = conn.execute("SELECT MAX(id) FROM tn").fetchone()[0] or 0
            logger.debug("Max id for %s = %s", name, row_ids[name])
        except Exception:
            row_ids[name] = -1
            logger.exception("Unable to read DB at %s", path)
    if all(val < 0 for val in row_ids.values()):
        logger.warning("No accessible database files to sync: %s", paths)
        logger.info("Exiting sync_db_from_backup without action")
        return
    # choose source with highest max id
    source = max(row_ids, key=row_ids.get)
    src_path = paths[source]
    # incremental append to others
    for name, tgt_path in paths.items():
        if name == source:
            continue
        if tgt_path.startswith("/media") and not is_mounted(tgt_path):
            logger.debug("Skipping sync to unmounted %s", tgt_path)
            continue
        tgt_dir = os.path.dirname(tgt_path) or "."
        try:
            os.makedirs(tgt_dir, exist_ok=True)
        except OSError as e:
            # skip backend if USB path not available
            if e.errno in (errno.ENODEV, errno.ENOENT):
                logger.debug("Skipping sync to %s: cannot create directory %s (%s)", tgt_path, tgt_dir, e)
                continue
            else:
                logger.exception("Error creating directory %s for %s", tgt_dir, tgt_path)
                continue
            with sqlite3.connect(tgt_path) as tgt_conn:
                tgt_cur = tgt_conn.cursor()
                tgt_cur.execute(
                    "CREATE TABLE IF NOT EXISTS tn ("
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                    "date TEXT, finished_serial TEXT, "
                    "component_serial1 TEXT, component_serial2 TEXT, status TEXT)"
                )
                tgt_cur.execute("SELECT MAX(id) FROM tn")
                max_id = tgt_cur.fetchone()[0] or 0
                tgt_conn.execute("ATTACH DATABASE ? AS src", (src_path,))
                new_count = tgt_conn.execute(
                    "SELECT COUNT(*) FROM src.tn WHERE id > ?", (max_id,)
                ).fetchone()[0]
                if new_count > 0:
                    tgt_conn.execute(
                        "INSERT INTO tn(date, finished_serial, component_serial1, component_serial1_date, "
                        "component_serial2, component_serial2_date, status) "
                        "SELECT date, finished_serial, component_serial1, component_serial1_date, "
                        "component_serial2, component_serial2_date, status "
                        "FROM src.tn WHERE id > ?", (max_id,)
                    )
                    tgt_conn.commit()
                    logger.info("Appended %d new rows from %s to %s", new_count, src_path, tgt_path)
                tgt_conn.execute("DETACH DATABASE src")
        except Exception:
            logger.exception("Failed incremental sync from %s to %s", src_path, tgt_path)
    logger.debug("Exiting sync_db_from_backup")


def sync_local_to_target(local_db: str, target_db: str) -> None:
    """
    One-way sync: append new rows from local_db into target_db.
    """
    # require mount
    if not is_mounted(target_db):
        logger.debug("Target %s not mounted, skipping one-way sync", target_db)
        return
    # ensure directory and schema
    parent = os.path.dirname(target_db)
    os.makedirs(parent, exist_ok=True)
    if not os.path.exists(target_db):
        try:
            with get_db_connection(target_db, timeout=3) as init_conn:
                init_conn.execute(
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
                init_conn.commit()
            logger.debug("Initialized target DB for one-way sync at %s", target_db)
        except Exception:
            logger.exception("Failed to initialize target DB at %s", target_db)
            return
    # attach and insert
    try:
        with get_db_connection(target_db, timeout=3) as tgt_conn:
            tgt_conn.execute("ATTACH DATABASE ? AS src", (local_db,))
            max_id = tgt_conn.execute("SELECT MAX(id) FROM tn").fetchone()[0] or 0
            new_count = tgt_conn.execute(
                "SELECT COUNT(*) FROM src.tn WHERE id > ?", (max_id,)
            ).fetchone()[0]
            if new_count > 0:
                tgt_conn.execute(
                    "INSERT INTO tn(date, finished_serial, component_serial1, component_serial1_date, "
                    "component_serial2, component_serial2_date, status) "
                    "SELECT date, finished_serial, component_serial1, component_serial1_date, "
                    "component_serial2, component_serial2_date, status "
                    "FROM src.tn WHERE id > ?", (max_id,)
                )
                tgt_conn.commit()
                logger.info("Auto-synced %d new rows from %s to %s", new_count, local_db, target_db)
            tgt_conn.execute("DETACH DATABASE src")
    except Exception:
        logger.exception("One-way sync failed from %s to %s", local_db, target_db)


def watch_usb_and_sync(local_db: str, usb_path: str, name: str) -> None:
    """
    Poll for usb_path mounting and trigger one-way sync when plugged in.
    """
    last_mounted = False
    while True:
        mounted = is_mounted(usb_path)
        if mounted and not last_mounted:
            logger.info("Detected %s mount at %s, running tri-directional backup sync", name, usb_path)
            sync_db_from_backup(local_db)
        last_mounted = mounted
        time.sleep(POLL_INTERVAL)


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
    """
    Edge-detect the DATASTORE tag: wait for false→true, return True, or exit on reset.
    """
    # ensure starting from false
    while True:
        rs = plc.read(PLC_TAGS['SEQ_STEP'])
        if rs and rs.value == 0:
            return False
        ds = plc.read(PLC_TAGS['DATASTORE'])
        if ds and ds.value:
            return True
        time.sleep(FAST_POLL_INTERVAL)


def wait_for_fail_or_reset(plc: LogixDriver) -> bool:
    """
    Edge-detect the PART_FAIL tag: wait for false→true, return True, or exit on reset.
    """
    # ensure starting from false
    while True:
        rs = plc.read(PLC_TAGS['SEQ_STEP'])
        if rs and rs.value == 0:
            return False
        fl = plc.read(PLC_TAGS['PART_FAIL'])
        if fl and not fl.value:
            break
        time.sleep(FAST_POLL_INTERVAL)
    # now wait for true or reset
    while True:
        rs = plc.read(PLC_TAGS['SEQ_STEP'])
        if rs and rs.value == 0:
            return False
        fl = plc.read(PLC_TAGS['PART_FAIL'])
        if fl and fl.value:
            return True
        time.sleep(FAST_POLL_INTERVAL)


def check_converter_sn(cursor: sqlite3.Cursor, column: str, sn: Any, label: str) -> bool:
    julian_date = extract_julian(sn)
    cursor.execute(f"SELECT {column} FROM tn WHERE {column}_date = ?", (julian_date,))
    matches = cursor.fetchall()

    if not matches:
        logger.info(f"{label} Converter SN Passed: {sn}")
        return True

    # Check for a full match with the serial number
    full_match = any(match[0] == sn for match in matches)
    if full_match:
        logger.warning(f"{label} Converter SN Failed: {sn}")
        return False

    logger.info(f"{label} Converter SN Passed: {sn}")
    return True


def set_pass(plc: LogixDriver, passed: bool) -> None:
    plc.write((PLC_TAGS['TN_CHECK_PASS'], passed))
    plc.write((PLC_TAGS['TN_CHECK_FAIL'], not passed))


def insert_tn_record(db_path: str, timestamp: str, finished_serial: Any,
                     finished_serial_date: Any, lhconv: Any, lhconv_date: Any,
                     rhconv: Any, rhconv_date: Any, status: str) -> None:
    logger.debug("Entering function: insert_tn_record")
    # skip writes to unmounted USB paths; if mounted, ensure directory exists
    if db_path.startswith("/media"):
        if not is_mounted(db_path):
            logger.debug(f"Skipping TN record write to unmounted {db_path}")
            return
        # create parent directory if missing
        parent_dir = os.path.dirname(db_path)
        try:
            os.makedirs(parent_dir, exist_ok=True)
        except Exception as e:
            logger.error("Failed to create directory %s for USB backup DB %s: %s", parent_dir, db_path, e)
            return
    try:
        with get_db_connection(db_path, timeout=3) as conn2:
            cur2 = conn2.cursor()
            # only insert if this is the first pass and no prior entry exists
            if not db_entry_complete:
                cur2.execute(SQL_STATEMENTS['insert_tn'],
                             (timestamp,
                              finished_serial,
                              finished_serial_date,
                              lhconv, lhconv_date,
                              rhconv, rhconv_date,
                              status))
                conn2.commit()
                db_entry_complete = True
                # clear DB error flag and detailed info on success
                try:
                    plc = globals().get('plc')
                    if plc:
                        plc.write((PLC_TAGS['TN_DB_ERROR'], False))
                        plc.write((PLC_TAGS['DB_ERROR_INFO'], 0))
                except Exception:
                    pass
                logger.info("Data stored in USB backup database: %s", db_path)
    except Exception as e:
        logger.error("Failed to write TN record to USB backup DB %s: %s", db_path, e)
        # set DB error flag and detailed info on failure
        try:
            plc = globals().get('plc')
            if plc:
                plc.write((PLC_TAGS['TN_DB_ERROR'], True))
                plc.write((PLC_TAGS['DB_ERROR_INFO'], DB_ERROR_INFO_CODES['WRITE_ERROR']))
        except Exception:
            pass
    logger.debug("Exiting function: insert_tn_record")


def replicate_tn_to_backups(timestamp: str, finished_serial: Any, finished_serial_date: Any,
                            lhconv: Any, lhconv_date: Any, rhconv: Any, rhconv_date: Any, status: str) -> None:
    """
    Write the same TN record into both USB backup databases.
    """
    for dbp in USB_DB_BACKUPS:
        insert_tn_record(dbp, timestamp, finished_serial, finished_serial_date, lhconv, lhconv_date, rhconv, rhconv_date, status)


def handle_fail(lh_pass: bool, rh_pass: bool, plc: LogixDriver,
                cursor: sqlite3.Cursor, lhconv: Any, rhconv: Any) -> None:
    # 1) Leak-test failure branch
    leak = plc.read(PLC_TAGS['LEAK_TEST_FAIL'])
    if leak and leak.value:
        status = "Part Failed Leaktest"
        logger.info("Leak Test Failed - No database entry created")
        write_plc_message(plc, status)
        return

    # 2) Duplicate-SN logic
    if not lh_pass and not rh_pass:
        base_status = "LH & RH TN Duplicate - Failed"
        cursor.execute(
            "SELECT finished_serial FROM tn "
            "WHERE component_serial1=? OR component_serial2=? "
            "ORDER BY id ASC LIMIT 1",
            (lhconv, rhconv)
        )
    elif not lh_pass:
        base_status = "LH TN Duplicate - Failed"
        cursor.execute(
            "SELECT finished_serial FROM tn "
            "WHERE component_serial1=? "
            "ORDER BY id ASC LIMIT 1",
            (lhconv,)
        )
    else:
        base_status = "RH TN Duplicate - Failed"
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
    write_plc_message(plc, status)
    if wait_for_fail_or_reset(plc):
        ts = time.strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute(SQL_STATEMENTS['insert_tn'],
                       (ts,
                        'N/A', extract_julian('N/A'),
                        lhconv, extract_julian(lhconv),
                        rhconv, extract_julian(rhconv),
                        status))
        cursor.connection.commit()
        logger.error("Failed TN Check - Data stored in database")
        replicate_tn_to_backups(ts, 'N/A', lhconv, rhconv, status)


def monitor_and_update(plc_ip_address: str, db_file: str) -> None:
    logger.debug("Starting monitor_and_update function with PLC IP: %s and DB file: %s", plc_ip_address, db_file)
     # create a persistent driver and open session once
    # set a very long timeout (disable practical CIP timeout)
    plc = LogixDriver(plc_ip_address, timeout=86400.0)
    try:
        plc.open()
        # disable OS-level socket timeouts
        plc._cli.socket.settimeout(None)
        logger.info("PLC connection established.")
        globals()['plc'] = plc
    except Exception as e:
        logger.error("Initial PLC open failed: %s", e)
    logger.info("Entering monitor_and_update with PLC=%s, DB=%s", plc_ip_address, db_file)
    # open a single persistent SQLite connection (WAL, NORMAL sync)
    try:
        conn = get_db_connection(db_file, timeout=3)
        logger.debug("Persistent DB connection opened for %s", db_file)
    except Exception as e:
        logger.error("Failed to open persistent DB connection: %s", e)
        sys.exit(1)
    # reset retry delay on fresh loop
    current_retry = RETRY_DELAY
    last_error_time = None
    while True:
        cycle_start = time.time()
         # reset back-off after stability
        if last_error_time and (time.time() - last_error_time) > RESET_BACKOFF_TIMEOUT:
            current_retry = RETRY_DELAY
            last_error_time = None  
            logger.debug("Reset retry delay to %ds after stability period", RETRY_DELAY)
        try:
            # main scan loop
            db_entry_complete = False  # Initialize the flag
            while True:
                # clear pass/fail and TLA flags on sequence reset
                if plc.read(PLC_TAGS['SEQ_STEP']).value == 0:
                    logger.debug("SEQ_STEP is 0. Resetting flags and db_entry_complete.")
                    db_entry_complete = False
                    plc.write((PLC_TAGS['TN_CHECK_PASS'], False))
                    plc.write((PLC_TAGS['TN_CHECK_FAIL'], False))
                    plc.write((PLC_TAGS['TLA_SN_PASS'], False))
                    plc.write((PLC_TAGS['TN_TLA_SN_CHECK_PASS'], False))
                    plc.write((PLC_TAGS['REWORK_LABEL_FINISHED'], ""))
                    plc.write((PLC_TAGS['TN_MANUAL_ENTRY'], ""))
                    plc.write((PLC_TAGS['TN_MESSAGE'], ""))
                    plc.write((PLC_TAGS['SCAN_COMPLETE'], False))
                    continue
                # wait for new SCAN_COMPLETE event (false→true)
                logger.debug("Waiting for SCAN_COMPLETE tag to transition to True.")
                wait_for_tag(plc, 'SCAN_COMPLETE')
                logger.debug("SCAN_COMPLETE tag is True. Proceeding with batch read.")
                # batch read converter values and record start time
                read_start = time.time()
                # reuse persistent connection
                cursor = conn.cursor()
                tags = [PLC_TAGS['LH_CONV'], PLC_TAGS['RH_CONV'], PLC_TAGS['FIRST_PIECE_CHECK']]
                results = [plc.read(tag) for tag in tags]
                read_latency = time.time() - read_start
                # record metrics for read latency
                metrics_logger.info(json.dumps({
                    "timestamp": datetime.utcnow().isoformat(),
                    "metric": "read_latency_s", "value": read_latency
                }))
                lhconv = results[0].value
                rhconv = results[1].value
                fpc_val = results[2].value

                # 0) First-piece check
                fpc = plc.read(PLC_TAGS['FIRST_PIECE_CHECK'])
                if fpc_val:
                    logger.info("First Piece Check - test part detected")
                    logger.debug("Mode 0: First-Piece Check detected.")

                    # Read LH_CONV and RH_CONV values from PLC
                    lhconv = plc.read(PLC_TAGS['LH_CONV']).value
                    rhconv = plc.read(PLC_TAGS['RH_CONV']).value

                    # Always set pass status for first-piece check
                    set_pass(plc, True)

                    # Check if SEQ_STEP == 143 for TLA serial number checks
                    if plc.read(PLC_TAGS['SEQ_STEP']).value == 143:
                        finished_serial = plc.read(PLC_TAGS['FINISHED_SERIAL']).value
                        ts = time.strftime('%Y-%m-%d %H:%M:%S')

                        # Check for leak test failure
                        leak = plc.read(PLC_TAGS['LEAK_TEST_FAIL'])
                        if leak and leak.value:
                            status = "First Piece Check - Failed Leak Test"
                        else:
                            status = "First Piece Check - Passed"

                        # Calculate Julian dates for TLA duplicate check
                        lhconv_date = extract_julian(lhconv)
                        rhconv_date = extract_julian(rhconv)

                        # Wait for SEQ_STEP to transition to 143 or 0
                        while True:
                            seq_step = plc.read(PLC_TAGS['SEQ_STEP']).value

                            if seq_step == 0:
                                # Reset the cycle if SEQ_STEP == 0
                                logger.info("SEQ_STEP reset to 0. Exiting first-piece check.")
                                break

                            if seq_step == 143:
                                # Perform TLA serial number checks and increment if duplicates are found
                                while True:
                                    cursor.execute(
                                        "SELECT COUNT(*) FROM tn WHERE finished_serial=?",
                                        (finished_serial,)
                                    )
                                    duplicate_count = cursor.fetchone()[0]

                                    if duplicate_count == 0:
                                        # No duplicates, proceed to insert database entry
                                        try:
                                            cursor.execute(SQL_STATEMENTS['insert_tn'],
                                                           (ts,
                                                            finished_serial,
                                                            extract_julian(finished_serial),
                                                            lhconv, lhconv_date,
                                                            rhconv, rhconv_date,
                                                            status))
                                            conn.commit()
                                            logger.info(f"Data stored in local database ({status})")
                                            replicate_tn_to_backups(ts, finished_serial, extract_julian(finished_serial), lhconv, lhconv_date, rhconv, rhconv_date, status)
                                            db_entry_complete = True
                                            break
                                        except sqlite3.IntegrityError:
                                            logger.info("Attempted to insert a duplicate entry into the database. Skipping insertion.")
                                    else:
                                        # Increment the serial number if duplicates are found
                                        logger.info(f"Duplicate TLA serial detected for {finished_serial}. Incrementing serial number.")
                                        part_select = plc.read(PLC_TAGS['PART_SELECT']).value
                                        current_serial = plc.read(f"FIX_513D.Serial_Number[{part_select}]").value
                                        plc.write((f"FIX_513D.Serial_Number[{part_select}]", current_serial + 1))
                                        finished_serial = f"{finished_serial}_DUP{duplicate_count}"
                                        time.sleep(POLL_INTERVAL)
                                break

                            time.sleep(POLL_INTERVAL)
                        continue

                # 1) Rework Mode
                rework = plc.read(PLC_TAGS['REWORK_MODE'])
                if rework and rework.value:
                    logger.debug("Mode 1: Rework Mode detected.")
                    logger.debug("Rework Mode detected. LH_CONV: %s, RH_CONV: %s", lhconv, rhconv)
                    # wait for rework label finished tag or reset
                    while True:
                        # abort on sequence reset
                        seq = plc.read(PLC_TAGS['SEQ_STEP'])
                        if seq and seq.value == 0:
                            label_fs = None
                            break
                        tag = plc.read(PLC_TAGS['REWORK_LABEL_FINISHED'])
                        if tag and tag.value:
                            label_fs = tag.value
                            break
                        time.sleep(POLL_INTERVAL)
                    # timestamp for record
                    ts = time.strftime('%Y-%m-%d %H:%M:%S')
                    if not label_fs:
                        # aborted or no label provided
                        continue
                    # validate inputs
                    if not label_fs or not lhconv or not rhconv:
                        plc.write((PLC_TAGS['TN_DB_ERROR'], True))
                        plc.write((PLC_TAGS['DB_ERROR_INFO'], DB_ERROR_INFO_CODES['WRITE_ERROR']))
                        plc.write((PLC_TAGS['DB_ERROR_INFO'], DB_ERROR_INFO_CODES['WRITE_ERROR']))
                        set_pass(plc, False)
                        logger.error("Rework inputs invalid: FS=%s, LH=%s, RH=%s", label_fs, lhconv, rhconv)
                        if wait_for_fail_or_reset(plc):
                            cursor.execute(SQL_STATEMENTS['insert_tn'],
                                           (ts,
                                            label_fs or 'N/A', extract_julian(label_fs or 'N/A'),
                                            lhconv or 'N/A', extract_julian(lhconv or 'N/A'),
                                            rhconv or 'N/A', extract_julian(rhconv or 'N/A'),
                                            'Rework Rerun Fail'))
                            conn.commit(); replicate_tn_to_backups(ts, label_fs or 'N/A', extract_julian(label_fs or 'N/A'), lhconv or 'N/A', extract_julian(lhconv or 'N/A'), rhconv or 'N/A', extract_julian(rhconv or 'N/A'), 'Rework Rerun Fail')
                        continue
                    # DB lookup
                    try:
                        cursor.execute(
                            "SELECT id FROM tn WHERE finished_serial=? AND component_serial1=? AND component_serial2=?",
                            (label_fs, lhconv, rhconv)
                        )
                        rows = cursor.fetchall()
                        cursor.execute(
                            "SELECT COUNT(*) FROM tn WHERE finished_serial=? OR component_serial1=? OR component_serial2=?",
                            (label_fs, lhconv, rhconv)
                        )
                        total = cursor.fetchone()[0]
                        valid = (len(rows) == 1 and total == 1)
                    except Exception as e:
                        plc.write((PLC_TAGS['TN_DB_ERROR'], True))
                        plc.write((PLC_TAGS['DB_ERROR_INFO'], DB_ERROR_INFO_CODES['REWORK_LOOKUP_ERROR']))
                        logger.exception("Rework DB lookup error")
                        set_pass(plc, False)
                        if wait_for_fail_or_reset(plc):
                            cursor.execute(SQL_STATEMENTS['insert_tn'],
                                           (ts,
                                            label_fs, extract_julian(label_fs),
                                            lhconv, extract_julian(lhconv),
                                            rhconv, extract_julian(rhconv),
                                            'Rework Rerun Fail'))
                            conn.commit(); replicate_tn_to_backups(ts, label_fs, extract_julian(label_fs), lhconv, extract_julian(lhconv), rhconv, extract_julian(rhconv), 'Rework Rerun Fail')
                        continue

                    # finalize pass/fail
                    if valid:
                        row_id = rows[0][0]
                        logger.info("Rework Rerun Pass: matched record %s", row_id)
                        set_pass(plc, True)
                        if wait_for_datastore_or_reset(plc):
                            cursor.execute(
                                SQL_STATEMENTS['insert_tn'],
                                (ts,
                                 label_fs, extract_julian(label_fs),
                                 lhconv, extract_julian(lhconv),
                                 rhconv, extract_julian(rhconv),
                                 'Rework Rerun')
                            )
                            conn.commit()
                            replicate_tn_to_backups(ts, label_fs, extract_julian(label_fs), lhconv, extract_julian(lhconv), rhconv, extract_julian(rhconv), 'Rework Rerun')
                    else:
                        logger.error("Rework Rerun Fail: no single matching record for FS=%s, LH=%s, RH=%s", label_fs, lhconv, rhconv)
                        set_pass(plc, False)
                        if wait_for_fail_or_reset(plc):
                            cursor.execute(
                                SQL_STATEMENTS['insert_tn'],
                                (ts,
                                 label_fs or 'N/A', extract_julian(label_fs or 'N/A'),
                                 lhconv, extract_julian(lhconv),
                                 rhconv, extract_julian(rhconv),
                                 'Rework Rerun Fail')
                            )
                            conn.commit()
                            replicate_tn_to_backups(ts, label_fs or 'N/A', extract_julian(label_fs or 'N/A'), lhconv, extract_julian(lhconv), rhconv, extract_julian(rhconv), 'Rework Rerun Fail')
                    continue

                # 3) Normal pass / fail
                logger.debug("Mode 2: Normal Pass/Fail mode.")
                logger.debug("Normal Pass/Fail mode. LH_CONV: %s, RH_CONV: %s", lhconv, rhconv)
                # Normal pass/fail logic
                lhconv_date = extract_julian(lhconv)
                rhconv_date = extract_julian(rhconv)

                # Check uniqueness of component_serial1 and component_serial2
                cursor.execute(
                    "SELECT COUNT(*) FROM tn WHERE component_serial1_date=?",
                    (lhconv_date,)
                )
                lh_match_count = cursor.fetchone()[0]

                cursor.execute(
                    "SELECT COUNT(*) FROM tn WHERE component_serial2_date=?",
                    (rhconv_date,)
                )
                rh_match_count = cursor.fetchone()[0]

                lh_pass = lh_match_count == 0
                rh_pass = rh_match_count == 0

                if lh_pass and rh_pass:
                    set_pass(plc, True)
                    logger.debug("Both LH_CONV and RH_CONV passed TN check.")

                    # Prevent duplicate database entries by using db_entry_complete flag
                    if not db_entry_complete:
                        try:
                            logger.debug("Inserting record into database. LH_CONV: %s, RH_CONV: %s", lhconv, rhconv)
                            cursor.execute(
                                SQL_STATEMENTS['insert_tn'],
                                (ts, finished_serial, extract_julian(finished_serial), lhconv, lhconv_date, rhconv, rhconv_date, 'Passed')
                            )
                            conn.commit()
                            replicate_tn_to_backups(ts, finished_serial, extract_julian(finished_serial), lhconv, lhconv_date, rhconv, rhconv_date, 'Passed')
                            logger.info("Database entry created for pass: Serial=%s", finished_serial)
                            db_entry_complete = True
                        except sqlite3.IntegrityError:
                            logger.debug("Duplicate entry detected. Skipping insertion.")
                else:
                    set_pass(plc, False)
                    logger.debug("Converter serial number check failed. LH_PASS: %s, RH_PASS: %s", lh_pass, rh_pass)
                    handle_fail(lh_pass, rh_pass, plc, cursor, lhconv, rhconv)

        except KeyboardInterrupt:
            logger.info("Interrupted by user, exiting.")
            plc.close()
            sys.exit(0)
        except CommError as e_comm:
            # track communication errors
            global error_comm_count
            error_comm_count += 1
            metrics_logger.info(json.dumps({
                "timestamp": datetime.utcnow().isoformat(),
                "metric": "comm_error_count", "value": error_comm_count
            }))
            logger.debug("CommError: %s – reconnecting immediately", e_comm)
            last_error_time = time.time()
            # attempt immediate reconnect
            try:
                plc.close()
            except Exception:
                pass
            try:
                plc.open()
            except Exception as e:
                logger.debug("Reopen PLC failed: %s", e)
                continue
            continue
        except Exception:
            global error_unexpected_count
            error_unexpected_count += 1
            metrics_logger.info(json.dumps({
                "timestamp": datetime.utcnow().isoformat(),
                "metric": "unexpected_error_count", "value": error_unexpected_count
            }))
            logger.exception("Unexpected error – retrying in %ds", current_retry)
            last_error_time = time.time()
            # immediate retry without delay
            # time.sleep(current_retry)
            # increase retry delay up to cap
            current_retry = min(current_retry * 2, MAX_RETRY_DELAY)
            continue
    logger.debug("Exiting function: monitor_and_update")


def ensure_all_dbs_initialized():
    """
    Ensure that all database paths (local, usbdrive, usbdrive2) are initialized with the correct schema.
    """
    db_paths = [default_local_db, USB_DB_BACKUP, USB_DB_BACKUP2]
    for db_path in db_paths:
        logger.info("Ensuring database schema for %s", db_path)
        ensure_db_schema(db_path)
        logger.info("Database schema ensured for %s", db_path)

def main() -> None:
    parser = argparse.ArgumentParser(description="TN barcode converter serial checker")
    parser.add_argument("--plc", default="10.131.201.60", help="PLC IP address")
    parser.add_argument("--db", default=default_local_db, help="Path to SQLite DB file")
    args = parser.parse_args()

    db_file = os.path.expanduser(args.db)
    # ensure the local database file and 'tn' table exist
    logger.info("Calling ensure_db_schema for %s", db_file)
    ensure_db_schema(db_file)
    logger.info("ensure_db_schema completed for %s", db_file)
    logger.info("Starting tnpy: PLC=%s, DB=%s", args.plc, db_file)
    logger.info("Calling sync_db_from_backup for %s", db_file)
    sync_db_from_backup(db_file)
    logger.info("sync_db_from_backup completed for %s", db_file)
    # start background watcher for USB hotplug one-way sync
    for idx, path in enumerate(USB_DB_BACKUPS, start=1):
        name = f"usb{idx}"
        threading.Thread(
            target=watch_usb_and_sync,
            args=(db_file, path, name),
            daemon=True
        ).start()
    logger.info("Calling monitor_and_update with PLC=%s, DB=%s", args.plc, db_file)
    monitor_and_update(args.plc, db_file)


if __name__ == "__main__":
    ensure_all_dbs_initialized()
    main()