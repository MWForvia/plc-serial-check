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
from typing import Any, Optional
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
    'LABEL_READ_COMPLETE':   'FIX_513D.Label_Barcode.READ_COMPLETE',
    'LABEL_BARCODE_EXTRACT': 'FIX_513D.Label_Barcode.EXTRACT[2]',
    'LABEL_FAULT':           'FIX_513D.Label_Barcode.FAULT_TIMER.DN',
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
    """Write a message to the TN.Message PLC tag using .len and .data fields."""
    try:
        # Ensure the message is within the 200-character limit
        formatted_message = message[:200]
        # Write the length and data separately
        plc.write((PLC_TAGS['TN_MESSAGE'] + '.len', len(formatted_message)))
        plc.write((PLC_TAGS['TN_MESSAGE'] + '.data', formatted_message))
    except Exception:
        logger.exception("Failed to write PLC message")

# Ensure local DB file exists and has the required schema
def ensure_db_schema(db_path: str) -> None:
    """
    Ensure the database file exists and has the required schema, with error reporting.
    """
    parent = os.path.dirname(db_path) or '.'
    try:
        os.makedirs(parent, exist_ok=True)
    except Exception as e:
        logger.error("Failed to create directory for DB %s: %s", parent, e)
        # Report schema error to PLC
        try:
            plc = globals().get('plc')
            if plc:
                plc.write((PLC_TAGS['TN_DB_ERROR'], True))
                plc.write((PLC_TAGS['DB_ERROR_INFO'], DB_ERROR_INFO_CODES['SCHEMA_ERROR']))
        except Exception:
            pass
        return

    try:
        with get_db_connection(db_path) as conn:
            conn.execute(
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
            conn.commit()
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tn_finished_date ON tn(finished_serial_date);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tn_finished_date_serial ON tn(finished_serial_date, finished_serial);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tn_component1_date ON tn(component_serial1_date);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tn_component1_date_serial ON tn(component_serial1_date, component_serial1);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tn_component2_date ON tn(component_serial2_date);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tn_component2_date_serial ON tn(component_serial2_date, component_serial2);")
            conn.commit()
    except Exception as e:
        logger.error("Failed to ensure schema for database %s: %s", db_path, e)
        # Report schema error to PLC
        try:
            plc = globals().get('plc')
            if plc:
                plc.write((PLC_TAGS['TN_DB_ERROR'], True))
                plc.write((PLC_TAGS['DB_ERROR_INFO'], DB_ERROR_INFO_CODES['SCHEMA_ERROR']))
        except Exception:
            pass

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
                        init_conn.execute("CREATE INDEX IF NOT EXISTS idx_tn_finished_date_serial ON tn(finished_serial_date, finished_serial);")
                        init_conn.execute("CREATE INDEX IF NOT EXISTS idx_tn_component1_date ON tn(component_serial1_date);")
                        init_conn.execute("CREATE INDEX IF NOT EXISTS idx_tn_component1_date_serial ON tn(component_serial1_date, component_serial1);")
                        init_conn.execute("CREATE INDEX IF NOT EXISTS idx_tn_component2_date ON tn(component_serial2_date);")
                        init_conn.execute("CREATE INDEX IF NOT EXISTS idx_tn_component2_date_serial ON tn(component_serial2_date, component_serial2);")
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
                    "date TEXT, finished_serial TEXT, finished_serial_date TEXT, "
                    "component_serial1 TEXT, component_serial1_date TEXT, "
                    "component_serial2 TEXT, component_serial2_date TEXT, status TEXT)"
                )
                tgt_cur.execute("SELECT MAX(id) FROM tn")
                max_id = tgt_cur.fetchone()[0] or 0
                tgt_conn.execute("ATTACH DATABASE ? AS src", (src_path,))
                new_count = tgt_conn.execute(
                    "SELECT COUNT(*) FROM src.tn WHERE id > ?", (max_id,)
                ).fetchone()[0]
                if new_count > 0:
                    tgt_conn.execute(
                        "INSERT INTO tn(date, finished_serial, finished_serial_date, component_serial1, component_serial1_date, "
                        "component_serial2, component_serial2_date, status) "
                        "SELECT date, finished_serial, finished_serial_date, component_serial1, component_serial1_date, "
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
                # Ensure indexes used by lookups exist on fresh targets
                init_conn.execute("CREATE INDEX IF NOT EXISTS idx_tn_finished_date ON tn(finished_serial_date);")
                init_conn.execute("CREATE INDEX IF NOT EXISTS idx_tn_finished_date_serial ON tn(finished_serial_date, finished_serial);")
                init_conn.execute("CREATE INDEX IF NOT EXISTS idx_tn_component1_date ON tn(component_serial1_date);")
                init_conn.execute("CREATE INDEX IF NOT EXISTS idx_tn_component1_date_serial ON tn(component_serial1_date, component_serial1);")
                init_conn.execute("CREATE INDEX IF NOT EXISTS idx_tn_component2_date ON tn(component_serial2_date);")
                init_conn.execute("CREATE INDEX IF NOT EXISTS idx_tn_component2_date_serial ON tn(component_serial2_date, component_serial2);")
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
                    "INSERT INTO tn(date, finished_serial, finished_serial_date, component_serial1, component_serial1_date, "
                    "component_serial2, component_serial2_date, status) "
                    "SELECT date, finished_serial, finished_serial_date, component_serial1, component_serial1_date, "
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
    while True:
        val = plc.read(name)
        if val and val.value:
            return  # Proceed immediately if the tag is already true
        time.sleep(POLL_INTERVAL)


def wait_for_datastore_or_reset(plc: LogixDriver) -> bool:
    """
    Check if DATASTORE is already true or wait for it, exit on reset.
    """
    while True:
        rs = plc.read(PLC_TAGS['SEQ_STEP'])
        if rs and rs.value == 0:
            logger.info("Cycle Reset.")
            return False  # Exit on reset
        ds = plc.read(PLC_TAGS['DATASTORE'])
        if ds and ds.value:
            return True  # Proceed if DATASTORE is already true
        time.sleep(FAST_POLL_INTERVAL)


def wait_for_fail_or_reset(plc: LogixDriver) -> bool:
    """
    Level-detect the PART_FAIL tag: return True as soon as it's observed true, or exit on reset.
    """
    while True:
        rs = plc.read(PLC_TAGS['SEQ_STEP'])
        if rs and rs.value == 0:
            logger.info("Cycle Reset.")
            return False
        fl = plc.read(PLC_TAGS['PART_FAIL'])
        if fl and fl.value:
            return True
        time.sleep(FAST_POLL_INTERVAL)


def check_converter_sn(cursor: sqlite3.Cursor, column: str, sn: Any, label: str) -> bool:
    julian_date = extract_julian(sn)
    cursor.execute(
        f"SELECT 1 FROM tn WHERE {column}_date = ? AND {column} = ?",
        (julian_date, sn)
    )
    if cursor.fetchone():
        logger.warning("%s Converter SN Failed: %s", label, sn)
        return False
    logger.info("%s Converter SN Passed: %s", label, sn)
    return True


# leak-test rerun logic removed


def set_pass(plc: LogixDriver, passed: bool) -> None:
    plc.write((PLC_TAGS['TN_CHECK_PASS'], passed))
    plc.write((PLC_TAGS['TN_CHECK_FAIL'], not passed))


def ensure_unique_finished_serial(plc: LogixDriver, cursor: sqlite3.Cursor, max_attempts: int = 1000, sleep_s: float = 0.1) -> Optional[str]:
    """
    At SEQ_STEP == 143, verify finished_serial from SERIAL_HOLDER is unique by
    (finished_serial_date, finished_serial). If duplicate, increment SERIAL_NUMBER[PART_SELECT]
    and retry until unique or max_attempts reached.

    On success, sets TN_TLA_SN_CHECK_PASS = True and returns the final unique serial string.
    On failure, returns None.
    """
    try:
        for attempt in range(1, max_attempts + 1):
            tla_sn = (plc.read(PLC_TAGS['SERIAL_HOLDER']).value or "").strip()
            if not tla_sn:
                time.sleep(sleep_s)
                continue
            date_val = extract_julian(tla_sn)
            cursor.execute(
                "SELECT COUNT(*) FROM tn WHERE finished_serial_date = ? AND finished_serial = ?",
                (date_val, tla_sn)
            )
            dup_count = cursor.fetchone()[0]
            if dup_count == 0:
                plc.write((PLC_TAGS['TN_TLA_SN_CHECK_PASS'], True))
                logger.info("TLA unique: %s (after %d attempt(s))", tla_sn, attempt)
                return tla_sn

            # Duplicate, increment PLC serial number for current part selection
            part_select = plc.read(PLC_TAGS['PART_SELECT']).value or 0
            current_sn_num = plc.read(f"{PLC_TAGS['SERIAL_NUMBER']}[{part_select}]").value or 0
            next_sn_num = current_sn_num + 1
            plc.write((f"{PLC_TAGS['SERIAL_NUMBER']}[{part_select}]", next_sn_num))
            logger.info("TLA duplicate detected for %s; incremented SERIAL_NUMBER[%s] to %s (attempt %d)",
                        tla_sn, part_select, next_sn_num, attempt)
            time.sleep(sleep_s)

        logger.error("Failed to resolve TLA duplicate within %d attempts", max_attempts)
        return None
    except Exception:
        logger.exception("Error during TLA uniqueness resolution loop")
        return None


def insert_tn_record(db_path: str, timestamp: str, finished_serial: Any,
                     finished_serial_date: Any, lhconv: Any, lhconv_date: Any,
                     rhconv: Any, rhconv_date: Any, status: str) -> None:
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
            cur2.execute(SQL_STATEMENTS['insert_tn'],
                         (timestamp,
                          finished_serial,
                          finished_serial_date,
                          lhconv, lhconv_date,
                          rhconv, rhconv_date,
                          status))
            conn2.commit()
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


def replicate_tn_to_backups(timestamp: str, finished_serial: Any, finished_serial_date: Any,
                            lhconv: Any, lhconv_date: Any, rhconv: Any, rhconv_date: Any, status: str) -> None:
    """
    Write the same TN record into all database storage locations, with error reporting.
    """
    for dbp in [default_local_db, USB_DB_BACKUP, USB_DB_BACKUP2]:
        try:
            ensure_db_schema(dbp)  # Ensure schema before writing
            insert_tn_record(dbp, timestamp, finished_serial, finished_serial_date, lhconv, lhconv_date, rhconv, rhconv_date, status)
        except Exception as e:
            logger.error("Failed to replicate TN record to %s: %s", dbp, e)
            # Report write error to PLC
            try:
                plc = globals().get('plc')
                if plc:
                    plc.write((PLC_TAGS['TN_DB_ERROR'], True))
                    plc.write((PLC_TAGS['DB_ERROR_INFO'], DB_ERROR_INFO_CODES['WRITE_ERROR']))
            except Exception:
                pass


def handle_fail(lh_pass: bool, rh_pass: bool, plc: LogixDriver,
                cursor: sqlite3.Cursor, lhconv: Any, rhconv: Any) -> None:
    # Leak-test failures are no longer recorded in the DB; duplicates still are handled below

    if not lh_pass and not rh_pass:
        base_status = "LH & RH TN Duplicate - Failed"
        cursor.execute(
            "SELECT finished_serial FROM tn WHERE (component_serial1_date = ? AND component_serial1 = ?) OR (component_serial2_date = ? AND component_serial2 = ?) ORDER BY id ASC LIMIT 1",
            (extract_julian(lhconv), lhconv, extract_julian(rhconv), rhconv)
        )
    elif not lh_pass:
        base_status = "LH TN Duplicate - Failed"
        cursor.execute(
            "SELECT finished_serial FROM tn WHERE component_serial1_date = ? AND component_serial1 = ? ORDER BY id ASC LIMIT 1",
            (extract_julian(lhconv), lhconv)
        )
    else:
        base_status = "RH TN Duplicate - Failed"
        cursor.execute(
            "SELECT finished_serial FROM tn WHERE component_serial2_date = ? AND component_serial2 = ? ORDER BY id ASC LIMIT 1",
            (extract_julian(rhconv), rhconv)
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
        replicate_tn_to_backups(ts, 'N/A', extract_julian('N/A'), lhconv, extract_julian(lhconv), rhconv, extract_julian(rhconv), status)
        logger.info("Exiting handle_fail")
        return

    logger.info("Exiting handle_fail - no action taken")


def monitor_and_update(plc_ip_address: str, db_file: str) -> None:

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
            while True:
                # clear pass/fail and TLA flags on sequence reset
                if plc.read(PLC_TAGS['SEQ_STEP']).value == 0:
                    plc.write((PLC_TAGS['TN_CHECK_PASS'], False))
                    plc.write((PLC_TAGS['TN_CHECK_FAIL'], False))
                    plc.write((PLC_TAGS['TN_TLA_SN_CHECK_PASS'], False))
                    plc.write((PLC_TAGS['REWORK_LABEL_FINISHED'], ""))
                    plc.write((PLC_TAGS['TN_MANUAL_ENTRY'], ""))
                    plc.write((PLC_TAGS['TN_MESSAGE'], ""))
                    plc.write((PLC_TAGS['SCAN_COMPLETE'], False))
                    continue
                # wait for new SCAN_COMPLETE event (false→true)
                wait_for_tag(plc, 'SCAN_COMPLETE')
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
                    set_pass(plc, True)
                    # Poll for reset (0), pass (143), or leak test fail
                    while True:
                        rs = plc.read(PLC_TAGS['SEQ_STEP'])
                        leak_fail = plc.read(PLC_TAGS['LEAK_TEST_FAIL'])
                        if leak_fail and leak_fail.value:
                            logger.info("Leak Test Failed during FPC - no DB entry created")
                            break
                        if rs and rs.value == 0:
                            logger.info("Cycle Reset.")
                            break
                        if rs and rs.value == 143:
                            # Ensure finished serial is unique; set PLC flag and capture TLA
                            tla_sn = ensure_unique_finished_serial(plc, cursor)
                            if tla_sn:
                                ts = time.strftime('%Y-%m-%d %H:%M:%S')
                                cursor.execute(SQL_STATEMENTS['insert_tn'],
                                               (ts,
                                                tla_sn,
                                                extract_julian(tla_sn),
                                                lhconv, extract_julian(lhconv),
                                                rhconv, extract_julian(rhconv),
                                                'First Piece Check'))
                                conn.commit()
                                logger.info("Data stored in local database (First Piece Check)")
                                replicate_tn_to_backups(ts, tla_sn, extract_julian(tla_sn), lhconv, extract_julian(lhconv), rhconv, extract_julian(rhconv), 'First Piece Check')
                            break
                        time.sleep(POLL_INTERVAL)
                    continue

                # 1) Leak-test rerun logic removed entirely

                # 2) Rework Mode
                rework = plc.read(PLC_TAGS['REWORK_MODE'])
                if rework and rework.value:
                    # wait for rework label finished tag or reset
                    while True:
                        # abort on sequence reset
                        seq = plc.read(PLC_TAGS['SEQ_STEP'])
                        if seq and seq.value == 0:
                            logger.info("Cycle Reset.")
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
                        # Use date-index narrowing for each field, then exact value match for consistency and speed
                        cursor.execute(
                            "SELECT id FROM tn WHERE finished_serial_date=? AND finished_serial=? "
                            "AND component_serial1_date=? AND component_serial1=? "
                            "AND component_serial2_date=? AND component_serial2=?",
                            (
                                extract_julian(label_fs), label_fs,
                                extract_julian(lhconv), lhconv,
                                extract_julian(rhconv), rhconv,
                            )
                        )
                        rows = cursor.fetchall()
                        # Keep existing uniqueness heuristic but make it date-indexed for consistency
                        cursor.execute(
                            "SELECT COUNT(*) FROM tn WHERE (finished_serial_date=? AND finished_serial=?) "
                            "OR (component_serial1_date=? AND component_serial1=?) "
                            "OR (component_serial2_date=? AND component_serial2=?)",
                            (
                                extract_julian(label_fs), label_fs,
                                extract_julian(lhconv), lhconv,
                                extract_julian(rhconv), rhconv,
                            )
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
                        # Rework uses datastore gating; skip DB insert if leak test failed
                        if wait_for_datastore_or_reset(plc):
                            leak_fail = plc.read(PLC_TAGS['LEAK_TEST_FAIL'])
                            if leak_fail and leak_fail.value:
                                logger.info("Leak Test Failed during Rework - no DB entry created")
                            else:
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
                lh_pass = check_converter_sn(cursor, 'component_serial1', lhconv, 'LH')
                rh_pass = check_converter_sn(cursor, 'component_serial2', rhconv, 'RH')

                if lh_pass and rh_pass:
                    set_pass(plc, True)
                    while True:
                        rs = plc.read(PLC_TAGS['SEQ_STEP'])
                        leak_fail = plc.read(PLC_TAGS['LEAK_TEST_FAIL'])
                        if leak_fail and leak_fail.value:
                            logger.info("Leak Test Failed during Normal run - no DB entry created")
                            break

                        if rs and rs.value == 0:
                            logger.info("Cycle Reset.")
                            # Exit loop on reset
                            break

                        if rs and rs.value == 143:
                            # Perform uniqueness loop and set PLC flag; then insert
                            tla_sn = ensure_unique_finished_serial(plc, cursor)
                            if tla_sn:
                                ts = time.strftime('%Y-%m-%d %H:%M:%S')
                                cursor.execute(
                                    SQL_STATEMENTS['insert_tn'],
                                    (ts, tla_sn, extract_julian(tla_sn), lhconv, extract_julian(lhconv), rhconv, extract_julian(rhconv), 'Passed')
                                )
                                conn.commit()
                                replicate_tn_to_backups(ts, tla_sn, extract_julian(tla_sn), lhconv, extract_julian(lhconv), rhconv, extract_julian(rhconv), 'Passed')                                
                                logger.info("Database entry created for pass: Serial=%s", tla_sn)

                        time.sleep(POLL_INTERVAL)
                else:
                    set_pass(plc, False)
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