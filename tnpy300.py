#!/usr/bin/env python3
"""
tnpy.py

This script pulls the serial numbers scanned from the LH and RH converter and compares them to a historical database.
It returns if they are a repeat or not, then adds the data to the db.

Database: tndb300.db
Table: tn
Schema:
    id integer primary key autoincrement,
    date text,
    tla1 text,
    tla1_date text,
    conv1 text,
    conv1_date text,
    tla2 text,
    tla2_date text,
    conv2 text,
    conv2_date text,
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
log_dir = Path.home() / "tnpy_logs300"
log_dir.mkdir(parents=True, exist_ok=True)

# INFO handler
info_log = Path.home() / "tnpy300.log"
info_handler = TimedRotatingFileHandler(
    filename=str(info_log), when="midnight", interval=1, backupCount=0
)
info_handler.suffix = "%Y-%m-%d"
info_handler.namer = lambda name: str(log_dir / Path(name).name)
info_handler.setLevel(logging.INFO)
info_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

# DEBUG handler
debug_log = Path.home() / "tnpy_debug300.log"
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
metrics_log = Path.home() / "tnpy_metrics300.log"
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
default_local_db   = "/home/gap300/tndb300.db"
USB_DB_BACKUP      = "/media/usbdrive/db_backup300/tndb300.db"
USB_DB_BACKUP2     = "/media/usbdrive2/db_backup300/tndb300.db"
USB_DB_BACKUPS     = [USB_DB_BACKUP, USB_DB_BACKUP2]

# Updated PLC tags to match the agreed-upon structure and descriptions

PLC_TAGS = {
    # Control flow
    'TN_CHECK_PASS':         'TN.CHECK_PASS',
    'TN_CHECK_FAIL':         'TN.CHECK_FAIL',
    'SCAN_COMPLETE':         'TN.SCAN_COMPLETE',

    # Error & diagnostic
    'TN_DB_ERROR':           'TN.DB_ERROR',
    'TN_MESSAGE':            'TN.MESSAGE',

    # Torque routine
    'TORQUE_PASS':           'TN.TORQUE_PASS',
    'TORQUE_FAIL':           'TN.TORQUE_FAIL',

    # Sequence step (not part of UDT)
    'SEQ_STEP':              'Local_Step_II_N',

    # Serial numbers and data
    'LH_CONV':               'TN.LH_CONV',
    'RH_CONV':               'TN.RH_CONV',
    'DATASTORE':             'TN.DATASTORE',

    # Rework extensions
    'ALLOW_MULTIPLE_REWORK': 'TN.ALLOW_MULTIPLE_REWORK',

    # Added PLC_TAGS dictionary to define all PLC tags at the top
    'HSCAN_GOOD': 'HScan.Good',
    'REWORK_MODE': 'TN.REWORK_MODE',
    'REWORK_COUNT': 'TN.REWORK_COUNT',
    'SUPERVISOR_KEY': 'TN.SUPERVISOR_KEY',
    'TLA1': 'PN.C2_TLA1_TN',
    'CONV1': 'PN.C2_CONV1_TN',
    'TLA2': 'PN.C2_TLA2_TN',
    'CONV2': 'PN.C2_CONV2_TN'
}

# Updated SQL_STATEMENTS to reflect the new schema
SQL_STATEMENTS = {
    'insert_tn': (
        "INSERT INTO tn (date, tla1, tla1_date, conv1, conv1_date, tla2, tla2_date, conv2, conv2_date, status) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )
}

def extract_julian(serial: Any) -> str:
    """
    Extract 5-digit Julian date from serial (chars 2-6), or empty string if invalid.
    """
    s = str(serial or "")
    return s[2:7] if len(s) >= 7 else ""

def extract_tla_from_barcode(barcode: Any, start_char_1_based: int = 47, length: int = 17) -> Optional[str]:
    """
    Extract the TLA serial from a full barcode string.
    By requirement: take 17 characters starting with character 47 (1-based indexing) of the barcode.
    Returns None if the barcode is too short.
    """
    try:
        s = str(barcode or "")
        start_idx = max(0, (start_char_1_based - 1))  # convert to 0-based
        end_idx = start_idx + max(0, length)
        if len(s) >= end_idx:
            tla = s[start_idx:end_idx]
            logger.info("Extracted TLA from barcode: start=%d length=%d total_len=%d -> %r", start_char_1_based, length, len(s), tla)
            return tla
        logger.error("Barcode too short to extract TLA: needed end_idx=%d, got len=%d; barcode=%r", end_idx, len(s), s)
        return None
    except Exception:
        logger.exception("Failed to extract TLA from barcode")
        return None

# Detailed DB error info codes for TN.DB_ERROR_INFO
DB_ERROR_INFO_CODES = {
    'SCHEMA_ERROR':        1,  # failed to create or migrate schema
    'WRITE_ERROR':         2,  # failed to write record to DB
    'REWORK_LOOKUP_ERROR': 3,  # failed during rework DB lookup
}
# helper to write a message string back to PLC TN.Message tag
def write_plc_message(plc: LogixDriver, message: str) -> None:
    """Write a message to the TN.Message PLC tag using pycomm3 string syntax."""
    try:
        # pycomm3 supports writing STRING tags by passing a Python str directly
        msg = str(message or "")
        # TN.Message is defined as STRING[200]; trim to 200 to avoid oversize errors
        if len(msg) > 200:
            msg = msg[:200]
        result = plc.write((PLC_TAGS['TN_MESSAGE'], msg))
        try:
            if not result or getattr(result, 'error', None):
                logger.error("TN.Message write failed: %s | error=%s", msg, getattr(result, 'error', None))
        except Exception:
            # tolerate variations in driver return types
            pass
    except Exception:
        logger.exception("Failed to write PLC message")


def self_test_tn_message(plc: LogixDriver) -> None:
    """Write a short test to TN.Message, verify read-back, then restore previous value."""
    logger.info("Starting TN.Message self-test")
    prev_val = ""
    try:
        prev = plc.read(PLC_TAGS['TN_MESSAGE'])
        if prev and getattr(prev, 'error', None) is None:
            prev_val = prev.value if isinstance(prev.value, str) else ""
        else:
            logger.error("TN.Message pre-read failed: error=%s", getattr(prev, 'error', None) if prev else 'None')

        test_msg = f"TNPY300 self-test @ {datetime.now().strftime('%H:%M:%S')}"
        # direct write to capture result details
        to_write = test_msg[:200]
        wr = plc.write((PLC_TAGS['TN_MESSAGE'], to_write))
        if not wr or getattr(wr, 'error', None):
            logger.error("TN.Message self-test write failed: error=%s", getattr(wr, 'error', None))

        # brief delay to allow update
        time.sleep(0.1)
        rb = plc.read(PLC_TAGS['TN_MESSAGE'])
        rb_err = getattr(rb, 'error', None) if rb else 'None'
        rb_val = rb.value if rb and rb_err is None else None
        logger.debug("TN.Message read-back type=%s value=%r error=%s", type(rb_val).__name__, rb_val, rb_err)

        ok = isinstance(rb_val, str) and rb_val == to_write
        if ok:
            logger.info("TN.Message self-test PASS")
        else:
            logger.error("TN.Message self-test FAIL: expected=%r got=%r", to_write, rb_val)
    except Exception:
        logger.exception("TN.Message self-test encountered an error")
    finally:
        try:
            # restore previous value to avoid leaving test text on HMI
            wr2 = plc.write((PLC_TAGS['TN_MESSAGE'], prev_val[:200]))
            if not wr2 or getattr(wr2, 'error', None):
                logger.error("TN.Message restore write failed: error=%s", getattr(wr2, 'error', None))
        except Exception:
            logger.exception("Failed to restore TN.Message after self-test")

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
        return

    try:
        with get_db_connection(db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS tn (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT,
                    tla1 TEXT,
                    tla1_date TEXT,
                    conv1 TEXT,
                    conv1_date TEXT,
                    tla2 TEXT,
                    tla2_date TEXT,
                    conv2 TEXT,
                    conv2_date TEXT,
                    status TEXT
                )
                """
            )
            conn.commit()
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tn_tla1_date ON tn(tla1_date);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tn_conv1_date ON tn(conv1_date);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tn_tla2_date ON tn(tla2_date);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tn_conv2_date ON tn(conv2_date);")
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
                                tla1 TEXT,
                                tla1_date TEXT,
                                conv1 TEXT,
                                conv1_date TEXT,
                                tla2 TEXT,
                                tla2_date TEXT,
                                conv2 TEXT,
                                conv2_date TEXT,
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
    logger.info("Called ensure_unique_finished_serial")
    try:
        for attempt in range(1, max_attempts + 1):
            tla_sn = (plc.read(PLC_TAGS['SERIAL_HOLDER']).value or "").strip()
            logger.info("Attempt %d: Read SERIAL_HOLDER = %r", attempt, tla_sn)
            if not tla_sn:
                time.sleep(sleep_s)
                continue
            date_val = extract_julian(tla_sn)
            cursor.execute(
                "SELECT COUNT(*) FROM tn WHERE finished_serial_date = ? AND finished_serial = ?",
                (date_val, tla_sn)
            )
            dup_count = cursor.fetchone()[0]
            logger.info("Attempt %d: dup_count = %d", attempt, dup_count)
            if dup_count == 0:
                logger.info("Setting TN.TLA_SN_CHECK_PASS True")
                result = plc.write((PLC_TAGS['TN_TLA_SN_CHECK_PASS'], True))
                if not result or getattr(result, 'error', None):
                    logger.error("Failed to write TN.TLA_SN_CHECK_PASS: %r", result)
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


def insert_tn_record(db_path: str, timestamp: str, tla1: Any, tla1_date: Any, conv1: Any, conv1_date: Any, tla2: Any, tla2_date: Any, conv2: Any, conv2_date: Any, status: str) -> None:
    """
    Insert a record into the tn table with the updated schema.
    """
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
            cur2.execute(
                "INSERT INTO tn (date, tla1, tla1_date, conv1, conv1_date, tla2, tla2_date, conv2, conv2_date, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (timestamp, tla1, tla1_date, conv1, conv1_date, tla2, tla2_date, conv2, conv2_date, status)
            )
            conn2.commit()
            # clear DB error flag and detailed info on success
            try:
                plc = globals().get('plc')
                if plc:
                    plc.write((PLC_TAGS['TN_DB_ERROR'], False))
                    plc.write((PLC_TAGS['DB_ERROR_INFO'], 0))
            except Exception:
                pass
            label = "USB backup database" if db_path.startswith("/media") else "local database"
            logger.info("Data stored in %s: %s", label, db_path)
    except Exception as e:
        label = "USB backup DB" if db_path.startswith("/media") else "local DB"
        logger.error("Failed to write TN record to %s %s: %s", label, db_path, e)
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
    Write the same TN record into the USB backup databases only.
    The local DB is already written by the caller; avoid duplicating it here.
    """
    for dbp in [USB_DB_BACKUP, USB_DB_BACKUP2]:
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


def record_and_signal_failure(plc: LogixDriver, cursor: sqlite3.Cursor,
                              finished_serial: Any, lhconv: Any, rhconv: Any,
                              status: str) -> None:
    """
    Log and write a single failure entry, then drive PLC to fail step (29).
    This is gated by SEQ_STEP != 29 to prevent repeats; level-trigger only.
    """
    try:
        seq = plc.read(PLC_TAGS['SEQ_STEP'])
        if seq and seq.value == 29:
            return  # Already in fail step; do nothing

        ts = time.strftime('%Y-%m-%d %H:%M:%S')
        fs = finished_serial or 'N/A'
        fs_date = extract_julian(fs)
        lh = lhconv or 'N/A'
        rh = rhconv or 'N/A'
        cursor.execute(
            SQL_STATEMENTS['insert_tn'],
            (ts, fs, fs_date, lh, extract_julian(lh), rh, extract_julian(rh), status)
        )
        cursor.connection.commit()
        replicate_tn_to_backups(ts, fs, fs_date, lh, extract_julian(lh), rh, extract_julian(rh), status)
        logger.error("Failed TN Check - Data stored in database")
        write_plc_message(plc, status)
        # Set PLC fail flags and step
        try:
            plc.write((PLC_TAGS['TN_CHECK_PASS'], False))
            plc.write((PLC_TAGS['TN_CHECK_FAIL'], True))
            plc.write((PLC_TAGS['SEQ_STEP'], 29))
        except Exception:
            logger.exception("Failed to write PLC fail flags/step")
    except Exception:
        logger.exception("Failure record/signaling encountered an error")


def handle_fail(lh_pass: bool, rh_pass: bool, plc: LogixDriver,
                cursor: sqlite3.Cursor, lhconv: Any, rhconv: Any) -> None:
    # Leak-test failures are no longer recorded in the DB; duplicates still are handled
    try:
        seq = plc.read(PLC_TAGS['SEQ_STEP'])
        if seq and seq.value == 29:
            return  # already handled this cycle

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
        # Record once, then drive PLC to step 29
        record_and_signal_failure(plc, cursor, 'N/A', lhconv, rhconv, status)
    except Exception:
        logger.exception("handle_fail encountered an error")


# Reintroduced timeout configuration and logging for PLC connection

def monitor_and_update(plc_ip_address: str, db_file: str) -> None:
    """
    Monitor PLC and update the database with relevant data.
    """
    try:
        with LogixDriver(plc_ip_address) as plc:
            plc._cli.socket.settimeout(None)
            logger.info("PLC connection established.")

            ensure_db_schema(db_file)
            conn = get_db_connection(db_file)
            cursor = conn.cursor()

            logger.info("Starting monitor_and_update loop")
            while True:
                try:
                    hscan_good = plc.read(PLC_TAGS['HSCAN_GOOD']).value
                    if not hscan_good:
                        time.sleep(POLL_INTERVAL)
                        continue

                    rework_mode = plc.read(PLC_TAGS['REWORK_MODE']).value
                    tla1 = plc.read(PLC_TAGS['TLA1']).value
                    conv1 = plc.read(PLC_TAGS['CONV1']).value
                    tla2 = plc.read(PLC_TAGS['TLA2']).value
                    conv2 = plc.read(PLC_TAGS['CONV2']).value

                    tla1_date = extract_julian(tla1)
                    conv1_date = extract_julian(conv1)
                    tla2_date = extract_julian(tla2)
                    conv2_date = extract_julian(conv2)

                    if rework_mode:
                        rework_count = plc.read(PLC_TAGS['REWORK_COUNT']).value
                        supervisor_key = plc.read(PLC_TAGS['SUPERVISOR_KEY']).value

                        cursor.execute(
                            "SELECT COUNT(*) FROM tn WHERE tla1_date = ? AND tla1 = ? AND conv1_date = ? AND conv1 = ? AND tla2_date = ? AND tla2 = ? AND conv2_date = ? AND conv2 = ?",
                            (tla1_date, tla1, conv1_date, conv1, tla2_date, tla2, conv2_date, conv2)
                        )
                        exact_match_count = cursor.fetchone()[0]

                        cursor.execute(
                            "SELECT COUNT(*) FROM tn WHERE tla1 = ? OR conv1 = ? OR tla2 = ? OR conv2 = ?",
                            (tla1, conv1, tla2, conv2)
                        )
                        partial_match_count = cursor.fetchone()[0]

                        if exact_match_count == 1 and partial_match_count == 1:
                            if rework_count == 0 or (rework_count > 0 and supervisor_key):
                                plc.write((PLC_TAGS['TN_CHECK_PASS'], True))
                                logger.info("Rework mode: Serial numbers match a single record. Marking as pass.")
                            else:
                                plc.write((PLC_TAGS['TN_CHECK_FAIL'], True))
                                logger.error("Rework mode: Supervisor key required for rework.")
                        else:
                            plc.write((PLC_TAGS['TN_CHECK_FAIL'], True))
                            logger.error("Rework mode: Serial numbers do not match a single record.")
                    else:
                        cursor.execute("SELECT 1 FROM tn WHERE tla1_date = ? AND tla1 = ?", (tla1_date, tla1))
                        tla1_exists = cursor.fetchone() is not None

                        cursor.execute("SELECT 1 FROM tn WHERE conv1_date = ? AND conv1 = ?", (conv1_date, conv1))
                        conv1_exists = cursor.fetchone() is not None

                        cursor.execute("SELECT 1 FROM tn WHERE tla2_date = ? AND tla2 = ?", (tla2_date, tla2))
                        tla2_exists = cursor.fetchone() is not None

                        cursor.execute("SELECT 1 FROM tn WHERE conv2_date = ? AND conv2 = ?", (conv2_date, conv2))
                        conv2_exists = cursor.fetchone() is not None

                        if not (tla1_exists or conv1_exists or tla2_exists or conv2_exists):
                            plc.write((PLC_TAGS['TN_CHECK_PASS'], True))
                            logger.info("All serial numbers are unique. Marking as pass.")
                            cursor.execute(
                                "INSERT INTO tn (date, tla1, tla1_date, conv1, conv1_date, tla2, tla2_date, conv2, conv2_date, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                (time.strftime('%Y-%m-%d %H:%M:%S'), tla1, tla1_date, conv1, conv1_date, tla2, tla2_date, conv2, conv2_date, "Passed")
                            )
                            conn.commit()
                        else:
                            plc.write((PLC_TAGS['TN_CHECK_FAIL'], True))
                            if tla1_exists or conv1_exists:
                                logger.error("LH Assembly Duplicate Serial Number Failure")
                                cursor.execute(
                                    "INSERT INTO tn (date, tla1, tla1_date, conv1, conv1_date, tla2, tla2_date, conv2, conv2_date, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                    (time.strftime('%Y-%m-%d %H:%M:%S'), tla1, tla1_date, conv1, conv1_date, "N/A - LH Failed", "N/A - LH Failed", "N/A - LH Failed", "N/A - LH Failed", "LH Assembly Duplicate Serial Number Failure")
                                )
                            elif tla2_exists or conv2_exists:
                                logger.error("RH Assembly Duplicate Serial Number Failure")
                                cursor.execute(
                                    "INSERT INTO tn (date, tla1, tla1_date, conv1, conv1_date, tla2, tla2_date, conv2, conv2_date, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                    (time.strftime('%Y-%m-%d %H:%M:%S'), "N/A - RH Failed", "N/A - RH Failed", "N/A - RH Failed", "N/A - RH Failed", tla2, tla2_date, conv2, conv2_date, "RH Assembly Duplicate Serial Number Failure")
                                )
                            conn.commit()

                except sqlite3.Error as db_err:
                    logger.exception("SQLite error occurred: %s", db_err)
                except Exception as e:
                    logger.exception("Unexpected error in monitor_and_update: %s", e)
    except Exception as e:
        logger.exception("Critical failure in monitor_and_update: %s", e)
        sys.exit(1)


def validate_db_schema(db_file: str) -> None:
    """
    Validate the database schema to ensure all required tables and columns exist.
    """
    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        # Example validation for 'tn' table
        cursor.execute("""
        SELECT 1 FROM sqlite_master 
        WHERE type='table' AND name='tn';
        """)
        if not cursor.fetchone():
            raise ValueError("Required table 'tn' does not exist in the database.")
        # Additional schema validations can be added here
        conn.close()
    except Exception as e:
        logger.exception("Database schema validation failed: %s", e)
        sys.exit(1)

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
    # Validate the database schema before starting
    logger.info("Validating database schema for %s", db_file)
    validate_db_schema(db_file)
    logger.info("Database schema validation completed for %s", db_file)

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