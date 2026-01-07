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
import subprocess
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
    'TN_MESSAGE':            'TN.MESSAGE',

    # Processing flow
    'FIRST_PIECE_CHECK':     'TN.RABBIT_MODE',
    'REWORK_MODE':           'TN.REWORK_MODE',
    'REWORK_LABEL_FINISHED': 'TN.RW_LABEL_FINISHED',
    'TN_MANUAL_ENTRY':       'TN.RW_MANUAL_ENTRY',

    # TLA duplicate handling
    'TN_TLA_SN_CHECK_PASS':  'TN.TLA_SN_CHECK_PASS',
    # DB entry handshake
    'SERIAL_DB_ENTRY_COMPLETE': 'SERIAL_DB_ENTRY_COMPLETE',
    # NEW: cycle-ready handshake bit
    'PI_CYCLE_READY':          'TN.PI_CYCLE_READY',
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
    # Rework extensions
    'ALLOW_MULTIPLE_REWORK': 'ALLOW_MULTIPLE_REWORK',
    'FORCE_REWORK_INPUT':    'I1.Data.15',
    'REWORK_TLA_WRITEBACK':  'ZEBRA.Working_String[33]',
    # PLC LocalDateTime from GSV into a DINT[7] array
    'PLC_TIME_ARRAY':        'PLC_Time_UDT',
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

def write_and_verify(plc: LogixDriver, tag: str, value: Any) -> bool:
    """Write a tag and verify by reading it back."""
    try:
        wr = plc.write((tag, value))
        if not wr or getattr(wr, 'error', None):
            logger.error("Write failed: %s -> %r | err=%s", tag, value, getattr(wr, 'error', None))
            return False
        rb = plc.read(tag)
        ok = rb and getattr(rb, 'error', None) is None and rb.value == value
        if not ok:
            logger.error("Readback mismatch: %s expected=%r got=%r", tag, value, rb.value if rb else None)
        return ok
    except Exception:
        logger.exception("write_and_verify error for %s", tag)
        return False
    
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

        test_msg = f"TNPY900 self-test @ {datetime.now().strftime('%H:%M:%S')}"
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
            current_sn_num = plc.read(f"{PLC_TAGS['SERIAL_NUMBER']}[1]").value or 0
            next_sn_num = current_sn_num + 1
            plc.write((f"{PLC_TAGS['SERIAL_NUMBER']}[1]", next_sn_num))
            # log fix: remove undefined part_select var
            logger.info("TLA duplicate detected for %s; incremented SERIAL_NUMBER[1] to %s (attempt %d)",
                        tla_sn, next_sn_num, attempt)
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
    Log and write a single failure entry, then drive PLC to fail step (89).
    This is gated by SEQ_STEP != 89 to prevent repeats; level-trigger only.
    """
    try:
        seq = plc.read(PLC_TAGS['SEQ_STEP'])
        if seq and seq.value == 89:
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
            plc.write((PLC_TAGS['SEQ_STEP'], 89))
        except Exception:
            logger.exception("Failed to write PLC fail flags/step")
    except Exception:
        logger.exception("Failure record/signaling encountered an error")


def handle_fail(lh_pass: bool, rh_pass: bool, plc: LogixDriver,
                cursor: sqlite3.Cursor, lhconv: Any, rhconv: Any) -> None:
    # Leak-test failures are no longer recorded in the DB; duplicates still are handled
    try:
        seq = plc.read(PLC_TAGS['SEQ_STEP'])
        if seq and seq.value == 89:
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
        # Record once, then drive PLC to step 89
        record_and_signal_failure(plc, cursor, 'N/A', lhconv, rhconv, status)
    except Exception:
        logger.exception("handle_fail encountered an error")


# Time sync settings
TIME_SYNC_ENABLED = True          # turn off if not desired
TIME_SYNC_INTERVAL_S = 10000         # how often to check PLC vs Pi
TIME_SKEW_THRESHOLD_S = 2         # correct time if skew exceeds this

def _plc_time_array(plc: LogixDriver) -> Optional[list]:
    """
    Read 7 DINTs written by GSV(LocalDateTime) into PLC_Time_UDT[0..6].
    Returns [year, month, day, hour, minute, second, microsecond] or None.
    """
    tag = PLC_TAGS['PLC_TIME_ARRAY']
    try:
        # Preferred: block read starting at [0] for 7 elements
        resp = plc.read((f"{tag}[0]", 7))
        vals = getattr(resp, "value", None)
        if isinstance(vals, (list, tuple)) and len(vals) >= 7:
            return list(vals[:7])
        # Fallback: read elements individually
        vals = []
        for i in range(7):
            r = plc.read(f"{tag}[{i}]")
            if not r or getattr(r, "error", None) is not None:
                return None
            vals.append(int(r.value))
        return vals
    except Exception:
        logger.exception("Failed to read PLC LocalDateTime array")
        return None

def _plc_time_to_datetime(vals: list) -> Optional[datetime]:
    """
    Convert [Y,M,D,h,m,s,usec] to a naive local datetime.
    """
    try:
        y, M, d, h, m, s, us = (int(vals[0]), int(vals[1]), int(vals[2]),
                                int(vals[3]), int(vals[4]), int(vals[5]), int(vals[6]))
        # clamp microseconds to valid range
        us = max(0, min(us, 999999))
        return datetime(y, M, d, h, m, s, us)
    except Exception:
        logger.exception("Invalid PLC time array: %r", vals)
        return None

def _set_pi_time(dt_local: datetime) -> bool:
    """
    Set system time using timedatectl. Returns True on success.
    Requires service to run as root or sudo NOPASSWD for timedatectl.
    """
    try:
        ts = dt_local.strftime("%Y-%m-%d %H:%M:%S")
        # First try directly
        r = subprocess.run(["sudo", "-n", "timedatectl", "set-time", ts],
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if r.returncode == 0:
            return True
        # If NTP blocks manual set, disable then set
        subprocess.run(["sudo", "-n", "timedatectl", "set-ntp", "false"],
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        r2 = subprocess.run(["sudo", "-n", "timedatectl", "set-time", ts],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if r2.returncode != 0:
            logger.error("timedatectl set-time failed: %s", r2.stderr.strip())
            return False
        return True
    except Exception:
        logger.exception("Failed to set system time via timedatectl")
        return False

def sync_pi_time_once(plc: LogixDriver) -> None:
    """
    Read PLC time once and update the Pi if skew exceeds threshold.
    """
    vals = _plc_time_array(plc)
    if not vals:
        return
    plc_dt = _plc_time_to_datetime(vals)
    if not plc_dt:
        return
    pi_now = datetime.now()
    skew = (plc_dt - pi_now).total_seconds()
    metrics_logger.info(json.dumps({
        "timestamp": datetime.utcnow().isoformat(),
        "metric": "time_skew_s", "value": skew
    }))
    if abs(skew) > TIME_SKEW_THRESHOLD_S:
        ok = _set_pi_time(plc_dt)
        logger.info("Time sync %s | PLC=%s | Pi(before)=%s | skew=%.3fs",
                    "OK" if ok else "FAILED",
                    plc_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    pi_now.strftime("%Y-%m-%d %H:%M:%S"),
                    skew)

def time_sync_worker(plc: LogixDriver) -> None:
    """
    Background thread to keep Pi time aligned with PLC time.
    """
    while True:
        try:
            sync_pi_time_once(plc)
        except Exception:
            logger.exception("Time sync worker error")
        time.sleep(TIME_SYNC_INTERVAL_S)

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
        # Startup self-test for TN.Message
        self_test_tn_message(plc)
        # Start PLC→Pi time sync thread
        if TIME_SYNC_ENABLED:
            threading.Thread(target=time_sync_worker, args=(plc,), daemon=True).start()
            # also do an immediate one-shot sync at startup
            sync_pi_time_once(plc)
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
    # per-cycle leak-fail log suppression flags (reset when SEQ_STEP returns to 0)
    leak_logged_normal = False
    leak_logged_fpc = False
    leak_logged_rework = False
    # per-cycle scan consumption flag to avoid reprocessing same SCAN_COMPLETE
    scan_consumed = False
    # per-cycle flag: track if we've already confirmed a unique TLA and set TN_TLA_SN_CHECK_PASS for this cycle
    tla_signal_sent = False
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
                seq_val = plc.read(PLC_TAGS['SEQ_STEP']).value
                if seq_val == 0:
                    # Hold PLC at step 0 until all resets succeed
                    plc.write((PLC_TAGS['PI_CYCLE_READY'], False))

                    ok = True
                    ok &= write_and_verify(plc, PLC_TAGS['TN_CHECK_PASS'], False)
                    ok &= write_and_verify(plc, PLC_TAGS['TN_CHECK_FAIL'], False)
                    ok &= write_and_verify(plc, PLC_TAGS['TN_TLA_SN_CHECK_PASS'], False)
                    ok &= write_and_verify(plc, PLC_TAGS['SERIAL_DB_ENTRY_COMPLETE'], False)
                    ok &= write_and_verify(plc, PLC_TAGS['REWORK_LABEL_FINISHED'], "")
                    ok &= write_and_verify(plc, PLC_TAGS['TN_MANUAL_ENTRY'], "")
                    ok &= write_and_verify(plc, PLC_TAGS['ALLOW_MULTIPLE_REWORK'], False)
                    ok &= write_and_verify(plc, PLC_TAGS['TN_MESSAGE'], "")

                    if ok:
                        plc.write((PLC_TAGS['PI_CYCLE_READY'], True))
                        logger.debug("PI_CYCLE_READY set True; PLC may advance from step 0")
                    else:
                        logger.error("Reset handshake failed; PI_CYCLE_READY remains False")

                    # reset per-cycle flags
                    leak_logged_normal = False
                    leak_logged_fpc = False
                    leak_logged_rework = False
                    scan_consumed = False
                    tla_signal_sent = False
                    continue
                # If PLC is holding in fail step 89, idle (prevents duplicate logging/inserts)
                if seq_val == 89:
                    time.sleep(POLL_INTERVAL)
                    continue
                # Prevent duplicate processing if SCAN_COMPLETE remains high: skip until reset
                if scan_consumed:
                    time.sleep(POLL_INTERVAL)
                    continue
                # wait for new SCAN_COMPLETE event (false→true)
                wait_for_tag(plc, 'SCAN_COMPLETE')
                # mark as consumed for this cycle; we'll only process once until reset
                scan_consumed = True
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
                    fpc_tla_sn = None
                    fpc_insert_done = False
                    while True:
                        rs = plc.read(PLC_TAGS['SEQ_STEP'])
                        leak_fail = plc.read(PLC_TAGS['LEAK_TEST_FAIL'])
                        if leak_fail and leak_fail.value:
                            if not leak_logged_fpc:
                                logger.info("Leak Test Failed during FPC - no DB entry created")
                                leak_logged_fpc = True
                            break
                        if rs and rs.value == 0:
                            logger.info("Cycle Reset.")
                            break
                        if rs and rs.value == 143 and not fpc_tla_sn:
                            logger.info("[FPC] Entered SEQ_STEP 143, calling ensure_unique_finished_serial")
                            fpc_tla_sn = ensure_unique_finished_serial(plc, cursor)
                            logger.info("[FPC] ensure_unique_finished_serial returned: %r", fpc_tla_sn)
                            if fpc_tla_sn and not tla_signal_sent:
                                tla_signal_sent = True
                        if rs and rs.value == 160 and fpc_tla_sn and not fpc_insert_done:
                            ts = time.strftime('%Y-%m-%d %H:%M:%S')
                            cursor.execute(SQL_STATEMENTS['insert_tn'],
                                           (ts,
                                            fpc_tla_sn,
                                            extract_julian(fpc_tla_sn),
                                            lhconv, extract_julian(lhconv),
                                            rhconv, extract_julian(rhconv),
                                            'First Piece Check'))
                            conn.commit()
                            replicate_tn_to_backups(ts, fpc_tla_sn, extract_julian(fpc_tla_sn), lhconv, extract_julian(lhconv), rhconv, extract_julian(rhconv), 'First Piece Check')
                            logger.info("Data stored in local database (First Piece Check)")
                            logger.info("Setting SERIAL_DB_ENTRY_COMPLETE True (FPC mode)")
                            result = plc.write((PLC_TAGS['SERIAL_DB_ENTRY_COMPLETE'], True))
                            if not result or getattr(result, 'error', None):
                                logger.error("Failed to write SERIAL_DB_ENTRY_COMPLETE: %r", result)
                            fpc_insert_done = True
                        time.sleep(POLL_INTERVAL)
                    continue

                # 1) Leak-test rerun logic removed entirely

                # 2) Rework Mode
                rework = plc.read(PLC_TAGS['REWORK_MODE'])
                if rework and rework.value:
                    ts = time.strftime('%Y-%m-%d %H:%M:%S')
                    if not lhconv or not rhconv:
                        set_pass(plc, False)
                        logger.error("Rework inputs invalid: LH=%s, RH=%s", lhconv, rhconv)
                        continue
                    try:
                        # Exact pair history (oldest -> newest)
                        cursor.execute(
                            "SELECT id, finished_serial, status FROM tn "
                            "WHERE component_serial1_date=? AND component_serial1=? "
                            "AND component_serial2_date=? AND component_serial2=? "
                            "ORDER BY id ASC",
                            (extract_julian(lhconv), lhconv, extract_julian(rhconv), rhconv)
                        )
                        pair_rows = cursor.fetchall()
                        cursor.execute(
                            "SELECT 1 FROM tn WHERE component_serial1_date=? AND component_serial1=? "
                            "AND NOT (component_serial2_date=? AND component_serial2=?) LIMIT 1",
                            (extract_julian(lhconv), lhconv, extract_julian(rhconv), rhconv)
                        )
                        mismatch_lh = cursor.fetchone() is not None
                        cursor.execute(
                            "SELECT 1 FROM tn WHERE component_serial2_date=? AND component_serial2=? "
                            "AND NOT (component_serial1_date=? AND component_serial1=?) LIMIT 1",
                            (extract_julian(rhconv), rhconv, extract_julian(lhconv), lhconv)
                        )
                        mismatch_rh = cursor.fetchone() is not None
                        normal_passes = sum(1 for _, _, st in pair_rows if st == 'Passed')
                        prev_tla = pair_rows[-1][1] if pair_rows else None
                    except Exception:
                        logger.exception("Rework DB lookup error")
                        set_pass(plc, False)
                        continue

                    # Must have at least one prior normal pass and no cross-mismatch
                    if normal_passes < 1 or mismatch_lh or mismatch_rh:
                        set_pass(plc, False)
                        logger.error("Rework blocked: pair_rows=%d, normal_passes=%d, mismatch_lh=%s, mismatch_rh=%s",
                                     len(pair_rows), normal_passes, mismatch_lh, mismatch_rh)
                        continue

                    # Always require supervisor key
                    try:
                        plc.write((PLC_TAGS['ALLOW_MULTIPLE_REWORK'], True))
                        write_plc_message(plc, "Supervisor key required for REWORK")
                    except Exception:
                        logger.exception("Failed to raise ALLOW_MULTIPLE_REWORK/message")
                    set_pass(plc, False)

                    # Wait for override or reset; then run 143/160 gated flow
                    while True:
                        rs = plc.read(PLC_TAGS['SEQ_STEP'])
                        if rs and rs.value == 0:
                            logger.info("Cycle Reset.")
                            break
                        ov = plc.read(PLC_TAGS['FORCE_REWORK_INPUT'])
                        if ov and ov.value:
                            logger.info("Supervisor override accepted for Rework")
                            set_pass(plc, True)
                            rework_tla_sn = None
                            rework_insert_done = False
                            while True:
                                rs2 = plc.read(PLC_TAGS['SEQ_STEP'])
                                leak_fail2 = plc.read(PLC_TAGS['LEAK_TEST_FAIL'])
                                if leak_fail2 and leak_fail2.value:
                                    if not leak_logged_rework:
                                        logger.info("Leak Test Failed during Rework - no DB entry created")
                                        leak_logged_rework = True
                                    break
                                if rs2 and rs2.value == 0:
                                    logger.info("Cycle Reset.")
                                    break
                                if rs2 and rs2.value == 143 and not rework_tla_sn:
                                    logger.info("[Rework] 143 -> ensure_unique_finished_serial()")
                                    rework_tla_sn = ensure_unique_finished_serial(plc, cursor)
                                    logger.info("[Rework] unique TLA = %r", rework_tla_sn)
                                if rs2 and rs2.value == 160 and not rework_insert_done:
                                    if not rework_tla_sn:
                                        logger.error("Skipping Rework DB insert: no TLA at 160")
                                        break
                                    ts = time.strftime('%Y-%m-%d %H:%M:%S')  # moved here
                                    status_text = f"Rework Pass <{prev_tla}> => <{rework_tla_sn}>" if prev_tla else f"Rework Pass => <{rework_tla_sn}>"
                                    cursor.execute(
                                        SQL_STATEMENTS['insert_tn'],
                                        (ts,
                                         rework_tla_sn, extract_julian(rework_tla_sn),
                                         lhconv, extract_julian(lhconv),
                                         rhconv, extract_julian(rhconv),
                                         status_text)
                                    )
                                    conn.commit()
                                    replicate_tn_to_backups(ts, rework_tla_sn, extract_julian(rework_tla_sn),
                                                            lhconv, extract_julian(lhconv),
                                                            rhconv, extract_julian(rhconv),
                                                            status_text)
                                    logger.info("Setting SERIAL_DB_ENTRY_COMPLETE True (Rework mode)")
                                    result = plc.write((PLC_TAGS['SERIAL_DB_ENTRY_COMPLETE'], True))
                                    if not result or getattr(result, 'error', None):
                                        logger.error("Failed to write SERIAL_DB_ENTRY_COMPLETE: %r", result)
                                    rework_insert_done = True
                                    break
                                time.sleep(POLL_INTERVAL)
                            break
                        time.sleep(POLL_INTERVAL)
                    continue

                # 3) Normal pass / fail
                lh_pass = check_converter_sn(cursor, 'component_serial1', lhconv, 'LH')
                rh_pass = check_converter_sn(cursor, 'component_serial2', rhconv, 'RH')

                if lh_pass and rh_pass:
                    set_pass(plc, True)
                    # prevent duplicate inserts while SEQ_STEP remains at 143 and PRINT_COMPLETE stays high
                    normal_insert_done = False
                    while True:
                        rs = plc.read(PLC_TAGS['SEQ_STEP'])
                        leak_fail = plc.read(PLC_TAGS['LEAK_TEST_FAIL'])
                        if leak_fail and leak_fail.value:
                            if not leak_logged_normal:
                                logger.info("Leak Test Failed during Normal run - no DB entry created")
                                leak_logged_normal = True
                            break


                        if rs and rs.value == 0:
                            logger.info("Cycle Reset.")
                            break

                        if rs and rs.value == 143:
                            logger.info("[Normal] Entered SEQ_STEP 143, calling ensure_unique_finished_serial")
                            tla_sn = ensure_unique_finished_serial(plc, cursor)
                            logger.info("[Normal] ensure_unique_finished_serial returned: %r", tla_sn)
                            if tla_sn and not tla_signal_sent:
                                tla_signal_sent = True
                        if rs and rs.value == 160:
                            ts = time.strftime('%Y-%m-%d %H:%M:%S')
                            cursor.execute(
                                SQL_STATEMENTS['insert_tn'],
                                (ts, tla_sn, extract_julian(tla_sn), lhconv, extract_julian(lhconv), rhconv, extract_julian(rhconv), 'Passed')
                            )
                            conn.commit()
                            replicate_tn_to_backups(ts, tla_sn, extract_julian(tla_sn), lhconv, extract_julian(lhconv), rhconv, extract_julian(rhconv), 'Passed')
                            logger.info("Database entry created for pass: Serial=%s", tla_sn)
                            logger.info("Setting SERIAL_DB_ENTRY_COMPLETE True")
                            result = plc.write((PLC_TAGS['SERIAL_DB_ENTRY_COMPLETE'], True))
                            if not result or getattr(result, 'error', None):
                                logger.error("Failed to write SERIAL_DB_ENTRY_COMPLETE: %r", result)
                            normal_insert_done = True
                            break
                        time.sleep(POLL_INTERVAL)

                        # If we created the record already, exit the outer loop to avoid a second insert
                        if normal_insert_done:
                            break
                        time.sleep(POLL_INTERVAL)
                else:
                    # Drive failure handling once; subsequent loops idle while SEQ_STEP==89
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