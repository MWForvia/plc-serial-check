#!/usr/bin/env python3
"""
tnpy300.py

Monitor converter/TLA serials from a PLC, check against historical SQLite
`tn` table and record Pass/Fail/Rework rows. This version includes:
- Verified PLC writes via `safe_write` (read-after-write, retries).
- Defensive PLC reads via `read_tag` to tolerate comm failures.
- USB-safe one-way and tri-directional DB replication to mounted backups.

Database: tndb300.db
Table: tn
Schema:
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

# Cache for change-detected logging of key PLC tag reads
_READ_CACHE = {}

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
    'TN_CHECK_FAIL_LH':      'TN.CHECK_FAIL_LH',
    'TN_CHECK_FAIL_RH':      'TN.CHECK_FAIL_RH',    
    'SCAN_COMPLETE':         'TN.SCAN_COMPLETE',
    'TN_DB_ERROR':           'TN.DB_ERROR',
    'DB_ERROR_INFO':         'TN.DB_ERROR_INFO',
    'TN_MESSAGE':            'TN.MESSAGE',
    'TN_MESSAGE_ACTIVE':     'TN.MESSAGE_ACTIVE',
    'DB_ENTRY_SUCCESS':      'TN.DB_ENTRY_SUCCESS',
    'TORQUE_PASS':           'TN.TORQUE_PASS',
    'TORQUE_FAIL':           'TN.TORQUE_FAIL',
    'REWORK_MODE':           'TN.REWORK_MODE',
    'SUPERVISOR_KEY':       'TN.SUPERVISOR_KEY',
    'CYCLE_READY':           'TN.CYCLE_READY',
    'DB_ENTRY_SUCCESS':     'TN.DB_ENTRY_SUCCESS',

    # Sequence step (not part of UDT)
    'SEQ_STEP':              'Local_Step_II_N',
    'TLA1':                  'PN.C2_TLA1_TN',
    'CONV1':                 'PN.C2_CONV1_TN',
    'TLA2':                  'PN.C2_TLA2_TN',
    'CONV2':                 'PN.C2_CONV2_TN'
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

# Detailed DB error info messages for TN.DB_ERROR_INFO (PLC STRING)
DB_ERROR_INFO_CODES = {
    'SCHEMA_ERROR':        'Schema error',
    'WRITE_ERROR':         'Write failed',
    'REWORK_LOOKUP_ERROR': 'Rework lookup error',
}
# helper to write a message string back to PLC TN.Message tag
def write_plc_message(plc: LogixDriver, message: str) -> None:
    """Write a message to the TN.Message PLC tag using pycomm3 string syntax."""
    try:
        # pycomm3 supports writing STRING tags by passing a Python str directly
        msg = str(message or "")
        # Allen-Bradley STRING limit is 82; use 80 chars safe limit for operator messages
        if len(msg) > 80:
            msg = msg[:80]
        # use verified write helper so failures are retried and reported
        ok = safe_write(plc, PLC_TAGS['TN_MESSAGE'], msg, verify=True, retries=3)
        # Set/clear active flag to mirror presence of a message
        active = bool(msg) if ok else False
        try:
            safe_write(plc, PLC_TAGS['TN_MESSAGE_ACTIVE'], active, verify=True, retries=2)
        except Exception:
            logger.debug("Failed to update TN.Message_Active", exc_info=True)
        if not ok:
            logger.error("TN.Message write failed after retries: %s", msg)
    except Exception:
        logger.exception("Failed to write PLC message")


def safe_write(plc: Optional[LogixDriver], tag_name: str, value: Any, verify: bool = True, retries: int = 3, verify_delay: float = 0.15) -> bool:
    logger.info(f"PLC WRITE: tag={tag_name} value={value!r}")
    """
    Perform a write to the PLC and verify by reading the tag back.
    - Attempts up to `retries` times (including initial attempt).
    - If verification is enabled, reads the tag after write and compares values.
    - On persistent failure, sets TN.DB_ERROR True (best-effort) and returns False.
    Returns True on confirmed success, False otherwise.
    """
    import time, random
    if plc is None:
        logger.error("safe_write called but plc is None for tag %s", tag_name)
        return False

    # Defensive truncation: if writing to TN.MESSAGE, enforce 80-char limit
    try:
        tn_msg_tag = PLC_TAGS.get('TN_MESSAGE')
    except Exception:
        tn_msg_tag = None
    if isinstance(value, str) and tn_msg_tag and (tag_name == tn_msg_tag or tag_name.endswith('.MESSAGE')):
        if len(value) > 80:
            value = value[:80]

    # No coercion here — DB_ERROR_INFO is a PLC STRING for this PLC

    for attempt in range(1, retries + 1):
        try:
            result = plc.write((tag_name, value))
        except Exception as exc:
            logger.warning("PLC write exception for %s attempt %d/%d: %s", tag_name, attempt, retries, exc)
            result = None

        ok = True
        # Quick heuristic checks on driver return types
        if result is False or result is None:
            ok = False
        else:
            try:
                # some drivers return objects with 'status'/'error' or truthy success
                if getattr(result, 'error', None):
                    ok = False
            except Exception:
                pass

        if ok and verify:
            # Read-back verification
            try:
                # Optional small pause before verifying to allow PLC logic to settle
                if verify_delay and verify_delay > 0:
                    time.sleep(verify_delay)
                read_obj = plc.read(tag_name)
                read_val = getattr(read_obj, 'value', read_obj)
                # Normalise bytes/str differences
                if isinstance(value, bytes) and isinstance(read_val, (bytes, bytearray)):
                    read_cmp = bytes(read_val)
                else:
                    read_cmp = read_val
                if read_cmp != value:
                    logger.warning("Read-back mismatch for %s: wrote=%r read=%r (attempt %d/%d)", tag_name, value, read_val, attempt, retries)
                    ok = False
            except Exception as exc:
                logger.warning("Read-back exception for %s attempt %d/%d: %s", tag_name, attempt, retries, exc)
                ok = False

        if ok:
            if attempt > 1:
                logger.info("PLC write succeeded for %s after %d attempts", tag_name, attempt)
            return True

        # backoff before retry
        if attempt < retries:
            delay = min(0.1 * (2 ** (attempt - 1)), 2.0) + random.uniform(0, 0.05)
            time.sleep(delay)

    # All retries exhausted — mark DB error flag (best-effort)
    logger.error("PLC write failed for %s after %d attempts; setting TN.DB_ERROR", tag_name, retries)
    try:
        # try best-effort write to TN.DB_ERROR without recursion
        plc.write((PLC_TAGS['TN_DB_ERROR'], True))
    except Exception:
        logger.exception("Failed to set TN.DB_ERROR after write failures")
    return False


def read_tag(plc: Optional[LogixDriver], tag_name: str) -> Any:
    """Safely read a PLC tag and return the tag value or None on error.

    - Returns the attribute `value` when present on the driver response,
      otherwise returns the raw driver result.
    - Returns None on exceptions or when the driver returns None.
    """
    if plc is None:
        return None
    try:
        res = plc.read(tag_name)
    except Exception:
        logger.debug("PLC read exception for %s", tag_name, exc_info=True)
        return None
    if res is None:
        return None
    val = getattr(res, 'value', res)
    # Change-detected logging for key tags only
    key_tags = [
        PLC_TAGS.get('SEQ_STEP'),
        PLC_TAGS.get('SCAN_COMPLETE'),
        PLC_TAGS.get('TORQUE_PASS'),
        PLC_TAGS.get('REWORK_MODE'),
        PLC_TAGS.get('SUPERVISOR_KEY'),
    ]
    if tag_name in key_tags:
        if tag_name not in _READ_CACHE:
            logger.info(f"PLC READ INIT: tag={tag_name} value={val!r}")
            _READ_CACHE[tag_name] = val
        else:
            prev = _READ_CACHE.get(tag_name)
            if prev != val:
                logger.info(f"PLC READ CHG: tag={tag_name} {prev!r} -> {val!r}")
                _READ_CACHE[tag_name] = val
    return val


def ensure_db_schema(db_path: str) -> None:
    """Ensure the database file exists and has the required `tn` table and indexes.

    This function is idempotent and safe to call multiple times for local and
    USB backup DB paths.
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
                safe_write(plc, PLC_TAGS['TN_DB_ERROR'], True, verify=True, retries=3)
                safe_write(plc, PLC_TAGS['DB_ERROR_INFO'], DB_ERROR_INFO_CODES['SCHEMA_ERROR'], verify=True, retries=3)
        except Exception:
            pass

POLL_INTERVAL      = 0.5   # general polling interval
FAST_POLL_INTERVAL = 0.25   # fast polling for fail
RETRY_DELAY        = 1    # seconds to wait before first retry
MAX_RETRY_DELAY    = 5    # maximum seconds to back off on repeated errors

# Connection resilience and observability
SOCKET_TIMEOUT_SEC = float(os.getenv('TNPY300_SOCKET_TIMEOUT', '1.5'))  # PLC socket timeout; prevents indefinite hangs
HEARTBEAT_INTERVAL_SEC = float(os.getenv('TNPY300_HEARTBEAT_SEC', '10'))  # periodic log while idling in SEQ_STEP 10
READ_FAILS_RECONNECT = int(os.getenv('TNPY300_READ_FAILS_RECONNECT', '10'))  # consecutive read None before reconnect
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
                        # create indexes on new backup DB (match current schema)
                        init_conn.execute("CREATE INDEX IF NOT EXISTS idx_tn_tla1_date ON tn(tla1_date);")
                        init_conn.execute("CREATE INDEX IF NOT EXISTS idx_tn_conv1_date ON tn(conv1_date);")
                        init_conn.execute("CREATE INDEX IF NOT EXISTS idx_tn_tla2_date ON tn(tla2_date);")
                        init_conn.execute("CREATE INDEX IF NOT EXISTS idx_tn_conv2_date ON tn(conv2_date);")
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
                    "date TEXT, tla1 TEXT, tla1_date TEXT, conv1 TEXT, conv1_date TEXT, "
                    "tla2 TEXT, tla2_date TEXT, conv2 TEXT, conv2_date TEXT, status TEXT)"
                )
                tgt_cur.execute("SELECT MAX(id) FROM tn")
                max_id = tgt_cur.fetchone()[0] or 0
                tgt_conn.execute("ATTACH DATABASE ? AS src", (src_path,))
                new_count = tgt_conn.execute(
                    "SELECT COUNT(*) FROM src.tn WHERE id > ?", (max_id,)
                ).fetchone()[0]
                if new_count > 0:
                    tgt_conn.execute(
                        "INSERT INTO tn(date, tla1, tla1_date, conv1, conv1_date, tla2, tla2_date, conv2, conv2_date, status) "
                        "SELECT date, tla1, tla1_date, conv1, conv1_date, tla2, tla2_date, conv2, conv2_date, status "
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
                # Ensure indexes used by lookups exist on fresh targets (match current schema)
                init_conn.execute("CREATE INDEX IF NOT EXISTS idx_tn_tla1_date ON tn(tla1_date);")
                init_conn.execute("CREATE INDEX IF NOT EXISTS idx_tn_conv1_date ON tn(conv1_date);")
                init_conn.execute("CREATE INDEX IF NOT EXISTS idx_tn_tla2_date ON tn(tla2_date);")
                init_conn.execute("CREATE INDEX IF NOT EXISTS idx_tn_conv2_date ON tn(conv2_date);")
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
                    "INSERT INTO tn(date, tla1, tla1_date, conv1, conv1_date, tla2, tla2_date, conv2, conv2_date, status) "
                    "SELECT date, tla1, tla1_date, conv1, conv1_date, tla2, tla2_date, conv2, conv2_date, status "
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
    logger.debug("Waiting for tag %s to become True", name)
    while True:
        val = read_tag(plc, name)
        if val:
            logger.debug("Tag %s observed True; proceeding", name)
            return  # Proceed immediately if the tag is already true
        time.sleep(POLL_INTERVAL)


def wait_for_fail_or_reset(plc: LogixDriver) -> bool:
    """
    Level-detect the PART_FAIL tag: return True as soon as it's observed true, or exit on reset.
    """
    while True:
        rs = read_tag(plc, PLC_TAGS['SEQ_STEP'])
        if rs == 10:
            logger.info("Cycle Reset.")
            return False
        time.sleep(FAST_POLL_INTERVAL)


def wait_for_torque_result(plc: LogixDriver) -> Optional[str]:
    """
    Active-poll torque result. Returns 'pass' if `TORQUE_PASS` goes high,
    or None if SEQ_STEP==10 (reset) or SCAN_COMPLETE clears.
    """
    logger.debug("Waiting for torque result (pass or reset/scan-clear)")
    while True:
        if read_tag(plc, PLC_TAGS['SEQ_STEP']) == 10:
            logger.debug("Torque wait aborted: SEQ_STEP==10 detected (reset)")
            return None
        if not read_tag(plc, PLC_TAGS['SCAN_COMPLETE']):
            logger.debug("Torque wait aborted: SCAN_COMPLETE cleared")
            return None
        if read_tag(plc, PLC_TAGS['TORQUE_PASS']):
            logger.debug("Torque PASS observed")
            return 'pass'
        time.sleep(FAST_POLL_INTERVAL)

def _disable_cip_timeouts(plc: LogixDriver) -> None:
    """Best-effort: disable socket/CIP timeouts so waits can be long-lived.
    Tries a few likely attributes; ignores failures. Logs what it changes.
    """
    try:
        socket.setdefaulttimeout(None)
        logger.debug("Set global socket default timeout to None")
    except Exception:
        logger.debug("Unable to set global socket default timeout", exc_info=True)
    paths = [
        ("_cli","socket"),
        ("_client","socket"),
        ("_conn","socket"),
        ("socket",),
        ("_sock",),
    ]
    for path in paths:
        try:
            obj = plc
            for p in path:
                obj = getattr(obj, p)
            if hasattr(obj, 'settimeout'):
                obj.settimeout(None)
                logger.debug("Disabled timeout on plc.%s", '.'.join(path))
        except Exception:
            # ignore missing paths
            continue


def check_converter_sn(cursor: sqlite3.Cursor, column: str, sn: Any, label: str) -> bool:
    julian_date = extract_julian(sn)
    cursor.execute(
        f"SELECT 1 FROM tn WHERE {column}_date = ? AND {column} = ?",
        (julian_date, sn)
    )
    if cursor.fetchone():
        logger.info(f"SERIAL RESULT: {label} {sn} - NOK")
        logger.warning("%s Converter SN Failed: %s", label, sn)
        return False
    logger.info(f"SERIAL RESULT: {label} {sn} - OK")
    logger.info("%s Converter SN Passed: %s", label, sn)
    return True


# leak-test rerun logic removed


def set_pass(plc: LogixDriver, passed: bool) -> None:
    # Set PASS and per-side FAIL flags consistently
    safe_write(plc, PLC_TAGS['TN_CHECK_PASS'], passed, verify=True, retries=3)
    # When passing, clear both fail flags; when failing, leave per-side flags to callers
    if passed:
        safe_write(plc, PLC_TAGS['TN_CHECK_FAIL_LH'], False, verify=True, retries=3)
        safe_write(plc, PLC_TAGS['TN_CHECK_FAIL_RH'], False, verify=True, retries=3)


def insert_tn_record(db_path: str, timestamp: str, tla1: Any, tla1_date: Any, conv1: Any, conv1_date: Any, tla2: Any, tla2_date: Any, conv2: Any, conv2_date: Any, status: str) -> None:
    logger.info(f"DB STORE: {db_path} date={timestamp} tla1={tla1} tla1_date={tla1_date} conv1={conv1} conv1_date={conv1_date} tla2={tla2} tla2_date={tla2_date} conv2={conv2} conv2_date={conv2_date} status={status}")
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
                    safe_write(plc, PLC_TAGS['TN_DB_ERROR'], False, verify=True, retries=3)
                    # clear DB_ERROR_INFO on success
                    safe_write(plc, PLC_TAGS['DB_ERROR_INFO'], "", verify=True, retries=3)
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
                safe_write(plc, PLC_TAGS['TN_DB_ERROR'], True, verify=True, retries=3)
                safe_write(plc, PLC_TAGS['DB_ERROR_INFO'], DB_ERROR_INFO_CODES['WRITE_ERROR'], verify=True, retries=3)
        except Exception:
            pass


# Updated replication helper to match current tn schema (tla1/conv1/tla2/conv2)
def replicate_tn_to_backups(timestamp: str,
                            tla1: Any, tla1_date: Any,
                            conv1: Any, conv1_date: Any,
                            tla2: Any, tla2_date: Any,
                            conv2: Any, conv2_date: Any,
                            status: str) -> None:
    """Replicate a tn row to mounted USB backup databases (non-fatal if absent)."""
    for dbp in USB_DB_BACKUPS:
        if not dbp.startswith('/media'):
            continue
        # Skip silently if device/mount not present
        mount_root = os.path.dirname(os.path.dirname(dbp))
        if not is_mounted(mount_root):
            logger.debug("Skip replicate: mount not present for %s", dbp)
            continue
        try:
            ensure_db_schema(dbp)
            insert_tn_record(dbp, timestamp, tla1, tla1_date, conv1, conv1_date, tla2, tla2_date, conv2, conv2_date, status)
        except Exception:
            logger.exception("Replication to %s failed (non-fatal)", dbp)


def monitor_and_update(plc_ip_address: str, db_file: str) -> None:
    """Main monitoring loop with Normal + Rework mode gating."""
    try:
        with LogixDriver(plc_ip_address) as plc:
            # Configure PLC socket timeout to avoid indefinite blocking
            try:
                plc._cli.socket.settimeout(SOCKET_TIMEOUT_SEC)
                logger.info("PLC socket timeout set to %.2fs", SOCKET_TIMEOUT_SEC)
            except Exception:
                logger.debug("Unable to set PLC socket timeout", exc_info=True)
            logger.info("PLC connection established.")
            ensure_db_schema(db_file)
            conn = get_db_connection(db_file)
            cursor = conn.cursor()
            cycle_reset_done = False  # debounce flag for SEQ_STEP 10 reset handling

            last_seq10_log = 0.0
            consecutive_read_failures = 0
            while True:
                try:
                    # Cycle reset handling
                    seq_val = read_tag(plc, PLC_TAGS['SEQ_STEP'])
                    if seq_val is None:
                        consecutive_read_failures += 1
                        if consecutive_read_failures >= READ_FAILS_RECONNECT:
                            logger.warning("Consecutive SEQ_STEP read failures (%d) – triggering reconnect", consecutive_read_failures)
                            raise CommError("Too many read failures; reconnecting")
                        time.sleep(FAST_POLL_INTERVAL)
                        continue
                    else:
                        if consecutive_read_failures:
                            logger.info("Cleared read failure streak after %d failures", consecutive_read_failures)
                        consecutive_read_failures = 0
                    if seq_val == 10:
                        if not cycle_reset_done:
                            logger.info("SEQ_STEP==10 detected; clearing per-cycle flags and preparing for next cycle")
                            safe_write(plc, PLC_TAGS['TN_CHECK_FAIL_LH'], False, verify=True, retries=3)
                            safe_write(plc, PLC_TAGS['TN_CHECK_FAIL_RH'], False, verify=True, retries=3)
                            safe_write(plc, PLC_TAGS['TN_CHECK_PASS'], False, verify=True, retries=3)
                            write_plc_message(plc, "")
                            # clear DB entry success flag on reset
                            try:
                                safe_write(plc, PLC_TAGS['DB_ENTRY_SUCCESS'], False, verify=True, retries=3)
                            except Exception:
                                logger.exception("Failed to clear DB_ENTRY_SUCCESS on reset")
                            # clear DB error flags at cycle start; any new DB failure will re-raise them
                            try:
                                safe_write(plc, PLC_TAGS['TN_DB_ERROR'], False, verify=True, retries=2)
                                safe_write(plc, PLC_TAGS['DB_ERROR_INFO'], "", verify=True, retries=2)
                            except Exception:
                                logger.exception("Failed to clear TN.DB_ERROR/DB_ERROR_INFO on reset")
                            cycle_reset_done = True
                        # Always assert CYCLE_READY while we are in step 10 so missed edges don't block startup
                        try:
                            cr = read_tag(plc, PLC_TAGS['CYCLE_READY'])
                            if not cr:
                                logger.info("Maintaining TN.CYCLE_READY True while SEQ_STEP==10")
                                safe_write(plc, PLC_TAGS['CYCLE_READY'], True, verify=True, retries=2, verify_delay=0.15)
                            now = time.time()
                            if now - last_seq10_log >= HEARTBEAT_INTERVAL_SEC:
                                logger.info("Heartbeat: still idling in SEQ_STEP 10; CYCLE_READY=%s", cr)
                                last_seq10_log = now
                        except Exception:
                            logger.debug("Unable to maintain TN.CYCLE_READY while in step 10", exc_info=True)
                    else:
                        # leaving SEQ_STEP 10: do not clear CYCLE_READY here
                        # PLC shall be responsible for clearing TN.CYCLE_READY; only the host sets it high.
                        cycle_reset_done = False

                    # Only proceed to scan gating when machine is in scan step (e.g., SEQ_STEP == 20)
                    if seq_val != 20:
                        time.sleep(FAST_POLL_INTERVAL)
                        continue
                    # Detect mode and wait for scan to be active (level-detect)
                    rework_mode = bool(read_tag(plc, PLC_TAGS['REWORK_MODE']))
                    # Wait until SCAN_COMPLETE is true, but abort immediately on reset (SEQ_STEP==10)
                    _aborted = False
                    while True:
                        if read_tag(plc, PLC_TAGS['SEQ_STEP']) == 10:
                            logger.info("Reset detected while waiting for SCAN_COMPLETE; aborting scan sequence")
                            _aborted = True
                            break
                        if read_tag(plc, PLC_TAGS['SCAN_COMPLETE']):
                            break
                        time.sleep(POLL_INTERVAL)
                    if _aborted:
                        continue

                    # Read serials safely
                    tla1 = (read_tag(plc, PLC_TAGS['TLA1']) or '').strip()
                    conv1 = (read_tag(plc, PLC_TAGS['CONV1']) or '').strip()
                    tla2 = (read_tag(plc, PLC_TAGS['TLA2']) or '').strip()
                    conv2 = (read_tag(plc, PLC_TAGS['CONV2']) or '').strip()
                    tla1_date = extract_julian(tla1)
                    conv1_date = extract_julian(conv1)
                    tla2_date = extract_julian(tla2)
                    conv2_date = extract_julian(conv2)
                    ts = time.strftime('%Y-%m-%d %H:%M:%S')
                    serials = [
                        ("TLA1", tla1),
                        ("CONV1", conv1),
                        ("TLA2", tla2),
                        ("CONV2", conv2),
                    ]
                    logger.info("SERIALS CHECKED: " + ", ".join(f"{label}={sn}" for label, sn in serials))

                    if rework_mode:
                        # Rework requirements (simplified):
                        # 1) There must be at least one exact 4-way serial match in the database.
                        # 2) None of the 4 serials may appear in any other row unless that row is the same exact 4-way match.
                        cursor.execute(
                            "SELECT 1 FROM tn WHERE tla1=? AND conv1=? AND tla2=? AND conv2=? AND lower(status)='passed' LIMIT 1",
                            (tla1, conv1, tla2, conv2)
                        )
                        if cursor.fetchone() is None:
                            logger.warning("Rework gate fail: no existing 4-way match with status=Passed for this serial set")
                            write_plc_message(plc, "Rework denied: no base pass")
                            safe_write(plc, PLC_TAGS['TN_CHECK_PASS'], False, verify=True, retries=3)
                            continue

                        cursor.execute(
                            """
                            SELECT id, date, status
                            FROM tn
                            WHERE (tla1=? OR conv1=? OR tla2=? OR conv2=?)
                              AND NOT (tla1=? AND conv1=? AND tla2=? AND conv2=?)
                            LIMIT 1
                            """,
                            (tla1, conv1, tla2, conv2, tla1, conv1, tla2, conv2)
                        )
                        conflict = cursor.fetchone()
                        if conflict is not None:
                            conflict_id, conflict_date, conflict_status = conflict
                            logger.warning(
                                "Rework gate fail: found non-4-way match row (id=%s date=%s status=%s)",
                                conflict_id, conflict_date, conflict_status
                            )
                            write_plc_message(plc, "Rework denied: serial mismatch history")
                            safe_write(plc, PLC_TAGS['TN_CHECK_PASS'], False, verify=True, retries=3)
                            continue

                        safe_write(plc, PLC_TAGS['TN_CHECK_PASS'], True, verify=True, retries=3)
                        logger.info("Rework gating pass: exact 4-way match exists and no conflicting pairings found")

                        tr = wait_for_torque_result(plc)
                        if tr == 'pass':
                            insert_tn_record(db_file, ts, tla1, tla1_date, conv1, conv1_date, tla2, tla2_date, conv2, conv2_date, 'Rework Pass')
                            replicate_tn_to_backups(ts, tla1, tla1_date, conv1, conv1_date, tla2, tla2_date, conv2, conv2_date, 'Rework Pass')
                            write_plc_message(plc, "")
                            try:
                                safe_write(plc, PLC_TAGS['DB_ENTRY_SUCCESS'], True, verify=True, retries=3)
                            except Exception:
                                logger.exception("Failed to set DB_ENTRY_SUCCESS after rework pass")
                            logger.info("Inserted Rework Pass record")
                            while read_tag(plc, PLC_TAGS['SEQ_STEP']) != 10:
                                time.sleep(FAST_POLL_INTERVAL)
                        # if tr is 'fail' or None, just continue to next cycle (no insert)
                        continue

                    # Normal mode
                    def dup_exists(col_prefix: str, serial: str, serial_date: str) -> bool:
                        if not serial:
                            return True
                        cursor.execute(
                            f"SELECT 1 FROM tn WHERE {col_prefix}_date=? AND {col_prefix}=? LIMIT 1",
                            (serial_date, serial)
                        )
                        return cursor.fetchone() is not None

                    tla1_dup = dup_exists('tla1', tla1, tla1_date)
                    conv1_dup = dup_exists('conv1', conv1, conv1_date)
                    tla2_dup = dup_exists('tla2', tla2, tla2_date)
                    conv2_dup = dup_exists('conv2', conv2, conv2_date)
                    lh_fail = tla1_dup or conv1_dup
                    rh_fail = tla2_dup or conv2_dup

                    safe_write(plc, PLC_TAGS['TN_CHECK_FAIL_LH'], lh_fail, verify=True, retries=3)
                    safe_write(plc, PLC_TAGS['TN_CHECK_FAIL_RH'], rh_fail, verify=True, retries=3)
                    safe_write(plc, PLC_TAGS['TN_CHECK_PASS'], not (lh_fail or rh_fail), verify=True, retries=3)

                    if not lh_fail and not rh_fail:
                        logger.info("Normal pass gating – waiting torque result")
                        tr = wait_for_torque_result(plc)
                        if tr == 'pass':
                            insert_tn_record(db_file, ts, tla1, tla1_date, conv1, conv1_date, tla2, tla2_date, conv2, conv2_date, 'Passed')
                            replicate_tn_to_backups(ts, tla1, tla1_date, conv1, conv1_date, tla2, tla2_date, conv2, conv2_date, 'Passed')
                            # write_plc_message(plc, "PASS")  # removed to avoid overriding HMI instructions
                            try:
                                safe_write(plc, PLC_TAGS['DB_ENTRY_SUCCESS'], True, verify=True, retries=3)
                            except Exception:
                                logger.exception("Failed to set DB_ENTRY_SUCCESS after pass")
                            logger.info("Inserted Pass record")
                            # wait for reset
                            while read_tag(plc, PLC_TAGS['SEQ_STEP']) != 10:
                                time.sleep(FAST_POLL_INTERVAL)
                        else:
                            logger.info("Torque did not pass or cycle reset detected; continuing")
                        continue

                    if lh_fail and not rh_fail:
                        cursor.execute(
                            "SELECT tla1 FROM tn WHERE (tla1=? OR conv1=? ) ORDER BY id ASC LIMIT 1",
                            (tla1, conv1)
                        )
                        row = cursor.fetchone()
                        first_tla = row[0] if row else 'Unknown'
                        status = f"LH failed - first TLA: {first_tla}"
                        insert_tn_record(db_file, ts, tla1, tla1_date, conv1, conv1_date, 'N/A', '', 'N/A', '', status)
                        replicate_tn_to_backups(ts, tla1, tla1_date, conv1, conv1_date, 'N/A', '', 'N/A', '', status)
                        write_plc_message(plc, "LH FAIL, Re-use RH and notify Quality about LH")
                        logger.warning("Partial failure recorded (LH)")
                        continue

                    if rh_fail and not lh_fail:
                        cursor.execute(
                            "SELECT tla2 FROM tn WHERE (tla2=? OR conv2=? ) ORDER BY id ASC LIMIT 1",
                            (tla2, conv2)
                        )
                        row = cursor.fetchone()
                        first_tla = row[0] if row else 'Unknown'
                        status = f"RH failed - first TLA: {first_tla}"
                        insert_tn_record(db_file, ts, 'N/A', '', 'N/A', '', tla2, tla2_date, conv2, conv2_date, status)
                        replicate_tn_to_backups(ts, 'N/A', '', 'N/A', '', tla2, tla2_date, conv2, conv2_date, status)
                        write_plc_message(plc, "RH FAIL, Re-use LH and notify Quality about RH")
                        logger.warning("Partial failure recorded (RH)")
                        continue

                    if lh_fail and rh_fail:
                        cursor.execute(
                            "SELECT tla1 FROM tn WHERE (tla1=? OR conv1=? ) ORDER BY id ASC LIMIT 1",
                            (tla1, conv1)
                        )
                        row = cursor.fetchone()
                        first_lh = row[0] if row else 'Unknown'
                        cursor.execute(
                            "SELECT tla2 FROM tn WHERE (tla2=? OR conv2=? ) ORDER BY id ASC LIMIT 1",
                            (tla2, conv2)
                        )
                        row = cursor.fetchone()
                        first_rh = row[0] if row else 'Unknown'
                        status = f"LH failed - first TLA: {first_lh} & RH failed - first TLA: {first_rh}"
                        insert_tn_record(db_file, ts, tla1, tla1_date, conv1, conv1_date, tla2, tla2_date, conv2, conv2_date, status)
                        replicate_tn_to_backups(ts, tla1, tla1_date, conv1, conv1_date, tla2, tla2_date, conv2, conv2_date, status)
                        write_plc_message(plc, "LH and RH FAIL, Notify Quality")
                        logger.error("Dual failure recorded")
                        continue

                except sqlite3.Error as db_err:
                    logger.exception("SQLite error in monitor loop: %s", db_err)
                except CommError as comm_err:
                    logger.exception("PLC communication error: %s", comm_err)
                    time.sleep(RETRY_DELAY)
                except Exception:
                    logger.exception("Unexpected error in monitor loop")
    except Exception:
        logger.exception("Fatal error establishing PLC monitor loop")
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
    """Ensure all known database files have the required schema."""
    db_paths = [default_local_db, USB_DB_BACKUP, USB_DB_BACKUP2]
    for db_path in db_paths:
        logger.info("Ensuring database schema for %s", db_path)
        ensure_db_schema(db_path)
        logger.info("Database schema ensured for %s", db_path)


def initialize_environment(db_file: str) -> None:
    """Prepare local and backup DBs, perform initial sync, and start USB watchers."""
    # Ensure log directory exists
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
    except Exception:
        logger.exception("Failed to ensure log directory %s", log_dir)

    # Ensure local and backup DB directories and schemas exist (create if missing)
    db_paths = [db_file, USB_DB_BACKUP, USB_DB_BACKUP2]
    for path in db_paths:
        parent = os.path.dirname(path) or '.'
        try:
            if not os.path.exists(parent):
                os.makedirs(parent, exist_ok=True)
        except Exception:
            logger.exception("Failed to create parent directory for %s", path)
        try:
            # ensure_db_schema is idempotent
            ensure_db_schema(path)
        except Exception:
            logger.exception("ensure_db_schema failed for %s", path)

    # Initial tri-directional sync (non-fatal)
    try:
        sync_db_from_backup(db_file)
    except Exception:
        logger.exception("Initial tri-directional sync failed (continuing)")

    # Start USB watcher threads for one-way sync on mount
    for idx, path in enumerate(USB_DB_BACKUPS, start=1):
        name = f"usb{idx}"
        try:
            t = threading.Thread(target=watch_usb_and_sync, args=(db_file, path, name), daemon=True)
            t.start()
            logger.info("Started USB watch thread for %s -> %s", name, path)
        except Exception:
            logger.exception("Failed to start watcher thread for %s", path)

def main() -> None:
    parser = argparse.ArgumentParser(description="TN barcode converter serial checker")
    parser.add_argument("--plc", default="192.168.1.1", help="PLC IP address")
    parser.add_argument("--db", default=default_local_db, help="Path to SQLite DB file")
    args = parser.parse_args()

    db_file = os.path.expanduser(args.db)
    logger.info("Initializing environment for DB=%s", db_file)
    initialize_environment(db_file)
    logger.info("Calling monitor_and_update with PLC=%s, DB=%s", args.plc, db_file)
    monitor_and_update(args.plc, db_file)


if __name__ == "__main__":
    ensure_all_dbs_initialized()
    main()