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
import os  # needed for path checks and directory creation
from logging.handlers import TimedRotatingFileHandler
import threading
import sqlite3
import time
from pathlib import Path
from typing import Any
from datetime import datetime, timedelta
import shutil
import subprocess

# Helper to detect real mount
# import os  # duplicate import removed

def is_mounted(path: str) -> bool:
    return os.path.ismount(os.path.dirname(path))

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
    # Created tags (for this project)
    'PI_HEARTBEAT':          'PI_Heartbeat',
    'TN_CHECK_PASS':         'TN_Check_Pass',
    'TN_CHECK_FAIL':         'TN_Check_Fail',
    'TN_DB_ERROR':           'TN_DB_Error',
    'FIRST_PIECE_CHECK':     'FIRST_PIECE_CHECK',
    'REWORK_MODE':           'REWORK_MODE',
    'REWORK_LABEL_DATE':     'REWORK_LABEL_DATE',
    'REWORK_LABEL_FINISHED': 'REWORK_LABEL_FINISHED',
    'REWORK_LABEL_LH':       'REWORK_LABEL_LH',
    'REWORK_LABEL_RH':       'REWORK_LABEL_RH',
    'TLA_SN_PASS':           'TLA_SN_PASS',
    'TN_TLA_SN_CHECK_PASS':  'TN_TLA_SN_CHECK_PASS',
    # Existing tags (from the PLC)
    'SERIAL_HOLDER':         'ZEBRA.Working_String[20]',    
    'LH_CONV':               'FIX_513D.Conv_Barcode.EXTRACT[2]',
    'RH_CONV':               'FIX_513D.Conv_Barcode_R.EXTRACT[2]',
    'DATASTORE':             'FIX_513D.Seq.Data_Store',
    'SEQ_STEP':              'SEQUENCE_STEP',
    'FINISHED_SERIAL':       'ZEBRA.Working_String[20]',
    'SCAN_COMPLETE':         'FIX_513D.Seq.Conv_Barcode_Passed',
    'PART_FAIL':             'FIX_513D.Seq.Part_Failed[0]',
    'LEAK_TEST_FAIL':        'FIX_513D.Seq.Leak_Test_Failed',
    # Label scanning and manual entry tags
    'LABEL_READ_COMPLETE':   'FIX_513D.Label_Barcode.READ_COMPLETE',
    'LABEL_BARCODE_EXTRACT': 'FIX_513D.Label_Barcode.EXTRACT[2]',
    'LABEL_FAULT':           'FIX_513D.Label_Barcode.FAULT_TIMER.DN',
    'TN_MANUAL_ENTRY':       'TN_Manual_Entry',
}

SQL_STATEMENTS = {
    'insert_tn': (
        "INSERT INTO tn (date, finished_serial, component_serial1, component_serial2, status) "
        "VALUES (?, ?, ?, ?, ?)"
    )
}

# Ensure local DB file exists and has the required schema
def ensure_db_schema(db_path: str) -> None:
    """
    Create the database file and the 'tn' table if they do not exist.
    """
    parent = os.path.dirname(db_path) or '.'
    try:
        os.makedirs(parent, exist_ok=True)
    except Exception as e:
        logger.error("Failed to create directory for DB %s: %s", parent, e)
        sys.exit(1)
    try:
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS tn (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT,
                    finished_serial TEXT,
                    component_serial1 TEXT,
                    component_serial2 TEXT,
                    status TEXT
                )
                """
            )
            conn.commit()
            logger.debug("Ensured schema for database %s", db_path)
    except Exception as e:
        logger.error("Failed to create database schema on %s: %s", db_path, e)
        sys.exit(1)

POLL_INTERVAL      = 0.5   # general polling interval
FAST_POLL_INTERVAL = 0.1   # fast polling for fail/datastore
RETRY_DELAY        = 10    # seconds between retries

# --- USB health & formatting helpers ---
def get_device_for_mount(mount_point: str) -> str:
    """
    Return the block device (e.g. /dev/sda1) backing this mount point.
    """
    try:
        out = subprocess.check_output(
            ["findmnt", "-n", "-o", "SOURCE", mount_point],
            text=True
        ).strip()
        return out
    except Exception:
        logger.exception("Cannot determine device for mount %s", mount_point)
        raise

def reformat_usb(mount_point: str) -> None:
    """
    Unmount, format as FAT32, and remount the USB at mount_point.
    """
    dev = get_device_for_mount(mount_point)
    subprocess.run(["umount", mount_point], check=True)
    subprocess.run(["mkfs.vfat", "-F", "32", dev], check=True)
    subprocess.run(["mount", dev, mount_point], check=True)
    logger.info("Reformatted and remounted %s (%s)", mount_point, dev)

def check_db_integrity(db_path: str) -> bool:
    """
    Run PRAGMA integrity_check on the given SQLite DB.
    Returns True if OK, False on any error or corruption.
    """
    try:
        with sqlite3.connect(db_path) as conn:
            result = conn.execute("PRAGMA integrity_check;").fetchone()[0]
        if result != "ok":
            logger.error("Integrity check failed for %s: %s", db_path, result)
            return False
    except Exception as e:
        logger.error("Error checking integrity of %s: %s", db_path, e)
        return False
    return True

# --- Helper functions ---

def sync_db_from_backup(local_db: str) -> None:
    """
    Tri‐directional sync: pick the DB (local, USB1 or USB2) with the most rows
    and copy it over the other two, so the richest DB is the source‐of‐truth.
    """
    # --- Health check USB2 only; if corrupted, reformat & clone from local or USB1 ---
    usb2 = USB_DB_BACKUP2
    if os.path.exists(usb2) and not check_db_integrity(usb2):
        # choose a healthy source: prefer local, else USB1
        healthy = None
        if os.path.exists(local_db) and check_db_integrity(local_db):
            healthy = local_db
        elif os.path.exists(USB_DB_BACKUP) and check_db_integrity(USB_DB_BACKUP):
            healthy = USB_DB_BACKUP

        if healthy:
            logger.warning("Repairing corrupted USB2 DB at %s from %s", usb2, healthy)
            mount_pt = os.path.dirname(usb2)
            reformat_usb(mount_pt)
            os.makedirs(mount_pt, exist_ok=True)
            shutil.copy2(healthy, usb2)
            logger.info("Cloned %s → %s", healthy, usb2)
        else:
            logger.critical(
                "No healthy source to repair corrupted USB2 DB at %s", usb2
            )

    # --- count rows in local, USB1, USB2 and sync richest → others ---
    paths = {
        'local': local_db,
        'usb1':  USB_DB_BACKUP,
        'usb2':  USB_DB_BACKUP2,
    }
    exists = {name: os.path.exists(p) for name, p in paths.items()}
    rows = {}
    for name, p in paths.items():
        if exists[name]:
            try:
                with sqlite3.connect(p) as conn:
                    rows[name] = conn.execute("SELECT COUNT(*) FROM tn").fetchone()[0]
            except Exception:
                rows[name] = -1
                logger.exception("Unable to read DB at %s", p)
        else:
            rows[name] = -1

    if all(r < 0 for r in rows.values()):
        logger.warning("No database files found at any of %s", paths)
        return

    # find the richest DB
    source = max(rows, key=lambda k: rows[k])
    src_path = paths[source]
    src_count = rows[source]

    # copy source to the other two
    for target, tgt_path in paths.items():
        # skip unmounted USB sync targets
        if tgt_path.startswith("/media") and not is_mounted(tgt_path):
            logger.debug(f"Skipping DB sync to unmounted {tgt_path}")
            continue
        if target == source or rows[target] == src_count:
            continue
        try:
            os.makedirs(os.path.dirname(tgt_path) or ".", exist_ok=True)
            shutil.copy2(src_path, tgt_path)
            logger.info(
                "Synced %s from %s: %s → %s (rows %d → %d)",
                target, source, src_path, tgt_path, src_count, rows[target]
            )
        except Exception:
            logger.exception("Failed to copy DB from %s to %s", src_path, tgt_path)


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
        # exit on reset
        rs = plc.read(PLC_TAGS['SEQ_STEP'])
        if rs and rs.value == 0:
            return False
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
        with sqlite3.connect(db_path) as conn2:
            cur2 = conn2.cursor()
            cur2.execute(SQL_STATEMENTS['insert_tn'],
                         (timestamp, finished_serial, lhconv, rhconv, status))
            conn2.commit()
            # clear DB error flag on success
            try:
                plc = globals().get('plc')
                if plc:
                    plc.write((PLC_TAGS['TN_DB_ERROR'], False))
            except Exception:
                pass
            logger.info("Data stored in USB backup database: %s", db_path)
    except Exception as e:
        logger.error("Failed to write TN record to USB backup DB %s: %s", db_path, e)
        # set DB error flag on failure
        try:
            plc = globals().get('plc')
            if plc:
                plc.write((PLC_TAGS['TN_DB_ERROR'], True))
        except Exception:
            pass


def replicate_tn_to_backups(timestamp: str, finished_serial: Any,
                             lhconv: Any, rhconv: Any, status: str) -> None:
    """
    Write the same TN record into both USB backup databases.
    """
    for dbp in USB_DB_BACKUPS:
        insert_tn_record(dbp, timestamp, finished_serial, lhconv, rhconv, status)


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
            replicate_tn_to_backups(ts, 'N/A', lhconv, rhconv, status)
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
        replicate_tn_to_backups(ts, 'N/A', lhconv, rhconv, status)


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
                # expose plc globally for error flag writes
                globals()['plc'] = plc
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
                                replicate_tn_to_backups(ts, finished_serial,
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
                                replicate_tn_to_backups(ts, finished_serial,
                                                     lhconv, rhconv,
                                                     'Passed - Previously failed leak test.')
                            continue

                        # 2) Rework Mode
                        rework = plc.read(PLC_TAGS['REWORK_MODE'])
                        if rework and rework.value:
                            # choose manual or label path
                            # wait for either label read complete, fault timer, or reset
                            while True:
                                # exit on reset
                                seq = plc.read(PLC_TAGS['SEQ_STEP'])
                                if seq and seq.value == 0:
                                    label_mode = None
                                    break
                                if plc.read(PLC_TAGS['LABEL_READ_COMPLETE']).value:
                                    label_mode = 'auto'
                                    break
                                if plc.read(PLC_TAGS['LABEL_FAULT']).value:
                                    label_mode = 'manual'
                                    break
                                time.sleep(POLL_INTERVAL)
                            # common vars
                            ts = time.strftime('%Y-%m-%d %H:%M:%S')
                            if label_mode == 'auto':
                                # read extracted finished SN
                                label_fs = plc.read(PLC_TAGS['LABEL_BARCODE_EXTRACT']).value
                            elif label_mode == 'manual':
                                # manual HMI entry
                                label_fs = plc.read(PLC_TAGS['TN_MANUAL_ENTRY']).value
                            else:
                                # aborted by reset
                                continue
                            # validate inputs
                            if not label_fs or not lhconv or not rhconv:
                                plc.write((PLC_TAGS['TN_DB_ERROR'], True))
                                set_pass(plc, False)
                                logger.error("Rework inputs invalid: FS=%s, LH=%s, RH=%s", label_fs, lhconv, rhconv)
                                if wait_for_fail_or_reset(plc):
                                    cursor.execute(SQL_STATEMENTS['insert_tn'], (ts, label_fs or 'N/A', lhconv or 'N/A', rhconv or 'N/A', 'Rework Rerun Fail'))
                                    conn.commit(); replicate_tn_to_backups(ts, label_fs or 'N/A', lhconv or 'N/A', rhconv or 'N/A', 'Rework Rerun Fail')
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
                                logger.exception("Rework DB lookup error")
                                set_pass(plc, False)
                                if wait_for_fail_or_reset(plc):
                                    cursor.execute(SQL_STATEMENTS['insert_tn'], (ts, label_fs, lhconv, rhconv, 'Rework Rerun Fail'))
                                    conn.commit(); replicate_tn_to_backups(ts, label_fs, lhconv, rhconv, 'Rework Rerun Fail')
                                continue

                            # finalize pass/fail
                            if valid:
                                row_id = rows[0][0]
                                logger.info("Rework Rerun Pass: matched record %s", row_id)
                                set_pass(plc, True)
                                if wait_for_datastore_or_reset(plc):
                                    cursor.execute(
                                        SQL_STATEMENTS['insert_tn'],
                                        (ts, label_fs, lhconv, rhconv, 'Rework Rerun')
                                    )
                                    conn.commit()
                                    replicate_tn_to_backups(ts, label_fs, lhconv, rhconv, 'Rework Rerun')
                            else:
                                logger.error("Rework Rerun Fail: no single matching record for FS=%s, LH=%s, RH=%s", label_fs, lhconv, rhconv)
                                set_pass(plc, False)
                                if wait_for_fail_or_reset(plc):
                                    cursor.execute(
                                        SQL_STATEMENTS['insert_tn'],
                                        (ts, label_fs or 'N/A', lhconv, rhconv, 'Rework Rerun Fail')
                                    )
                                    conn.commit()
                                    replicate_tn_to_backups(ts, label_fs or 'N/A', lhconv, rhconv, 'Rework Rerun Fail')
                            continue

                        # 3) Normal pass / fail
                        lh_pass = check_converter_sn(cursor, 'component_serial1', lhconv, 'LH')
                        rh_pass = check_converter_sn(cursor, 'component_serial2', rhconv, 'RH')

                        if lh_pass and rh_pass:
                            set_pass(plc, True)
                            if wait_for_datastore_or_reset(plc):
                                # If we're at step 143, enforce unique finished_serial
                                if plc.read(PLC_TAGS['SEQ_STEP']).value == 143:
                                    while True:
                                        raw_fs = plc.read(PLC_TAGS['SERIAL_HOLDER']).value or ""
                                        tla_sn = raw_fs[1:] if raw_fs.startswith('T') else raw_fs

                                        cursor.execute(
                                            "SELECT COUNT(*) FROM tn WHERE finished_serial = ?",
                                            (tla_sn,)
                                        )
                                        dup_count = cursor.fetchone()[0]
                                        if dup_count == 0:
                                            # unique—pass the TLA_SN check
                                            plc.write((PLC_TAGS['TN_TLA_SN_CHECK_PASS'], True))
                                            logger.info(
                                                "TN_TLA_SN_CHECK_PASS=1 for serial %s", tla_sn
                                            )
                                            finished_serial = tla_sn
                                            break

                                        # duplicate—bump the PLC serial counter
                                        part_sel = plc.read("FIX_513D.Part_Select").value
                                        serial_tag = f"FIX_513D.Serial_Number[{part_sel}]"
                                        curr = plc.read(serial_tag).value or 0
                                        plc.write((serial_tag, curr + 1))
                                        logger.info(
                                            "Incremented %s to %d", serial_tag, curr + 1
                                        )

                                else:
                                    # non‐143 steps: just read the finished serial
                                    finished_serial = plc.read(
                                        PLC_TAGS['FINISHED_SERIAL']
                                    ).value

                                ts = time.strftime('%Y-%m-%d %H:%M:%S')
                                # insert into local DB
                                cursor.execute(
                                    SQL_STATEMENTS['insert_tn'],
                                    (ts, finished_serial, lhconv, rhconv, 'Passed')
                                )
                                conn.commit()
                                logger.info("Data stored in local database (Passed)")

                                # replicate to USB
                                replicate_tn_to_backups(
                                    ts, finished_serial, lhconv, rhconv, 'Passed'
                                )

                                # final PLC pass‐flag for TLA_SN
                                # (optional: leave TLA_SN_PASS driven by DUP logic above)
                                plc.write(
                                    (PLC_TAGS['TLA_SN_PASS'], True)
                                )
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
    # initialize database and create 'tn' table if missing
    ensure_db_schema(db_file)
    # ensure local DB directory and USB backup directories exist
    os.makedirs(os.path.dirname(db_file) or '.', exist_ok=True)
    for dbp in USB_DB_BACKUPS:
        os.makedirs(os.path.dirname(dbp), exist_ok=True)
    logger.info("Starting tnpy: PLC=%s, DB=%s", args.plc, db_file)
    sync_db_from_backup(db_file)
    monitor_and_update(args.plc, db_file)


if __name__ == "__main__":
    main()