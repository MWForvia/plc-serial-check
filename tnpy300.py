#!/usr/bin/env python3
"""tnpy300.py

This script pulls the serial numbers scanned from the LH and RH converters and compares them to a historical database.
It returns if they are a repeat or not, then adds the data to the db.

Database: tndb300.db
Table: tn
Schema:
    id integer primary key autoincrement,
    date text,
    tla1_pn text,
    tla1_tn text,
    tla1_vpps text,
    tla1_duns text,
    conv1_pn text,
    conv1_tn text,
    conv1_vpps text,
    conv1_duns text,
    tla2_pn text,
    tla2_tn text,
    tla2_vpps text,
    tla2_duns text,
    conv2_pn text,
    conv2_tn text,
    conv2_vpps text,
    conv2_duns text
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
import subprocess
from pathlib import Path
from typing import Any
from datetime import datetime, timedelta

# Helper to detect real mount
def is_mounted(path: str) -> bool:
    return os.path.ismount(os.path.dirname(path))

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
debug_log = Path.home() / "tnpy300_debug.log"
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
default_local_db   = "/home/gap300/tndb300.db"
USB_DB_BACKUP      = "/media/usbdrive/db_backup/tndb300.db"
USB_DB_BACKUP2     = "/media/usbdrive2/db_backup/tndb300.db"
USB_DB_BACKUPS     = [USB_DB_BACKUP, USB_DB_BACKUP2]

PLC_TAGS = {
    # Heartbeat and control tags
    'PI_HEARTBEAT':          'PI_Heartbeat',
    'REWORK_MODE':           'REWORK_MODE',
    'REWORK_LABEL_DATE':     'REWORK_LABEL_DATE',
    'REWORK_LABEL_FINISHED': 'REWORK_LABEL_FINISHED',
    'REWORK_LABEL_LH':       'REWORK_LABEL_LH',
    'REWORK_LABEL_RH':       'REWORK_LABEL_RH',
    'TLA_SN_PASS':           'TLA_SN_PASS',
    'TN_CHECK_PASS':         'TN_Check_Pass',
    'TN_CHECK_FAIL':         'TN_Check_Fail',
    'TN_DB_ERROR':           'TN_DB_Error',
    # Data fields in DB order
    'TLA1_PN':               'PN.C2_TLA1_PN',
    'TLA1_TN':               'PN.C2_TLA1_TN',
    'TLA1_VPPS':             'PN.C2_TLA1_VPPS',
    'TLA1_DUNS':             'PN.C2_TLA1_DUNS',
    'CONV1_PN':              'PN.C2_CONV1_PN',
    'CONV1_TN':              'PN.C2_CONV1_TN',
    'CONV1_VPPS':            'PN.C2_CONV1_VPPS',
    'CONV1_DUNS':            'PN.C2_CONV1_DUNS',
    'TLA2_PN':               'PN.C2_TLA2_PN',
    'TLA2_TN':               'PN.C2_TLA2_TN',
    'TLA2_VPPS':             'PN.C2_TLA2_VPPS',
    'TLA2_DUNS':             'PN.C2_TLA2_DUNS',
    'CONV2_PN':              'PN.C2_CONV2_PN',
    'CONV2_TN':              'PN.C2_CONV2_TN',
    'CONV2_VPPS':            'PN.C2_CONV2_VPPS',
    'CONV2_DUNS':            'PN.C2_CONV2_DUNS',
    # Control & sequence tags
    'SCAN_COMPLETE':         'HScan.Good',
    'PART_FAIL':             'Z_Torque_Gun:I.Data[0].3',
    'DATASTORE':             'Data.CMD_Record',
    'SEQ_STEP':              'Local_Step_II_N',
}

SQL_STATEMENTS = {
    'insert_tn': (
        "INSERT INTO tn (date, tla1_pn, tla1_tn, tla1_vpps, tla1_duns, "
        "conv1_pn, conv1_tn, conv1_vpps, conv1_duns, "
        "tla2_pn, tla2_tn, tla2_vpps, tla2_duns, "
        "conv2_pn, conv2_tn, conv2_vpps, conv2_duns) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )
}
# order of PLC tags matching DB columns (tla1_pn,...,conv2_duns)
INSERT_FIELDS = [
    'TLA1_PN','TLA1_TN','TLA1_VPPS','TLA1_DUNS',
    'CONV1_PN','CONV1_TN','CONV1_VPPS','CONV1_DUNS',
    'TLA2_PN','TLA2_TN','TLA2_VPPS','TLA2_DUNS',
    'CONV2_PN','CONV2_TN','CONV2_VPPS','CONV2_DUNS'
]
# initialize local DB & schema if missing
def init_local_db(db_path: str) -> None:
    dirpath = os.path.dirname(db_path)
    os.makedirs(dirpath, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS tn (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT,
                tla1_pn TEXT, tla1_tn TEXT, tla1_vpps TEXT, tla1_duns TEXT,
                conv1_pn TEXT, conv1_tn TEXT, conv1_vpps TEXT, conv1_duns TEXT,
                tla2_pn TEXT, tla2_tn TEXT, tla2_vpps TEXT, tla2_duns TEXT,
                conv2_pn TEXT, conv2_tn TEXT, conv2_vpps TEXT, conv2_duns TEXT
            )
            """
        )
        conn.commit()
def read_insert_fields(plc):
    """
    Read all PN/TN/VPPS/DUNS tags in schema order and return a list of values.
    """
    # read fields and validate non-empty
    vals = [plc.read(PLC_TAGS[key]).value for key in INSERT_FIELDS]
    if any(v is None or (isinstance(v, str) and not v.strip()) for v in vals):
        try:
            plc.write((PLC_TAGS['TN_DB_ERROR'], True))
        except Exception:
            pass
    return vals

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
    # --- Auto-repair USB stick 1 only; skip USB2 auto-format ---
    usb1 = USB_DB_BACKUP
    if os.path.exists(usb1) and not check_db_integrity(usb1):
        # choose a healthy source: prefer local, else USB1
        healthy = None
        if os.path.exists(local_db) and check_db_integrity(local_db):
            healthy = local_db
        elif os.path.exists(USB_DB_BACKUP) and check_db_integrity(USB_DB_BACKUP):
            healthy = USB_DB_BACKUP

        if healthy:
            logger.warning("Repairing corrupted USB1 DB at %s from %s", usb1, healthy)
            mount_pt = os.path.dirname(usb1)
            reformat_usb(mount_pt)
            os.makedirs(mount_pt, exist_ok=True)
            shutil.copy2(healthy, usb1)
            logger.info("Cloned %s → %s", healthy, usb1)
        else:
            logger.critical(
                "No healthy source to repair corrupted USB1 DB at %s", usb1
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
                # flag DB error on read failure
                try:
                    plc_main = globals().get('plc')
                    plc_main.write((PLC_TAGS['TN_DB_ERROR'], True))
                except Exception:
                    pass
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
            # flag DB error on copy failure
            try: plc_main = globals().get('plc'); plc_main.write((PLC_TAGS['TN_DB_ERROR'], True))
            except: pass


def wait_for_tag(plc: LogixDriver, tag_key: str) -> None:
    name = PLC_TAGS[tag_key]
    # wait for false→true transition
    while True:
        # exit on reset
        rs = plc.read(PLC_TAGS['SEQ_STEP'])
        if rs and rs.value == 0:
            return
        val = plc.read(name)
        if val and not val.value:
            break
        time.sleep(POLL_INTERVAL)
    # then true
    while True:
        # exit on reset
        rs = plc.read(PLC_TAGS['SEQ_STEP'])
        if rs and rs.value == 0:
            return
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


def set_pass(plc: LogixDriver, passed: bool) -> None:
    plc.write((PLC_TAGS['TN_CHECK_PASS'], passed))
    plc.write((PLC_TAGS['TN_CHECK_FAIL'], not passed))


def insert_tn_record(db_path: str, record_values: list) -> None:
    # skip writes to unmounted USB backup DBs
    if db_path.startswith("/media") and not is_mounted(db_path):
        logger.debug(f"Skipping TN record write to unmounted {db_path}")
        return
    try:
        with sqlite3.connect(db_path) as conn2:
            cur2 = conn2.cursor()
            cur2.execute(SQL_STATEMENTS['insert_tn'], record_values)
            conn2.commit()
            # clear DB error flag on success
            plc = None
            try:
                # retrieve PLC instance from caller context if available
                plc = globals().get('plc')
                if plc:
                    plc.write((PLC_TAGS['TN_DB_ERROR'], False))
            except Exception:
                pass
            logger.info("Data stored in USB backup database: %s", db_path)
    except Exception as e:
        logger.error("Failed to write TN record to USB backup DB %s: %s", db_path, e)
        # set DB error flag
        try:
            plc = globals().get('plc')
            if plc:
                plc.write((PLC_TAGS['TN_DB_ERROR'], True))
        except Exception:
            pass


def replicate_tn_to_backups(record_values: list) -> None:
    """
    Write the same TN record into both USB backup databases.
    """
    for dbp in USB_DB_BACKUPS:
        insert_tn_record(dbp, record_values)


def handle_fail(lh_pass: bool, rh_pass: bool, plc: LogixDriver,
                cursor: sqlite3.Cursor, lhconv: Any, rhconv: Any) -> None:
    # 1) Duplicate-SN logic
    if not lh_pass and not rh_pass:
        base_status = "LH & RH SN Dupe - Failed"
        cursor.execute(
            "SELECT tla1_tn FROM tn "
            "WHERE conv1_tn=? OR conv2_tn=? "
            "ORDER BY id ASC LIMIT 1",
            (lhconv, rhconv)
        )
    elif not lh_pass:
        base_status = "LH SN Dupe - Failed"
        cursor.execute(
            "SELECT tla1_tn FROM tn "
            "WHERE conv1_tn=? "
            "ORDER BY id ASC LIMIT 1",
            (lhconv,)
        )
    else:
        base_status = "RH SN Dupe - Failed"
        cursor.execute(
            "SELECT tla1_tn FROM tn "
            "WHERE conv2_tn=? "
            "ORDER BY id ASC LIMIT 1",
            (rhconv,)
        )

    row = cursor.fetchone()
    first_tla = row[0] if row and row[0] else "Unknown"
    status = f"{base_status} (first TLA: {first_tla})"
    logger.error(status)
    if wait_for_fail_or_reset(plc):
        ts = time.strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute(SQL_STATEMENTS['insert_tn'], [ts] + read_insert_fields(plc))
        cursor.connection.commit()
        logger.error("Failed TN Check - Data stored in database")
        # replicate full failed record to USB backups
        record_values = [ts] + read_insert_fields(plc)
        replicate_tn_to_backups(record_values)


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
                        # read converter SNs with error handling
                        try:
                            res_lh = plc.read(PLC_TAGS['CONV1_TN'])
                            res_rh = plc.read(PLC_TAGS['CONV2_TN'])
                        except CommError as e:
                            logger.error("PLC read error: %s", e)
                            continue

                        if not res_lh or not res_rh or res_lh.value is None or res_rh.value is None:
                            logger.error("Invalid converter SN read – LH=%s, RH=%s", res_lh, res_rh)
                            continue

                        lhconv = res_lh.value
                        rhconv = res_rh.value
                        # 0) Rework Mode
                        rework = plc.read(PLC_TAGS['REWORK_MODE'])
                        if rework and rework.value:
                            # helper: poll a PLC tag until it returns a non-empty string or exit on reset
                            def wait_for_label(tag_key: str) -> str:
                                tag = PLC_TAGS[tag_key]
                                while True:
                                    seq = plc.read(PLC_TAGS['SEQ_STEP'])
                                    if seq and seq.value == 0:
                                        return None
                                    r = plc.read(tag)
                                    val = r.value if r else None
                                    if isinstance(val, str) and val.strip():
                                        return val
                                    time.sleep(POLL_INTERVAL)

                            # poll the raw Julian date and convert to ISO YYYY-MM-DD
                            raw_julian = wait_for_label('REWORK_LABEL_DATE')
                            if raw_julian is None:
                                continue
                            yy = int(raw_julian[:2])
                            doy = int(raw_julian[2:])
                            label_date = (
                                datetime(2000 + yy, 1, 1)
                                + timedelta(days=doy - 1)
                            ).strftime('%Y-%m-%d')

                            # now poll the rest of the label fields
                            label_fs   = wait_for_label('REWORK_LABEL_FINISHED')
                            label_lh   = wait_for_label('REWORK_LABEL_LH')
                            label_rh   = wait_for_label('REWORK_LABEL_RH')

                            # look for an exact match on the date portion of our timestamp
                            cursor.execute(
                                "SELECT id FROM tn "
                                "WHERE substr(date,1,10) = ? "
                                "  AND tla1_tn = ? "
                                "  AND conv1_tn = ? "
                                "  AND conv2_tn = ?",
                                (label_date, label_fs, label_lh, label_rh)
                            )
                            row = cursor.fetchone()

                            valid = False
                            if row:
                                row_id = row[0]
                                # ensure none of those SNs appear in any other row
                                cursor.execute(
                                    "SELECT COUNT(*) FROM tn WHERE tla1_tn = ? AND id != ?",
                                    (label_fs, row_id)
                                )
                                fs_dup = cursor.fetchone()[0]
                                cursor.execute(
                                    "SELECT COUNT(*) FROM tn WHERE conv1_tn = ? AND id != ?",
                                    (label_lh, row_id)
                                )
                                lh_dup = cursor.fetchone()[0]
                                cursor.execute(
                                    "SELECT COUNT(*) FROM tn WHERE conv2_tn = ? AND id != ?",
                                    (label_rh, row_id)
                                )
                                rh_dup = cursor.fetchone()[0]

                                if fs_dup == 0 and lh_dup == 0 and rh_dup == 0:
                                    # new: also verify the scanned LH_CONV/RH_CONV match the label & DB
                                    if lhconv == label_lh and rhconv == label_rh:
                                        valid = True
                                    else:
                                        logger.error(
                                            "Rework Fail: scanned converter SNs do not match label data "
                                            "(scanned LH=%s vs label %s, scanned RH=%s vs label %s)",
                                            lhconv, label_lh, rhconv, label_rh
                                        )

                            if valid:
                                logger.info("Rework Pass: matched row %s", row_id)
                                set_pass(plc, True)
                                if wait_for_datastore_or_reset(plc):
                                    ts = time.strftime("%Y-%m-%d %H:%M:%S")
                                    cursor.execute(SQL_STATEMENTS['insert_tn'], [ts] + read_insert_fields(plc))
                                    conn.commit()
                                    logger.info("Data stored in local DB (Rework Pass)")
                                    # replicate full rework-pass record to USB backups
                                    record_values = [ts] + read_insert_fields(plc)
                                    replicate_tn_to_backups(record_values)
                            else:
                                logger.error(
                                    "Rework Fail: label data date=%s, fs=%s, lh=%s, rh=%s",
                                    label_date, label_fs, label_lh, label_rh
                                )
                                set_pass(plc, False)
                                if wait_for_fail_or_reset(plc):
                                    ts = time.strftime("%Y-%m-%d %H:%M:%S")
                                    cursor.execute(SQL_STATEMENTS['insert_tn'], [ts] + read_insert_fields(plc))
                                    conn.commit()
                                    logger.error("Data stored in local DB (Rework Fail)")
                                    # replicate full rework-fail record to USB backups
                                    record_values = [ts] + read_insert_fields(plc)
                                    replicate_tn_to_backups(record_values)
                            continue

                        # 1) Normal pass / fail
                        lh_pass = check_converter_sn(cursor, 'conv1_tn', lhconv, 'LH')
                        rh_pass = check_converter_sn(cursor, 'conv2_tn', rhconv, 'RH')

                        if lh_pass and rh_pass:
                            set_pass(plc, True)
                            if wait_for_datastore_or_reset(plc):
                                # Always read the finished serial directly (no unique SN increment)
                                # finished serial is same as TLA1_TN
                                finished_serial = plc.read(PLC_TAGS['TLA1_TN']).value

                                ts = time.strftime('%Y-%m-%d %H:%M:%S')
                                # insert into local DB
                                cursor.execute(SQL_STATEMENTS['insert_tn'], [ts] + read_insert_fields(plc))
                                conn.commit()
                                logger.info("Data stored in local database (Passed)")

                                # replicate to USB
                                # replicate full passed record to USB backups
                                record_values = [ts] + read_insert_fields(plc)
                                replicate_tn_to_backups(record_values)

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
    parser.add_argument("--plc", default="192.168.1.1", help="PLC IP address")
    parser.add_argument("--db", default=default_local_db, help="Path to SQLite DB file")
    args = parser.parse_args()

    db_file = os.path.expanduser(args.db)
    logger.info("Starting tnpy: PLC=%s, DB=%s", args.plc, db_file)
    init_local_db(db_file)
    sync_db_from_backup(db_file)
    monitor_and_update(args.plc, db_file)


if __name__ == "__main__":
    main()