import sqlite3
import csv
import time
import os
import logging
from datetime import datetime, timedelta


# Database path
db_path = "/home/gap900/tndb900.db"
# CSV export directory
csv_dir = "/home/gap900/csv_exports"
usb_csv_dir = "/media/usb/csv_exports"

# Configure logging
logging.basicConfig(
    filename='dbtocsv.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def log_and_print(level: str, message: str) -> None:
    """
    Log a message at the specified level and print it to the console.
    """
    getattr(logging, level)(message)
    print(message)

def export_csv():
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Select all entries from the tn table
        cursor.execute("SELECT id, date, finished_serial, component_serial1, component_serial2, status FROM tn")
        rows = cursor.fetchall()

        if rows:
            # Generate CSV filename with yesterday's date
            yesterday = datetime.now() - timedelta(days=1)
            csv_filename = yesterday.strftime("%Y-%m-%d") + ".csv"

            # Write rows to CSV folder on SD card
            csv_path = os.path.join(csv_dir, csv_filename)
            write_csv(csv_path, rows)

            # Write rows to CSV folder on USB Media
            usb_csv_path = os.path.join(usb_csv_dir, csv_filename)
            write_csv(usb_csv_dir, rows)

            log_and_print('info', f"Exported {len(rows)} new entries to {csv_path} and {usb_csv_path}")

        else:
            log_and_print('info', "No new entries to export.")

    except sqlite3.Error as e:
        log_and_print('error', f"SQLite error: {e}")

    finally:
        if conn:
            conn.close()

def write_csv(csv_path, usb_csv_path, rows):
    """Helper function to write rows to CSV file."""
    with open(csv_path, mode='w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file, delimiter='\t')
        # Write header
        csv_writer.writerow(['id', 'date', 'finished_serial', 'component_serial1', 'component_serial2', 'status'])
        # Write rows
        csv_writer.writerows(rows)
    log_and_print('info', f"CSV file written to {csv_path} and {usb_csv_path}")

def main():
    # Run the export function
    export_csv()

if __name__ == "__main__":
    main()