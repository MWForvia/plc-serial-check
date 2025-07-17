import sqlite3
import csv
import time
import os
from datetime import datetime, timedelta

# Database path
db_path = "/home/gap900/tndb900.db"
# CSV export directory
csv_dir = "/home/gap900/csv_exports"
usb_csv_dir = "/media/usb/csv_exports"

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
            csv_path = os.path.join(usb_csv_dir, csv_filename)
            write_csv(usb_csv_dir, rows)

            print(f"Exported {len(rows)} new entries to {csv_path} and {usb_csv_dir}")

        else:
            print("No new entries to export.")

    except sqlite3.Error as e:
        print(f"SQLite error: {e}")

    finally:
        if conn:
            conn.close()

def write_csv(csv_path, rows):
    """Helper function to write rows to a CSV file."""
    with open(csv_path, mode='w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file, delimiter='\t')
        # Write header
        csv_writer.writerow(['id', 'date', 'finished_serial', 'component_serial1', 'component_serial2', 'status'])
        # Write rows
        csv_writer.writerows(rows)

def main():
    # Run the export function
    export_csv()

if __name__ == "__main__":
    main()