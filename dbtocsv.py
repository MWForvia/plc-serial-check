import sqlite3
import csv
import time
import os
from datetime import datetime, timedelta

# Database path
db_path = "/home/gap900/tndb900.db"
# CSV export directory
csv_dir = "/home/gap900/csv_exports"

def export_csv(last_exported_id):
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Select entries with ID greater than last_exported_id
        cursor.execute("SELECT id, date, finished_serial, component_serial1, component_serial2, status FROM tn")
        rows = cursor.fetchall()

        if rows:
            # Generate CSV filename with yesterday's date
            yesterday = datetime.now() - timedelta(days=1)
            csv_filename = yesterday.strftime("%Y-%m-%d") + ".csv"
            csv_path = os.path.join(csv_dir, csv_filename)

            # Write rows to CSV file
            with open(csv_path, mode='w', newline='') as csv_file:
                csv_writer = csv.writer(csv_file, delimiter='\t')
                # Write header
                csv_writer.writerow(['id', 'date', 'finished_serial', 'component_serial1', 'component_serial2', 'status'])
                # Write rows
                csv_writer.writerows(rows)

            print(f"Exported {len(rows)} new entries to {csv_path}")

        else:
            print("No new entries to export.")

    except sqlite3.Error as e:
        print(f"SQLite error: {e}")

    finally:
        if conn:
            conn.close()

def main():
    # Run the export function
    export_csv(last_exported_id)

if __name__ == "__main__":
    main()