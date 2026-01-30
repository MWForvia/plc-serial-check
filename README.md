900 Line RPi:
gap900
maint1585
ip - 10.131.201.150
plc ip - 10.131.201.60
view log files via ssh (putty) via terminal commands:
tail -f tnpy900.log
tail -f tnpy900_debug.log

300 Line RPi:
gap300
maint1585
ip - 192.168.1.150
plc ip - 192.168.1.1
view log files via ssh (putty) via terminal commands:
tail -f tnpy300.log
tail -f tnpy300_debug.log


All scripts create missing files/directories if needed.
If hardware fails, backup RPi on shelf - one for 900, one for 300. These are fully imaged, to get the spare RPi running:
  Unplug failed raspberry pi from the PLC.
  Remove the USB Memory stick
  Plug the USB Memory stick into the new spare RPi
  Plug in ethernet to the new RPi
  Plug in power to the new RPi
On startup it will copy the backed up database from the usb memory to the sdcard directory.

Saved files (dbtocsv.py - runs daily at 3am)
    CSV Exports (File name has yesterday's date appended)
        /home/gap900/csv_exports/YYYY-MM-DD.csv
        /media/usbdrive/csv_exports/YYYY-MM-DD.csv
    DB Backups (Current and dated)
        /home/gap900/db_backup/tndb900.db
        /media/usbdrive/db_backup/tndb900.db
        /home/gap900/db_backup/tndb900_YYYY-MM-DD.db
        /media/usbdrive/db_backup/tndb900_YYYY-MM-DD.db


