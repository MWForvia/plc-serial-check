900 Line RPi:
gap900
maint1585
ip - 10.131.201.150
plc ip - 10.131.201.60
see print: "tail f ~/tnpy.log"

300 Line RPi:
gap300
maint1585
ip - 192.168.1.150
plc ip - 192.168.1.1
see print: "tail f ~/tnpy.log"

All scripts create missing files/directories if needed.
If hardware fails, backup RPi on shelf - one for 900, one for 300. These are fully imaged, to get the spare RPi running:
  Unplug failed raspberry pi from the PLC.
  Remove the USB Memory stick
  Plug the USB Memory stick into the new spare RPi
  Plug in ethernet to the new RPi
  Plug in power to the new RPi
On startup it will copy the backed up database from the usb memory to the sdcard directory.

PLC_TAGS (tnpy.py) = 
    'LH_CONV': 'FIX_513D.Conv_Barcode.EXTRACT[2]',
    'RH_CONV': 'FIX_513D.Conv_Barcode_R.EXTRACT[2]',
    'DATASTORE': 'FIX_513D.Seq.Data_Store',
    'UNCLAMP': 'FIX_513D.Main.Unclamp_Part',
    'SEQ_STEP': 'SEQUENCE_STEP',
    'FINISHED_SERIAL': 'ZEBRA.Working_String[20]',
    'SCAN_COMPLETE': 'FIX_513D.Seq.Conv_Barcode_Passed',
    'TN_CHECK_PASS': 'TN_Check_Pass',
    'TN_CHECK_FAIL': 'TN_Check_Fail',
    'TN_DB_ERROR': 'TN_DB_Error'

Saved files (dbtocsv.py - runs daily at 3am)
    CSV Exports (File name has yesterday's date appended)
        /home/gap900/csv_exports/YYYY-MM-DD.csv
        /media/usbdrive/csv_exports/YYYY-MM-DD.csv
    DB Backups (Current and dated)
        /home/gap900/db_backup/tndb900.db
        /media/usbdrive/db_backup/tndb900.db
        /home/gap900/db_backup/tndb900_YYYY-MM-DD.db
        /media/usbdrive/db_backup/tndb900_YYYY-MM-DD.db


