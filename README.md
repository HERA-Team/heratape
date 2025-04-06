![](https://github.com/HERA-Team/heratape/actions/workflows/testsuite.yaml/badge.svg?branch=main)
[![codecov](https://codecov.io/gh/HERA-Team/heratape/graph/badge.svg?token=KhahqtfWJ2)](https://codecov.io/gh/HERA-Team/heratape)

# heratape
A system for backing up the librarian to tape.


# heratape_backup.py  
Designed to run as a service, run one for each tape drive.  Assumes two tape drives, with 0 backing up even days and 1 backing up odd.
Builds a set of files that will fit on a tape and then finds an empty tape and writes them.
Saves the records to the heratape db before writing with a blank write date and then sets the date once finished writing. 


# retry_failed_tape.py
Clean up files that have no write_date. Make sure heratape_backup services are active when running this! Hopefully isn't needed very often.

If the write is interrupted, the date will be None.   At the start, heratape_backup checks for records with no date and halts if it finds any.  The expectation is that the write got interrupted somewhere in the middle. Run "retry_failed_tape.py" to fix this.  Requires operator to manually load the tape to be retried into the drive and rewind it. Use caution! Once rewound a tape can be overwritten. 

# drives
herastore01 has two drives
`/dev/nst0` and `/dev/nst1`
Thge changer is 
`/dev/sg25`



# tape command reference
List the status of the tape archive. Show what tapes are where
`sudo mtx -f /dev/sg25 status`
List the status of an individual drive.
`mt -f /dev/nst0 status`
```
SCSI 2 tape drive:
File number=0, block number=0, partition=0.
Tape block size 0 bytes. Density code 0x60 (no translation).
Soft error count since last status=0
General status bits on (41010000):
 BOT ONLINE IM_REP_EN
```
Rewind a drive
`mt -f /dev/nst0 rewind`


# references
Consult the excellent [gnu tar manual](https://www.gnu.org/software/tar/manual/html_chapter/index.html).
