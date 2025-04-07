![](https://github.com/HERA-Team/heratape/actions/workflows/testsuite.yaml/badge.svg?branch=main)
[![codecov](https://codecov.io/gh/HERA-Team/heratape/graph/badge.svg?token=KhahqtfWJ2)](https://codecov.io/gh/HERA-Team/heratape)


# Theory of operation
There two drives. The basic scheme is for drive 0 to handle even and drive 1 to backup odd nights. There is a 24 slot jukebox and a robot arm to move the tapes around. To keep matters simple, drive 0 will load tapes from slots 1 to 12 and drive 1 will load from slots 13-24.*

This scheme is implemented by `heratape_backup.py <drivenum>`, for each drive.  These will run in screen sessions on heratape01.

`heratape_backup.py` handles the process of unloading full tapes, finding empty tapes and loading them.  It also handles scanning for new tapes. The only thing it can't do is put fresh tapes in the jukebox.  When new tapes are needed it will alert with a message in the log. Once in this state it will check for new tapes every 30s.   Once both processes are requesting new tapes it is safe to remove the jukebox and fill it with new tapes. Once replaced, the backup process should identify the fresh tapes and get back to business.

If tape writing is interrupted the service will detect unfinished records next time it is run and alert the operator with a log message.  See `retry_failed_tape.py` to recover from this situation.

The backup archive is tracked in a psql database called `heratape` on `herastore01`. The script connects to the Librarian to choose files to backup and get relevant metadata. `heratape` tables are described in the Files and Tapes modules in this repository. 


*Note that though there are 24 slots, there are only 22 tapes in it at any one time. One must stay empty and one holds a cleaner tape. With two tapes actually physically in the drive 

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
