# Copyright (c) 2025 HERA-Team
# Licensed under the 2-clause BSD License
"""Define the database table objects."""

from __future__ import annotations

import datetime
from math import floor
from pathlib import Path

from astropy.time import Time
from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Float,
    ForeignKey,
    String,
    insert,
    update,
)
from sqlalchemy.orm import Session

from heratape.tapes import get_tape

from .base import Base, HTSessionWrapper

# define some default tolerances for various units
DEFAULT_DAY_TOL = {"atol": 1e-3 / (3600.0 * 24.0), "rtol": 0}  # ms
DEFAULT_GPS_TOL = {"atol": 1e-3, "rtol": 0}  # ms


class Files(Base):
    """
    Defines the files table, listing all the files written to tape.

    Attributes
    ----------
    filebase : String Column
        The file base name. Primary key.
    filepath : String Column
        The full file path.
    tape_id : String
        Tape this file is written to. Foreign key into the tapes table.
    obsid : BigInteger Column
        Observation identification number, generally equal to the floor of
        the start time in gps seconds.
    jd_start : Float Column
        Observation start time in JDs.
    jd : BigInteger Column
        Integer JD for this file. Used to group files by day.
    size : BigInteger Column
        File size in bytes.
    write_date : DateTime column
        Date and time when the file was written to tape.

    """

    __tablename__ = "files"
    filebase = Column(String, primary_key=True)
    filepath = Column(String)
    tape_id = Column(String, ForeignKey("tapes.tape_id"), nullable=False)
    obsid = Column(BigInteger)
    jd_start = Column(Float)
    jd = Column(BigInteger, index=True)
    size = Column(BigInteger, nullable=False)
    write_date = Column(DateTime)

    # tolerances set to 1ms
    tols = {"jd_start": DEFAULT_DAY_TOL}


def add_files_to_tape(
    *,
    tape_id: str,
    filepath_list: list[str],
    obsid_list: list[int],
    jd_start_list: list[float],
    size_list: list[int],
    write_date: Time | datetime.datetime | None = None,
    session: Session | None = None,
    testing: bool = False,
):
    """
    Add a list of files on one tape to the Files table.

    Parameters
    ----------
    tape_id : str
        The unique identifier of the tape the files are written to.
    filepath_list : list of str
        List of full filepaths for all files.
    obsid_list : list of int
        List of obsids for each file. Must be the same length as filepath_list.
    jd_start_list : list of float
        List of start times in JD for each file. Must be the same length as
        filepath_list.
    size_list : list of int
        List of file sizes in bytes. Must be the same length as filepath_list.
    write_date : :class:`astropy.time.Time` or datetime, optional
        The date and time the files are written. To pass a human typed date use e.g.
        Time("2025-01-15 12:15:00").
    session : :class:sqlalchemy.orm.Session, optional
        Database session to use. If None, will start a new session, then close.
    testing : bool
        Option to do the operation on the testing database rather than the default one.

    """
    tape_record = get_tape(tape_id, session=session, testing=testing)
    if tape_record is None:
        raise ValueError(
            f"tape {tape_id} is not yet in the tapes table. Use the add_tape "
            "function to add it before adding files to it."
        )

    if isinstance(write_date, Time):
        write_date = write_date.tt.datetime
    elif write_date is not None and not isinstance(write_date, datetime.datetime):
        raise ValueError("If set, write_date must be a datetime or astropy Time object")

    n_files = len(filepath_list)
    n_obsids = len(obsid_list)
    if n_obsids != n_files:
        raise ValueError(
            f"obsid_list is length {n_obsids}, filepath_list is length {n_files}. "
            "They must be the same length."
        )
    n_jds = len(jd_start_list)
    if n_jds != n_files:
        raise ValueError(
            f"jd_start_list is length {n_jds}, filepath_list is length {n_files}. "
            "They must be the same length."
        )
    n_sizes = len(size_list)
    if n_sizes != n_files:
        raise ValueError(
            f"size_list is length {n_sizes}, filepath_list is length {n_files}. "
            "They must be the same length."
        )

    filebase_list = [Path(filepath).name for filepath in filepath_list]
    jd_int_list = [int(floor(jd)) for jd in jd_start_list]

    file_dict_list = [
        {
            "filebase": fbase,
            "filepath": fpath,
            "tape_id": tape_id,
            "obsid": obsid,
            "jd_start": jd_start,
            "jd": jd_int,
            "size": size,
            "write_date": write_date,
        }
        for fbase, fpath, obsid, jd_start, jd_int, size in zip(
            filebase_list,
            filepath_list,
            obsid_list,
            jd_start_list,
            jd_int_list,
            size_list,
            strict=True,
        )
    ]

    with HTSessionWrapper(session=session, testing=testing) as ht_sess:
        # This does a bulk insert in sqlalchemy>=2.0
        ht_sess.execute(insert(Files), file_dict_list)


def get_all_jds(*, session: Session | None = None, testing: bool = False):
    """
    Return a list of all integer JDs with entries in Files table.

    Parameters
    ----------
    session : :class:sqlalchemy.orm.Session, optional
        Database session to use. If None, will start a new session, then close.
    testing : bool
        Option to do the operation on the testing database rather than the default one.

    """
    with HTSessionWrapper(session=session, testing=testing) as ht_sess:
        jd_tuple_list = ht_sess.query(Files.jd).distinct().all()
    jd_list = [val[0] for val in jd_tuple_list]
    return jd_list


def set_write_date(
    file_list: list[str],
    write_date: Time | datetime.datetime,
    session: Session | None = None,
    testing: bool = False,
):
    """
    Set the write date on a list of files in the Files table.

    Parameters
    ----------
    filebase_list : list of str
        List of files (either full paths or file base names) to set the write date for.
    write_date : :class:`astropy.time.Time` or datetime
        The date the files were written. To pass a human typed date use e.g.
        Time("2025-01-15 12:15:00").
    session : :class:sqlalchemy.orm.Session, optional
        Database session to use. If None, will start a new session, then close.
    testing : bool
        Option to do the operation on the testing database rather than the default one.

    """
    if isinstance(write_date, Time):
        write_date = write_date.tt.datetime
    elif not isinstance(write_date, datetime.datetime):
        raise ValueError("write_date must be a datetime or astropy Time object")

    # get file base names. a no-op if basenames are already passed in.
    filebase_list = [Path(filepath).name for filepath in file_list]

    file_dict_list = [
        {"filebase": fbase, "write_date": write_date} for fbase in filebase_list
    ]
    with HTSessionWrapper(session=session, testing=testing) as ht_sess:
        # This does a bulk update in sqlalchemy>=2.0
        ht_sess.execute(update(Files), file_dict_list)


def update_file(
    filename: str,
    *,
    tape_id: str | None = None,
    obsid: int | None = None,
    jd_start: str | None = None,
    size: int | None = None,
    write_date: Time | datetime.datetime | None = None,
    session: Session | None = None,
    testing: bool = False,
):
    """
    Update a single file record.

    The filename must be passed and the base name must match an existing entry
    in the database. If a full file path is passed, the filepath column will be
    updated. The other column values (tape_id, obsid, jd_start, size, write_date)
    should only be passed if you want to update them.

    Parameters
    ----------
    filename : str
        The filename to update the record for. This can be a file base name or
        a full filepath. If a full file path is passed, the filepath column
        will be updated. The file base name must already exist in the database.
    tape_id : str
        The unique identifier of the tape the files are written to.
    obsid : int
        Observation ID (obsid) for the file. Typically the floor of the start time
        in gps seconds.
    jd_start : float
        Start time in JD for the file.
    size : int
        File size in bytes.
    write_date : :class:`astropy.time.Time` or datetime, optional
        The date the file was written. To pass a human typed date use e.g.
        Time("2025-01-15 12:15:00").
    session : :class:sqlalchemy.orm.Session, optional
        Database session to use. If None, will start a new session, then close.
    testing : bool
        Option to do the operation on the testing database rather than the default one.

    """
    if tape_id is not None:
        tape_record = get_tape(tape_id, session=session, testing=testing)
        if tape_record is None:
            raise ValueError(
                f"tape {tape_id} is not yet in the tapes table. Use the add_tape "
                "function to add it before adding files to it."
            )

    if isinstance(write_date, Time):
        write_date = write_date.tt.datetime
    elif write_date is not None and not isinstance(write_date, datetime.datetime):
        raise ValueError("If set, write_date must be a datetime or astropy Time object")

    filebase = Path(filename).name

    update_vals = {}
    if filename != filebase:
        update_vals["filepath"] = filename
    if tape_id is not None:
        update_vals["tape_id"] = tape_id
    if obsid is not None:
        update_vals["obsid"] = obsid
    if jd_start is not None:
        update_vals["jd_start"] = jd_start
        update_vals["jd"] = int(floor(jd_start))
    if size is not None:
        update_vals["size"] = size
    if write_date is not None:
        update_vals["write_date"] = write_date

    if len(update_vals) == 0:
        return

    stmt = update(Files).where(Files.filebase == filebase).values(**update_vals)
    with HTSessionWrapper(session=session, testing=testing) as ht_sess:
        ht_sess.execute(stmt)
