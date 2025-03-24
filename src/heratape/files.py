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
    Index,
    String,
    insert,
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
        Date when the file was written to tape.

    """

    __tablename__ = "files"
    filebase = Column(String, primary_key=True)
    filepath = Column(String)
    tape_id = Column(String, ForeignKey("tapes.tape_id"), nullable=False)
    obsid = Column(BigInteger)
    jd_start = Column(Float)
    jd = Column(BigInteger)
    size = Column(BigInteger, nullable=False)
    write_date = Column(DateTime, nullable=False)

    # tolerances set to 1ms
    tols = {"jd_start": DEFAULT_DAY_TOL}

    Index("idx_jd", "jd")


def add_files_to_tape(
    *,
    tape_id: str,
    write_date: Time | datetime.datetime,
    filepath_list: list[str],
    obsid_list: list[int],
    jd_start_list: list[float],
    size_list: list[int],
    session: Session | None = None,
    testing: bool = False,
):
    """
    Add a list of files on one tape to the Files table.

    Parameters
    ----------
    tape_id : str
        The unique identifier of the tape the files are written to.
    write_date : :class:`astropy.time.Time` or datetime
        The date the files are written. To pass a human typed date use e.g.
        Time("2025-01-15").
    filepath_list : list of str
        List of full filepaths for all files.
    obsid_list : list of int
        List of obsids for each file. Must be the same length as filepath_list.
    jd_start_list : list of float
        List of start times in JD for each file. Must be the same length as
        filepath_list.
    size_list : list of int
        List of file sizes in bytes. Must be the same length as filepath_list.
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
    elif not isinstance(write_date, datetime.datetime):
        raise ValueError("purchase date must be a datetime or astropy Time object")

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
