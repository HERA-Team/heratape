# Copyright (c) 2025 HERA-Team
# Licensed under the 2-clause BSD License
"""Define the database table objects."""

from __future__ import annotations

import datetime

from astropy.time import Time
from sqlalchemy import BigInteger, Column, Date, String
from sqlalchemy.orm import Session

from .base import Base, HTSessionWrapper

# define some default tolerances for various units
DEFAULT_DAY_TOL = {"atol": 1e-3 / (3600.0 * 24.0), "rtol": 0}  # ms
DEFAULT_GPS_TOL = {"atol": 1e-3, "rtol": 0}  # ms


class Tapes(Base):
    """
    Defines the tapes table.

    Attributes
    ----------
    tape_id : String Column
        The unique identifier of the tape. Primary key.
    tape_type : String Column
        The tape type.
    size : BigInteger Column
        Tape capacity in bytes.
    purchase_date : DateTime Column
        Purchase date.

    """

    __tablename__ = "tapes"
    tape_id = Column(String, primary_key=True)
    tape_type = Column(String)
    size = Column(BigInteger, nullable=False)
    purchase_date = Column(Date)


def add_tape(
    *,
    tape_id: str,
    tape_type: str,
    size: int,
    purchase_date: Time | datetime.datetime,
    session: Session | None = None,
    testing: bool = False,
):
    """
    Add a new tape to the Tapes table.

    Parameters
    ----------
    tape_id : str
        The unique identifier of the tape.
    tape_type : str
        The tape type.
    size : int
        Tape capacity in bytes.
    purchase_date : :class:`astropy.time.Time` or datetime
        Purchase date. To pass a human typed date use e.g. Time("2025-01-15").
    session : :class:sqlalchemy.orm.Session, optional
        Database session to use. If None, will start a new session, then close.
    testing : bool
        Option to do the operation on the testing database rather than the default one.

    """
    if isinstance(purchase_date, Time):
        purchase_date = purchase_date.tt.datetime
    elif not isinstance(purchase_date, datetime.datetime):
        raise ValueError("purchase date must be a datetime or astropy Time object")

    if size < 1e12:
        raise ValueError(
            f"size is less than 1TB (note the units are bytes). size: {size}"
        )

    tape_obj = Tapes(
        tape_id=tape_id, tape_type=tape_type, size=size, purchase_date=purchase_date
    )

    with HTSessionWrapper(session=session, testing=testing) as ht_sess:
        ht_sess.add(tape_obj)
        ht_sess.commit()


def get_tape(tape_id: str, session: Session | None = None, testing: bool = False):
    """
    Get a Tape object.

    Parameters
    ----------
    tape_id : str
        The unique identifier of the tape.
    session : :class:sqlalchemy.orm.Session, optional
        Database session to use. If None, will start a new session, then close.
    testing : bool
        Option to do the operation on the testing database rather than the default one.

    """
    with HTSessionWrapper(session=session, testing=testing) as ht_sess:
        record_list = ht_sess.query(Tapes).filter(Tapes.tape_id == tape_id).all()
    if len(record_list) == 0:
        return None
    else:
        return record_list[0]
