# Copyright (c) 2025 HERA-Team
# Licensed under the 2-clause BSD License
"""Test tapes."""

import datetime
import re

import pytest
from astropy.time import Time

from heratape import Tapes
from heratape.tapes import add_tape, get_tape


@pytest.mark.parametrize(
    "purchase_date", [Time("2025-01-15"), datetime.datetime(2025, 1, 15)]
)
def test_add_tape(test_session, purchase_date):
    tape_id = "HERA_01"
    tape_type = "foo"
    size = int(8e12)

    add_tape(
        tape_id=tape_id,
        tape_type=tape_type,
        size=size,
        purchase_date=purchase_date,
        session=test_session,
    )

    tape_records = test_session.query(Tapes).all()
    assert len(tape_records) == 1

    test_obj = Tapes(
        tape_id=tape_id,
        tape_type=tape_type,
        size=size,
        purchase_date=datetime.date(2025, 1, 15),
    )
    assert tape_records[0].isclose(test_obj)

    tape_obj = get_tape(tape_id, session=test_session)
    assert tape_obj.isclose(test_obj)

    tape_obj = get_tape("foo", testing=True)
    assert tape_obj is None


@pytest.mark.parametrize(
    ("param", "value", "err_msg"),
    [
        (
            "purchase_date",
            "2025-01-15",
            "purchase date must be a datetime or astropy Time object",
        ),
        (
            "size",
            int(1e6),
            re.escape(
                "size is less than 1TB (note the units are bytes). size: 1000000"
            ),
        ),
    ],
)
def test_add_tape_errors(test_session, param, value, err_msg):
    tape_id = "HERA_01"
    tape_type = "foo"
    size = int(8e12)
    purchase_date = datetime.datetime(2025, 1, 15)

    kwargs = {
        "tape_id": tape_id,
        "tape_type": tape_type,
        "size": size,
        "purchase_date": purchase_date,
        "session": test_session,
    }
    kwargs[param] = value

    with pytest.raises(ValueError, match=err_msg):
        add_tape(**kwargs)
