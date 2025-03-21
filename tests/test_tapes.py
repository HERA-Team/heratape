# Copyright (c) 2025 HERA-Team
# Licensed under the 2-clause BSD License
"""Test tapes."""

import datetime

from astropy.time import Time

from heratape import Tapes
from heratape.tapes import add_tape, get_tape


def test_add_tape(test_session):
    tape_id = "HERA_01"
    tape_type = "foo"
    size = int(8e12)
    purchase_date = Time("2025-01-15")

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
