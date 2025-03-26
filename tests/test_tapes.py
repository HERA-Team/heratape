# Copyright (c) 2025 HERA-Team
# Licensed under the 2-clause BSD License
"""Test tapes."""

import copy
import datetime
import re

import pytest
from astropy.time import Time

from heratape import Tapes
from heratape.tapes import add_tape, get_tape, update_tape


@pytest.mark.parametrize(
    "purchase_date",
    [Time("2025-01-15"), datetime.datetime(2025, 1, 15), datetime.date(2025, 1, 15)],
)
def test_add_tape(test_session, tape_dict, purchase_date):
    dict_use = copy.deepcopy(tape_dict)
    dict_use["purchase_date"] = purchase_date

    add_tape(session=test_session, **dict_use)

    tape_records = test_session.query(Tapes).all()
    assert len(tape_records) == 1

    test_obj = Tapes(**tape_dict)
    assert tape_records[0].isclose(test_obj)

    tape_obj = get_tape(tape_dict["tape_id"], session=test_session)
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
def test_add_tape_errors(test_session, tape_dict, param, value, err_msg):
    tape_dict[param] = value

    with pytest.raises(ValueError, match=err_msg):
        add_tape(session=test_session, **tape_dict)


@pytest.mark.parametrize(
    "update_dict",
    [
        {"tape_type": "foo"},
        {"size": int(5e12)},
        {"purchase_date": Time("2025-02-15")},
        {"purchase_date": datetime.datetime(2025, 2, 15)},
        {"purchase_date": datetime.date(2025, 2, 15)},
        {},
    ],
)
def test_update_tapes(test_session, tape_dict, update_dict):
    add_tape(session=test_session, **tape_dict)

    update_tape(tape_id=tape_dict["tape_id"], session=test_session, **update_dict)

    exp_dict = copy.deepcopy(tape_dict)

    for key, value in update_dict.items():
        exp_dict[key] = value
    if isinstance(exp_dict["purchase_date"], Time):
        exp_dict["purchase_date"] = exp_dict["purchase_date"].tt.datetime.date()
    elif isinstance(exp_dict["purchase_date"], datetime.datetime):
        exp_dict["purchase_date"] = exp_dict["purchase_date"].date()

    tape_records = test_session.query(Tapes).all()
    assert len(tape_records) == 1

    test_obj = Tapes(**exp_dict)
    assert tape_records[0].isclose(test_obj)


@pytest.mark.parametrize(
    ("update_dict", "err_msg"),
    [
        (
            {"purchase_date": "2025-01-15"},
            "purchase date must be a datetime or astropy Time object",
        ),
        (
            {"size": int(1e6)},
            re.escape(
                "size is less than 1TB (note the units are bytes). size: 1000000"
            ),
        ),
    ],
)
def test_update_tape_errors(test_session, tape_dict, update_dict, err_msg):
    add_tape(session=test_session, **tape_dict)

    with pytest.raises(ValueError, match=err_msg):
        update_tape(tape_id=tape_dict["tape_id"], session=test_session, **update_dict)
