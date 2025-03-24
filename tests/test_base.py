# Copyright (c) 2025 HERA-Team
# Licensed under the 2-clause BSD License
"""Test base."""

import datetime

import numpy as np

import heratape
from heratape import Files, Tapes


def tape_obj(
    tape_id="HERA_01",
    tape_type="foo",
    size=int(8e12),
    purchase_date=datetime.date(2025, 1, 15),
):
    return Tapes(
        tape_id=tape_id, tape_type=tape_type, size=size, purchase_date=purchase_date
    )


def files_obj(
    filebase="zen_2459562.5_sum.uvh5",
    filepath="/home/data/2459562/zen_2459562.5_sum.uvh5",
    tape_id="HERA_01",
    obsid=1323475218,
    jd_start=2459562.5,
    jd=2459562,
    size=int(2e9),
    write_date=datetime.datetime(2025, 3, 15, 10, 20, 6),
):
    return Files(
        filebase=filebase,
        filepath=filepath,
        tape_id=tape_id,
        obsid=obsid,
        jd_start=jd_start,
        jd=jd,
        size=size,
        write_date=write_date,
    )


def test_base_repr():
    expected_repr = "<Tapes(HERA_01, foo, 8000000000000, 2025-01-15)>"
    assert str(tape_obj()) == expected_repr


def test_isclose_class():
    assert not tape_obj().isclose(files_obj())


def test_isclose_int():
    test_obj1 = tape_obj(size=3)
    test_obj2 = tape_obj(size=3)
    assert test_obj1.isclose(test_obj2)

    test_obj3 = tape_obj(size=5)
    assert not test_obj1.isclose(test_obj3)

    test_obj4 = tape_obj(size=3.0)
    assert not test_obj1.isclose(test_obj4)

    test_obj5 = tape_obj(size=None)
    assert not test_obj1.isclose(test_obj5)

    test_obj6 = tape_obj(size=None)
    assert test_obj5.isclose(test_obj6)


def test_isclose_bool():
    test_obj1 = tape_obj(tape_type=True)
    test_obj2 = tape_obj(tape_type=True)
    assert test_obj1.isclose(test_obj2)

    test_obj3 = tape_obj(tape_type=False)
    assert not test_obj1.isclose(test_obj3)

    test_obj4 = tape_obj(tape_type=1)
    assert not test_obj1.isclose(test_obj4)


def test_isclose_str():
    test_obj1 = tape_obj(tape_type="foo")
    test_obj2 = tape_obj(tape_type="foo")
    assert test_obj1.isclose(test_obj2)

    test_obj3 = tape_obj(tape_type="bar")
    assert not test_obj1.isclose(test_obj3)

    test_obj4 = tape_obj(tape_type=5.0)
    assert not test_obj1.isclose(test_obj4)


def test_isclose_date():
    test_obj1 = tape_obj(purchase_date=datetime.date(2025, 3, 10))
    test_obj2 = tape_obj(purchase_date=datetime.date(2025, 3, 10))
    assert test_obj1.isclose(test_obj2)

    test_obj3 = tape_obj(purchase_date=datetime.date(2025, 3, 11))
    assert not test_obj1.isclose(test_obj3)

    test_obj4 = tape_obj(purchase_date=datetime.datetime(2025, 3, 10, 5, 3, 8))
    assert not test_obj1.isclose(test_obj4)

    test_obj5 = tape_obj(purchase_date=None)
    assert not test_obj1.isclose(test_obj5)


def test_isclose_datetime():
    test_obj1 = files_obj(write_date=datetime.datetime(2025, 3, 10))
    test_obj2 = files_obj(write_date=datetime.datetime(2025, 3, 10, 0, 0, 0))
    assert test_obj1.isclose(test_obj2)

    test_obj3 = files_obj(write_date=datetime.date(2025, 3, 11))
    assert not test_obj1.isclose(test_obj3)

    test_obj4 = files_obj(write_date=datetime.datetime(2025, 3, 10, 5, 3, 8))
    assert not test_obj1.isclose(test_obj4)

    test_obj5 = files_obj(write_date=None)
    assert not test_obj1.isclose(test_obj5)


def test_isclose_float():
    test_obj1 = files_obj(jd_start=3.1415)
    test_obj1.tols = {"jd_start": {"atol": 1e-3, "rtol": 0}}

    test_obj2 = files_obj(jd_start=3.1415)
    assert test_obj1.isclose(test_obj2)

    test_obj3 = files_obj(jd_start=3.1414)
    assert test_obj1.isclose(test_obj3)

    test_obj4 = files_obj(jd_start=3.1426)
    assert not test_obj1.isclose(test_obj4)

    test_obj5 = files_obj(jd_start=3.1415 + 9e-9)
    assert test_obj2.isclose(test_obj5)

    test_obj6 = files_obj(jd_start=(3.1415 * (1 + 1e-5)) + 1.1e-8)
    assert not test_obj2.isclose(test_obj6)


def test_isclose_float_array():
    test_obj1 = files_obj(jd_start=np.array([5.1, 4.2, 3.3]))
    test_obj1.tols = {"jd_start": {"atol": 1e-3, "rtol": 0}}

    test_obj2 = files_obj(jd_start=np.array([5.1, 4.2, 3.3]))
    assert test_obj1.isclose(test_obj2)

    test_obj3 = files_obj(jd_start=np.array([5.1001, 4.2, 3.3]))
    assert test_obj1.isclose(test_obj3)

    test_obj4 = files_obj(jd_start=np.array([5.101, 4.2, 3.3]))
    assert not test_obj1.isclose(test_obj4)

    test_obj5 = files_obj(jd_start=[5.1, 4.2, 3.3])
    test_obj5.tols = {"jd_start": {"atol": 1e-3, "rtol": 0}}
    assert not test_obj1.isclose(test_obj5)

    test_obj6 = files_obj(jd_start=[5.1, 4.2, 3.3])
    assert test_obj5.isclose(test_obj6)

    test_obj7 = files_obj(jd_start=[5.1, 4.2001, 3.3])
    assert test_obj5.isclose(test_obj7)

    test_obj8 = files_obj(jd_start=[5.1, 4.202, 3.3])
    assert not test_obj5.isclose(test_obj8)


def test_version():
    # this just exercises the version code in `__init__.py`

    assert heratape.__version__ is not None
