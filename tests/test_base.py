# Copyright (c) 2025 HERA-Team
# Licensed under the 2-clause BSD License
"""Test base."""

import datetime

import numpy as np
from sqlalchemy import Boolean, Column, Date, DateTime, Float, Integer, String

import heratape
from heratape.base import Base


class TableTest(Base):
    __tablename__ = "table_test"
    intcol = Column(Integer, primary_key=True)
    boolcol = Column(Boolean)
    strcol = Column(String)
    floatcol = Column(Float)
    datecol = Column(Date)
    datetimecol = Column(DateTime)


def table_test_obj(
    intval=3,
    boolval=False,
    strval="foo",
    floatval=3.14,
    dateval=datetime.datetime(2025, 3, 10),
    datetimeval=datetime.datetime(2025, 3, 10, 12, 5, 10),
):
    return TableTest(
        intcol=intval,
        boolcol=boolval,
        strcol=strval,
        floatcol=floatval,
        datecol=dateval,
        datetimecol=datetimeval,
    )


def test_base_repr():
    expected_repr = (
        "<TableTest(3, False, foo, 3.14, 2025-03-10 00:00:00, 2025-03-10 12:05:10)>"
    )
    assert str(table_test_obj()) == expected_repr


def test_isclose_class():
    class NewTest(Base):
        __tablename__ = "new_test"
        intcol = Column(Integer, primary_key=True)

    new_obj = NewTest(intcol=3)

    assert not table_test_obj().isclose(new_obj)


def test_isclose_int():
    test_obj1 = table_test_obj(intval=3)
    test_obj2 = table_test_obj(intval=3)
    assert test_obj1.isclose(test_obj2)

    test_obj3 = table_test_obj(intval=5)
    assert not test_obj1.isclose(test_obj3)

    test_obj4 = table_test_obj(intval=3.0)
    assert not test_obj1.isclose(test_obj4)

    test_obj5 = table_test_obj(intval=None)
    assert not test_obj1.isclose(test_obj5)

    test_obj6 = table_test_obj(intval=None)
    assert test_obj5.isclose(test_obj6)


def test_isclose_bool():
    test_obj1 = table_test_obj(boolval=True)
    test_obj2 = table_test_obj(boolval=True)
    assert test_obj1.isclose(test_obj2)

    test_obj3 = table_test_obj(boolval=False)
    assert not test_obj1.isclose(test_obj3)

    test_obj4 = table_test_obj(boolval=1)
    assert not test_obj1.isclose(test_obj4)


def test_isclose_str():
    test_obj1 = table_test_obj(strval="foo")
    test_obj2 = table_test_obj(strval="foo")
    assert test_obj1.isclose(test_obj2)

    test_obj3 = table_test_obj(strval="bar")
    assert not test_obj1.isclose(test_obj3)

    test_obj4 = table_test_obj(strval=5.0)
    assert not test_obj1.isclose(test_obj4)


def test_isclose_date():
    test_obj1 = table_test_obj(dateval=datetime.date(2025, 3, 10))
    test_obj2 = table_test_obj(dateval=datetime.date(2025, 3, 10))
    assert test_obj1.isclose(test_obj2)

    test_obj3 = table_test_obj(dateval=datetime.date(2025, 3, 11))
    assert not test_obj1.isclose(test_obj3)

    test_obj4 = table_test_obj(dateval=datetime.datetime(2025, 3, 10, 5, 3, 8))
    assert not test_obj1.isclose(test_obj4)

    test_obj5 = table_test_obj(dateval=None)
    assert not test_obj1.isclose(test_obj5)


def test_isclose_datetime():
    test_obj1 = table_test_obj(dateval=datetime.datetime(2025, 3, 10))
    test_obj2 = table_test_obj(dateval=datetime.datetime(2025, 3, 10, 0, 0, 0))
    assert test_obj1.isclose(test_obj2)

    test_obj3 = table_test_obj(dateval=datetime.date(2025, 3, 11))
    assert not test_obj1.isclose(test_obj3)

    test_obj4 = table_test_obj(dateval=datetime.datetime(2025, 3, 10, 5, 3, 8))
    assert not test_obj1.isclose(test_obj4)

    test_obj5 = table_test_obj(dateval=None)
    assert not test_obj1.isclose(test_obj5)


def test_isclose_float():
    test_obj1 = table_test_obj(floatval=3.1415)
    test_obj1.tols = {"floatcol": {"atol": 1e-3, "rtol": 0}}

    test_obj2 = table_test_obj(floatval=3.1415)
    assert test_obj1.isclose(test_obj2)

    test_obj3 = table_test_obj(floatval=3.1414)
    assert test_obj1.isclose(test_obj3)

    test_obj4 = table_test_obj(floatval=3.1426)
    assert not test_obj1.isclose(test_obj4)

    test_obj5 = table_test_obj(floatval=3.1415 + 9e-9)
    assert test_obj2.isclose(test_obj5)

    test_obj6 = table_test_obj(floatval=(3.1415 * (1 + 1e-5)) + 1.1e-8)
    assert not test_obj2.isclose(test_obj6)


def test_isclose_float_array():
    test_obj1 = table_test_obj(floatval=np.array([5.1, 4.2, 3.3]))
    test_obj1.tols = {"floatcol": {"atol": 1e-3, "rtol": 0}}

    test_obj2 = table_test_obj(floatval=np.array([5.1, 4.2, 3.3]))
    assert test_obj1.isclose(test_obj2)

    test_obj3 = table_test_obj(floatval=np.array([5.1001, 4.2, 3.3]))
    assert test_obj1.isclose(test_obj3)

    test_obj4 = table_test_obj(floatval=np.array([5.101, 4.2, 3.3]))
    assert not test_obj1.isclose(test_obj4)

    test_obj5 = table_test_obj(floatval=[5.1, 4.2, 3.3])
    test_obj5.tols = {"floatcol": {"atol": 1e-3, "rtol": 0}}
    assert not test_obj1.isclose(test_obj5)

    test_obj6 = table_test_obj(floatval=[5.1, 4.2, 3.3])
    assert test_obj5.isclose(test_obj6)

    test_obj7 = table_test_obj(floatval=[5.1, 4.2001, 3.3])
    assert test_obj5.isclose(test_obj7)

    test_obj8 = table_test_obj(floatval=[5.1, 4.202, 3.3])
    assert not test_obj5.isclose(test_obj8)


def test_version():
    # this just exercises the version code in `__init__.py`

    assert heratape.__version__ is not None
