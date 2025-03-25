# Copyright (c) 2025 HERA-Team
# Licensed under the 2-clause BSD License

"""Testing environment setup and teardown for pytest."""

import datetime
import urllib
import warnings

import pytest
from astropy.time import Time
from astropy.utils import iers
from sqlalchemy.orm import Session

from heratape.base import get_heratape_testing_db


@pytest.fixture(autouse=True, scope="session")
def setup_and_teardown_package():
    # Do a calculation that requires a current IERS table. This will trigger
    # automatic downloading of the IERS table if needed, including trying the
    # mirror site in python 3 (but won't redownload if a current one exists).
    # If there's not a current IERS table and it can't be downloaded, turn off
    # auto downloading for the tests and turn it back on once all tests are
    # completed (done by extending auto_max_age).
    # Also, the checkWarnings function will ignore IERS-related warnings.
    try:
        t1 = Time.now()
        _ = t1.ut1
    except (urllib.error.URLError, OSError, iers.IERSRangeError):
        iers.conf.auto_max_age = None

    test_db = get_heratape_testing_db()
    test_db.create_tables()

    yield test_db

    iers.conf.auto_max_age = 30
    test_db.drop_tables()


@pytest.fixture(scope="function")
def test_engine(setup_and_teardown_package):
    test_db = setup_and_teardown_package
    return test_db.engine


@pytest.fixture(scope="function")
def test_session(setup_and_teardown_package):
    test_db = setup_and_teardown_package
    with test_db.engine.connect() as test_conn, test_conn.begin() as test_trans:
        session = Session(bind=test_conn)
        with session as test_session:
            yield test_session

        # rollback - everything that happened with the
        # Session above (including calls to commit())
        # is rolled back.
        with warnings.catch_warnings():
            # If an error was raised, rollback may have already been called. If so, this
            # will give a warning which we filter out here.
            warnings.filterwarnings(
                "ignore", "transaction already deassociated from connection"
            )
            test_trans.rollback()


@pytest.fixture(scope="function")
def tape_dict():
    return {
        "tape_id": "HERA_01",
        "tape_type": "foo",
        "size": int(8e12),
        "purchase_date": datetime.date(2025, 1, 15),
    }
