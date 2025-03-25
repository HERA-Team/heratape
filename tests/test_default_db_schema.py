# Copyright 2016 the HERA Collaboration
# Licensed under the 2-clause BSD license.

"""
Test that default database matches code schema.
"""

import pytest

from heratape.base import Base, HTSessionWrapper
from heratape.db_check import is_valid_database

# Sometimes a connection is closed, which is handled and doesn't produce an error
# or even a warning under normal testing. But for the warnings test where we
# pass `-W error`, the warning causes an error so we filter it out here.
pytestmark = pytest.mark.filterwarnings("ignore:connection:ResourceWarning:psycopg")


def test_default_db_schema():
    # this test will fail if the default database schema does not match the code schema

    with HTSessionWrapper() as ht_sess:
        valid, _ = is_valid_database(Base, ht_sess)
        assert valid
