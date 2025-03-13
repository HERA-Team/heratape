# Copyright (c) 2025 HERA-Team
# Licensed under the 2-clause BSD License
"""Define the database base objects."""

import json
import os
from abc import ABCMeta
from datetime import date

import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm.session import sessionmaker

config_file = os.path.expanduser("~/.heratape/heratape_config.json")


class Base:
    """Base table object."""

    def __repr__(self):
        """Define standard representation."""
        columns = self.__table__.columns.keys()
        rep_str = "<" + self.__class__.__name__ + "("
        for c in columns:
            rep_str += str(getattr(self, c)) + ", "
        rep_str = rep_str[0:-2]
        rep_str += ")>"
        return rep_str

    def isclose(self, other):
        """Test if two objects are nearly equal."""
        if not isinstance(other, self.__class__):
            print("not the same class")
            return False

        self_columns = self.__table__.columns
        other_columns = other.__table__.columns
        # the following is structured as an assert because I cannot make it fail but
        # think it should be checked.
        assert {col.name for col in self_columns} == {
            col.name for col in other_columns
        }, (
            "Set of columns are not the same. This should not happen, please make an "
            "issue in our repo."
        )
        for col in self_columns:
            self_col = getattr(self, col.name)
            other_col = getattr(other, col.name)
            if not isinstance(other_col, type(self_col)):
                print(
                    f"column {col} has different types, left is {type(self_col)}, "
                    f"right is {type(other_col)}."
                )
                return False
            if isinstance(self_col, bool):
                # have to check bool before int because bool is a subclass of int
                if self_col != other_col:
                    print(f"column {col} is a boolean, values are not equal")
                    return False
            elif isinstance(self_col, int):
                if self_col != other_col:
                    print(f"column {col} is an int, values are not equal")
                    return False
            elif isinstance(self_col, str):
                if self_col != other_col:
                    print(f"column {col} is a str, values are not equal")
                    return False
            elif isinstance(self_col, date):
                if self_col != other_col:
                    print(f"column {col} is a datetime, values are not equal")
                    return False
            elif self_col is None:
                # nullable columns, both null (otherwise caught as different types)
                pass
            else:
                if hasattr(self, "tols") and col.name in self.tols:
                    atol = self.tols[col.name]["atol"]
                    rtol = self.tols[col.name]["rtol"]
                else:
                    # use numpy defaults
                    atol = 1e-08
                    rtol = 1e-05
                if isinstance(self_col, np.ndarray | list):
                    if not np.allclose(self_col, other_col, atol=atol, rtol=rtol):
                        print(
                            f"column {col} is a float-like array, values are not equal"
                        )
                        return False
                else:
                    if not np.isclose(self_col, other_col, atol=atol, rtol=rtol):
                        print(f"column {col} is float-like, values are not equal")
                        return False
        return True


Base = declarative_base(cls=Base)


# revist whether this should be an ABC b/c of B024 linter error. Ignoring for now
class DB(metaclass=ABCMeta):  # noqa B024
    """
    Abstract base class for heratape database object.

    This ABC is only instantiated through the AutomappedDB or DeclarativeDB
    subclasses.

    """

    engine = None
    sessionmaker = sessionmaker()
    sqlalchemy_base = None

    def __init__(self, db_url):  # noqa
        self.sqlalchemy_base = Base
        self.engine = create_engine(db_url)
        self.sessionmaker.configure(bind=self.engine)


class DeclarativeDB(DB):
    """Declarative database object -- to create database tables."""

    def __init__(self, db_url):
        super().__init__(db_url)

    def create_tables(self):
        """Create all tables."""
        self.sqlalchemy_base.metadata.create_all(self.engine)

    def drop_tables(self):
        """Drop all tables."""
        self.sqlalchemy_base.metadata.bind = self.engine
        self.sqlalchemy_base.metadata.drop_all(self.engine)


class AutomappedDB(DB):
    """Automapped database object -- attaches to an existing database.

    This is intended for use with the production database. __init__()
    raises an exception if the existing database does not match the schema
    defined in the SQLAlchemy initialization magic.

    """

    def __init__(self, db_url):
        super().__init__(automap_base(), db_url)

        from .db_check import is_valid_database

        with self.sessionmaker() as session:
            db_valid, valid_msg = is_valid_database(Base, session)
            if not db_valid:  # pragma: no cover
                raise RuntimeError(
                    f"Database {db_url} does not match expected schema. " + valid_msg
                )


def get_heratape_db(
    config_file: str = config_file,
    forced_db_name: str | None = None,
    check_connect: bool = True,
):
    """
    Get a DB object that is connected to the heratape database.

    Parameters
    ----------
    config_file : str
        Path to the heratape_config.json configuration file.
    forced_db_name : str, optional
        Database name to use (forced). If not set, uses the default one from
        args.
    check_connect : bool
        Option to test the database connection.

    Returns
    -------
    DB object
        An instance of the `DB` class providing access to the heratape database.

    """
    if forced_db_name is not None:
        db_name = forced_db_name

    with open(config_file) as f:
        config_data = json.load(f)

    if db_name is None:
        db_name = config_data.get("default_db_name")
        if db_name is None:
            raise RuntimeError(
                "cannot connect to heratape database: no DB name "
                f"provided, and no default listed in {config_file!r}"
            )

    db_data = config_data.get("databases")
    if db_data is None:
        raise RuntimeError(
            'cannot connect to heratape database: no "databases" '
            f"section in {config_file!r}"
        )

    db_data = db_data.get(db_name)
    if db_data is None:
        raise RuntimeError(
            f"cannot connect to heratape database: no DB named {db_name!r} "
            f'in the "databases" section of {config_file!r}'
        )

    db_url = db_data.get("url")
    if db_url is None:
        raise RuntimeError(
            'cannot connect to heratape database: no "url" item for '
            f"the DB named {db_name!r} in {config_file!r}"
        )

    db_mode = db_data.get("mode")
    if db_mode is None:
        raise RuntimeError(
            'cannot connect to heratape database: no "mode" item for '
            f"the DB named {db_name!r} in {config_file!r}"
        )

    if db_mode == "testing":
        db = DeclarativeDB(db_url)
    elif db_mode == "production":
        db = AutomappedDB(db_url)
    else:
        raise RuntimeError(
            "cannot connect to heratape database: unrecognized mode "
            f"{db_mode!r} for the DB named {db_name!r} in {config_file!r}"
        )

    if check_connect:
        # Test database connection
        with db.sessionmaker() as session:
            from . import db_check

            if not db_check.check_connection(session):
                raise RuntimeError("Could not establish valid connection to database.")

    return db


def get_heratape_testing_db(forced_db_name="testing"):
    """
    Get a DB object that is connected to the testing heratape database.

    Parameters
    ----------
    forced_db_name : str
        Database name to use.

    Returns
    -------
    DB object
        An instance of the `DB` class providing access to the testing heratape
        database.

    """
    return get_heratape_db(forced_db_name=forced_db_name)
