# Copyright (c) 2025 HERA-Team
# Licensed under the 2-clause BSD License
"""Test tapes."""

import datetime
from math import floor
from pathlib import Path

import numpy as np
import pytest
from astropy.time import Time, TimeDelta

from heratape import Files
from heratape.files import add_files_to_tape, get_all_jds, set_write_date
from heratape.tapes import add_tape


@pytest.mark.parametrize(
    "write_date",
    [
        datetime.datetime(2025, 3, 15, 10, 20, 6),
        Time("2025-03-15T10:20:06", scale="utc"),
    ],
)
@pytest.mark.parametrize("set_date", [True, False])
def test_add_files_to_tape(test_session, write_date, set_date):
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

    n_files = 10
    time_starts = Time("2021-12-14T00:00:00", scale="utc") + TimeDelta(
        np.linspace(-3600, 3600, n_files), format="sec"
    )
    jd_starts = time_starts.jd
    obids = [int(floor(tval.gps)) for tval in time_starts]
    int_jds = [int(floor(jd)) for jd in jd_starts]
    assert np.all(np.asarray(int_jds) == int_jds[0])

    filepaths = [
        str(Path(f"/home/data/{int_jds[0]}/zen_{jd}_sum.uvh5")) for jd in jd_starts
    ]
    filebases = [f"zen_{jd}_sum.uvh5" for jd in jd_starts]
    sizes = [int(2e9)] * n_files

    if set_date:
        write_date_use = None
    else:
        write_date_use = write_date

    add_files_to_tape(
        tape_id=tape_id,
        write_date=write_date_use,
        filepath_list=filepaths,
        obsid_list=obids,
        jd_start_list=jd_starts,
        size_list=sizes,
        session=test_session,
    )

    if set_date:
        file_records = test_session.query(Files).all()
        test_session.commit()
        set_write_date(
            filebase_list=filebases, write_date=write_date, session=test_session
        )

    if isinstance(write_date, Time):
        exp_write_date = write_date.tt.datetime
    else:
        exp_write_date = write_date
    expected_list = [
        Files(
            filebase=fbase,
            filepath=fpath,
            tape_id=tape_id,
            obsid=obsid,
            jd_start=jd_start,
            jd=jd_int,
            size=size,
            write_date=exp_write_date,
        )
        for fbase, fpath, obsid, jd_start, jd_int, size in zip(
            filebases, filepaths, obids, jd_starts, int_jds, sizes, strict=True
        )
    ]

    file_records = test_session.query(Files).order_by(Files.filebase).all()
    assert len(file_records) == n_files

    for f_ind, f_rec in enumerate(file_records):
        assert f_rec.isclose(expected_list[f_ind])

    db_jds = get_all_jds(session=test_session)
    assert len(db_jds) == 1
    assert db_jds[0] == int_jds[0]


@pytest.mark.parametrize(
    ("param", "value", "err_msg"),
    [
        (
            "tape_id",
            "foo",
            "tape foo is not yet in the tapes table. Use the add_tape "
            "function to add it before adding files to it.",
        ),
        (
            "write_date",
            "2021-12-14T00:00:00",
            "If set, write_date must be a datetime or astropy Time object",
        ),
        (
            "obsid_list",
            [1323471618, 1323472418],
            "obsid_list is length 2, filepath_list is length 10. They must be "
            "the same length.",
        ),
        (
            "jd_start_list",
            [2459562.45833333, 2459562.46759259],
            "jd_start_list is length 2, filepath_list is length 10. They must be "
            "the same length.",
        ),
        (
            "size_list",
            [int(2e9)] * 2,
            "size_list is length 2, filepath_list is length 10. They must be "
            "the same length.",
        ),
    ],
)
def test_add_files_to_tape_errors(test_session, param, value, err_msg):
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

    n_files = 10
    time_starts = Time("2021-12-14T00:00:00", scale="utc") + TimeDelta(
        np.linspace(-3600, 3600, n_files), format="sec"
    )
    jd_starts = time_starts.jd
    obids = [int(floor(tval.gps)) for tval in time_starts]
    int_jds = [int(floor(jd)) for jd in jd_starts]
    assert np.all(np.asarray(int_jds) == int_jds[0])

    filepaths = [
        str(Path(f"/home/data/{int_jds[0]}/zen_{jd}_sum.uvh5")) for jd in jd_starts
    ]
    sizes = [int(2e9)] * n_files
    write_date = datetime.datetime(2025, 3, 15, 10, 20, 6)

    kwargs = {
        "tape_id": tape_id,
        "write_date": write_date,
        "filepath_list": filepaths,
        "obsid_list": obids,
        "jd_start_list": jd_starts,
        "size_list": sizes,
        "session": test_session,
    }
    kwargs[param] = value

    with pytest.raises(ValueError, match=err_msg):
        add_files_to_tape(**kwargs)


def test_set_write_date_errors():
    with pytest.raises(
        ValueError, match="write_date must be a datetime or astropy Time object"
    ):
        set_write_date(filebase_list=["foo"], write_date="2021-12-14T00:00:00")
