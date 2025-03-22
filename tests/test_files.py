# Copyright (c) 2025 HERA-Team
# Licensed under the 2-clause BSD License
"""Test tapes."""

import datetime
from math import floor
from pathlib import Path

import numpy as np
from astropy.time import Time, TimeDelta

from heratape import Files
from heratape.files import add_files_to_tape, get_all_jds
from heratape.tapes import add_tape


def test_add_files_to_tape(test_session):
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
    write_date = datetime.datetime(2025, 3, 15, 10, 20, 6)

    add_files_to_tape(
        tape_id=tape_id,
        write_date=write_date,
        filepath_list=filepaths,
        obsid_list=obids,
        jd_start_list=jd_starts,
        size_list=sizes,
        session=test_session,
    )

    expected_list = [
        Files(
            filebase=fbase,
            filepath=fpath,
            tape_id=tape_id,
            obsid=obsid,
            jd_start=jd_start,
            jd=jd_int,
            size=size,
            write_date=write_date,
        )
        for fbase, fpath, obsid, jd_start, jd_int, size in zip(
            filebases, filepaths, obids, jd_starts, int_jds, sizes, strict=True
        )
    ]

    file_records = test_session.query(Files).all()
    assert len(file_records) == n_files

    for f_ind, f_rec in enumerate(file_records):
        assert f_rec.isclose(expected_list[f_ind])

    db_jds = get_all_jds(session=test_session)
    print(db_jds)
    assert len(db_jds) == 1
    assert db_jds[0] == int_jds[0]
