# Copyright (c) 2025 HERA-Team
# Licensed under the 2-clause BSD License
"""Test tapes."""

import copy
import datetime
from math import floor
from pathlib import Path

import numpy as np
import pytest
from astropy.time import Time, TimeDelta

from heratape import Files
from heratape.files import add_files_to_tape, get_all_jds, set_write_date, update_file
from heratape.tapes import add_tape


@pytest.fixture(scope="function")
def file_dict(tape_dict):
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

    return {
        "filebases": filebases,
        "tape_id": tape_dict["tape_id"],
        "write_date": datetime.datetime(2025, 3, 15, 10, 20, 6),
        "filepath_list": filepaths,
        "obsid_list": obids,
        "jd_start_list": jd_starts,
        "int_jds": int_jds,
        "size_list": sizes,
    }


@pytest.mark.parametrize(
    "write_date",
    [
        datetime.datetime(2025, 3, 15, 10, 20, 6),
        Time("2025-03-15T10:20:06", scale="utc"),
    ],
)
@pytest.mark.parametrize(
    ("set_date", "set_full_paths"), [(True, True), (True, False), (False, False)]
)
def test_add_files_to_tape(
    test_session, tape_dict, file_dict, write_date, set_date, set_full_paths
):
    add_tape(session=test_session, **tape_dict)

    file_dict_use = copy.deepcopy(file_dict)
    file_dict_use.pop("filebases")
    file_dict_use.pop("int_jds")
    if set_date:
        file_dict_use["write_date"] = None
    else:
        file_dict_use["write_date"] = write_date

    add_files_to_tape(session=test_session, **file_dict_use)

    if set_date:
        file_records = test_session.query(Files).all()
        test_session.commit()
        if set_full_paths:
            file_list = file_dict["filepath_list"]
        else:
            file_list = file_dict["filebases"]
        set_write_date(file_list=file_list, write_date=write_date, session=test_session)

    if isinstance(write_date, Time):
        exp_write_date = write_date.tt.datetime
    else:
        exp_write_date = write_date
    expected_list = [
        Files(
            filebase=fbase,
            filepath=fpath,
            tape_id=tape_dict["tape_id"],
            obsid=obsid,
            jd_start=jd_start,
            jd=jd_int,
            size=size,
            write_date=exp_write_date,
        )
        for fbase, fpath, obsid, jd_start, jd_int, size in zip(
            file_dict["filebases"],
            file_dict["filepath_list"],
            file_dict["obsid_list"],
            file_dict["jd_start_list"],
            file_dict["int_jds"],
            file_dict["size_list"],
            strict=True,
        )
    ]

    file_records = test_session.query(Files).order_by(Files.filebase).all()
    assert len(file_records) == len(file_dict["filebases"])

    for f_ind, f_rec in enumerate(file_records):
        assert f_rec.isclose(expected_list[f_ind])

    db_jds = get_all_jds(session=test_session)
    assert len(db_jds) == 1
    assert db_jds[0] == file_dict["int_jds"][0]


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
def test_add_files_to_tape_errors(
    test_session, tape_dict, file_dict, param, value, err_msg
):
    add_tape(session=test_session, **tape_dict)

    file_dict_use = copy.deepcopy(file_dict)
    file_dict_use.pop("filebases")
    file_dict_use.pop("int_jds")
    file_dict_use["write_date"] = datetime.datetime(2025, 3, 15, 10, 20, 6)

    file_dict_use[param] = value

    with pytest.raises(ValueError, match=err_msg):
        add_files_to_tape(session=test_session, **file_dict_use)


def test_set_write_date_errors():
    with pytest.raises(
        ValueError, match="write_date must be a datetime or astropy Time object"
    ):
        set_write_date(file_list=["foo"], write_date="2021-12-14T00:00:00")


@pytest.mark.parametrize(
    "update_dict",
    [
        {"filename": "/mnt/data1/2459562/zen_2459562.4583333335_sum.uvh5"},
        {"tape_id": "HERA_02"},
        {"obsid": 1323471619},
        {"jd_start": 2459563.4583333335},
        {"size": int(5e9)},
        {"write_date": datetime.datetime(2025, 3, 16, 10, 20, 6)},
        {"write_date": Time("2025-03-16T10:20:06", scale="utc")},
        {},
    ],
)
def test_update_file(test_session, tape_dict, file_dict, update_dict):
    add_tape(session=test_session, **tape_dict)
    if "tape_id" in update_dict:
        new_tape_dict = copy.deepcopy(tape_dict)
        new_tape_dict["tape_id"] = update_dict["tape_id"]
        add_tape(session=test_session, **new_tape_dict)

    file_dict_use = copy.deepcopy(file_dict)
    file_dict_use.pop("filebases")
    file_dict_use.pop("int_jds")
    file_dict_use["write_date"] = datetime.datetime(2025, 3, 15, 10, 20, 6)

    add_files_to_tape(session=test_session, **file_dict_use)

    filebase = file_dict["filebases"][0]

    exp_dict = {
        "filebase": filebase,
        "filepath": file_dict["filepath_list"][0],
        "tape_id": tape_dict["tape_id"],
        "obsid": file_dict["obsid_list"][0],
        "jd_start": file_dict["jd_start_list"][0],
        "jd": file_dict["int_jds"][0],
        "size": file_dict["size_list"][0],
        "write_date": file_dict["write_date"],
    }
    for key, value in update_dict.items():
        if key == "filename":
            exp_dict["filepath"] = value
        elif key == "jd_start":
            exp_dict[key] = value
            exp_dict["jd"] = int(floor(value))
        else:
            exp_dict[key] = value

    if "filename" not in update_dict:
        update_dict["filename"] = filebase
    update_file(session=test_session, **update_dict)

    file_records = test_session.query(Files).where(Files.filebase == filebase).all()
    assert len(file_records) == 1

    if isinstance(exp_dict["write_date"], Time):
        exp_dict["write_date"] = exp_dict["write_date"].tt.datetime
    exp_obj = Files(**exp_dict)

    assert file_records[0].isclose(exp_obj)


@pytest.mark.parametrize(
    ("update_dict", "err_msg"),
    [
        (
            {"write_date": "2025-01-15"},
            "If set, write_date must be a datetime or astropy Time object",
        ),
        (
            {"tape_id": "HERA_02"},
            "tape HERA_02 is not yet in the tapes table. Use the add_tape "
            "function to add it before adding files to it.",
        ),
    ],
)
def test_update_file_errors(test_session, tape_dict, update_dict, err_msg):
    add_tape(session=test_session, **tape_dict)

    with pytest.raises(ValueError, match=err_msg):
        update_file(filename="foo", session=test_session, **update_dict)
