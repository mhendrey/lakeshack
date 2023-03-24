"""
Copyright (C) 2022 Matthew Hendrey

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.
You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""
from datetime import datetime, date
import pandas as pd
from pathlib import Path
from pyarrow import fs
import pyarrow.dataset as ds
import pytest
import pytz
from sqlalchemy import select

from .utils import write_parquet_files
from lakeshack.metastore import Metastore


@pytest.fixture(scope="session")
def pq_dir_ts(tmp_path_factory: Path):
    """
    Create parquet files with naive timestamp to be used during the testing session.
    You can then pass this fixture into test functions.

    Parameters
    ----------
    tmp_path_factory : Path
        Session-scoped fixture which can be used to create arbitrary temporary
        directories from any other fixture or test

    Returns
    -------
    parquet_dir : Path
        Path to temporary parquet files for testing
    """
    parquet_dir = tmp_path_factory.mktemp("data_ts")
    write_parquet_files(str(parquet_dir))

    return parquet_dir


@pytest.fixture(scope="session")
def pq_dir_tz(tmp_path_factory: Path):
    """
    Create parquet files with tz aware timestamp to be used during the testing session.
    You can then pass this fixture into test functions.

    Parameters
    ----------
    tmp_path_factory : Path
        Session-scoped fixture which can be used to create arbitrary temporary
        directories from any other fixture or test

    Returns
    -------
    parquet_dir : Path
        Path to temporary parquet files for testing
    """
    parquet_dir = tmp_path_factory.mktemp("data_tz")
    write_parquet_files(str(parquet_dir), timezone="US/Eastern")

    return parquet_dir


@pytest.fixture(scope="session")
def pq_dir_dt(tmp_path_factory: Path):
    """
    Create parquet files with dates to be used during the testing session.
    You can then pass this fixture into test functions.

    Parameters
    ----------
    tmp_path_factory : Path
        Session-scoped fixture which can be used to create arbitrary temporary
        directories from any other fixture or test

    Returns
    -------
    parquet_dir_date : Path
        Path to temporary parquet files for testing
    """
    parquet_dir = tmp_path_factory.mktemp("data_dt")
    write_parquet_files(str(parquet_dir), use_date=True)

    return parquet_dir


@pytest.fixture(scope="session")
def metastore_db_ts(tmp_path_factory: Path, pq_dir_ts):
    """
    Create a metastore based upon the parquet files created for testing naive
    timestamps.

    Parameters
    ----------
    tmp_path_factor : Path
        Session-scoped fixture which can be used to create arbitrary temporary
        directories from any other fixture or test
    pq_dir_ts : Path
        User generated session-scoped fixture containing test parquet files to
        be added into the metastore

    Returns
    -------
    metastore_db_ts : Path
        Path to the metastore database
    """
    data_dir = str(pq_dir_ts)
    dbname = tmp_path_factory.mktemp("db") / "ts.db"

    local_fs = fs.LocalFileSystem()
    dataset = ds.dataset(data_dir, format="parquet", filesystem=local_fs)
    pa_schema = dataset.schema

    metastore = Metastore(f"sqlite:///{dbname}", "test", pa_schema, "id", "timestamp")
    metastore.update(data_dir, local_fs)

    return dbname


@pytest.fixture(scope="session")
def metastore_db_tz(tmp_path_factory: Path, pq_dir_tz):
    """
    Create a metastore based upon the parquet files created for testing tz aware
    timestamps.

    Parameters
    ----------
    tmp_path_factor : Path
        Session-scoped fixture which can be used to create arbitrary temporary
        directories from any other fixture or test
    pq_dir_tz : Path
        User generated session-scoped fixture containing test parquet files to
        be added into the metastore

    Returns
    -------
    metastore_db_tz : Path
        Path to the metastore database
    """
    data_dir = str(pq_dir_tz)
    dbname = tmp_path_factory.mktemp("db") / "tz.db"

    local_fs = fs.LocalFileSystem()
    dataset = ds.dataset(data_dir, format="parquet", filesystem=local_fs)
    pa_schema = dataset.schema

    metastore = Metastore(f"sqlite:///{dbname}", "test", pa_schema, "id", "timestamp")
    metastore.update(data_dir, local_fs)

    return dbname


@pytest.fixture(scope="session")
def metastore_db_dt(tmp_path_factory: Path, pq_dir_dt):
    """
    Create a metastore based upon the parquet files created for testing. This version
    uses date instead of timestamp

    Parameters
    ----------
    tmp_path_factor : Path
        Session-scoped fixture which can be used to create arbitrary temporary
        directories from any other fixture or test
    pq_dir_dt : Path
        User generated session-scoped fixture containing test parquet files to
        be added into the metastore

    Returns
    -------
    metastore_db_dt : Path
        Path to the metastore database
    """
    data_dir = str(pq_dir_dt)
    dbname = tmp_path_factory.mktemp("db") / "dt.db"

    local_fs = fs.LocalFileSystem()
    dataset = ds.dataset(data_dir, format="parquet", filesystem=local_fs)
    pa_schema = dataset.schema

    metastore = Metastore(f"sqlite:///{dbname}", "test", pa_schema, "id", "timestamp")
    metastore.update(data_dir, local_fs)

    return dbname


def update_check(metastore: Metastore, is_datetime: bool):
    """Check that the metastore contains the information that it should

    Parameters
    ----------
    metastore : Metastore
        Metastore to check
    is_datetime : bool
        If True, metastore's timestamp is a datetime. Otherwise it is a date
    """
    with metastore.engine.connect() as conn:
        results = conn.execute(select(metastore.table).order_by("filepath")).fetchall()

        assert len(results) == 10, f"Update failed {len(results)} != 10"

        for idx, (filepath, id_min, id_max, ts_min, ts_max) in enumerate(results):
            if is_datetime:
                ts_min = ts_min.date()
                ts_max = ts_max.date()
            ts_min = ts_min.isoformat()
            ts_max = ts_max.isoformat()
            filename = filepath.split("/")[-1]
            fname = f"part-{idx:02}-{id_min}_{id_max}_{ts_min}_{ts_max}.gzip.parquet"

            assert fname == filename, f"test_update failed {filename} != {fname}"


def test_update_ts(metastore_db_ts, pq_dir_ts):
    """
    Test that naive timestamp metastore gets updated properly

    Parameters
    ----------
    metastore_db_ts : Path
        Session-scoped fixture that is a path (file) to a test metastore
    pq_dir_ts : Path
        Session-scoped fixture that is a path (dir) to the test parquet files
    """
    dbname = str(metastore_db_ts)
    data_dir = str(pq_dir_ts)
    dataset = ds.dataset(data_dir, format="parquet", filesystem=fs.LocalFileSystem())
    pa_schema = dataset.schema

    metastore = Metastore(f"sqlite:///{dbname}", "test", pa_schema)
    update_check(metastore, is_datetime=True)


def test_update_tz(metastore_db_tz, pq_dir_tz):
    """
    Test that naive timestamp metastore gets updated properly

    Parameters
    ----------
    metastore_db_tz : Path
        Session-scoped fixture that is a path (file) to a test metastore
    pq_dir_tz : Path
        Session-scoped fixture that is a path (dir) to the test parquet files
    """
    dbname = str(metastore_db_tz)
    data_dir = str(pq_dir_tz)
    dataset = ds.dataset(data_dir, format="parquet", filesystem=fs.LocalFileSystem())
    pa_schema = dataset.schema

    metastore = Metastore(f"sqlite:///{dbname}", "test", pa_schema)
    update_check(metastore, is_datetime=True)


def test_update_dt(metastore_db_dt, pq_dir_dt):
    """
    Test that metastore using dates gets updated properly

    Parameters
    ----------
    metastore_db_dt : Path
        Session-scopedfixture that is a path (file) to a test metastore
    pq_dir_dt : Path
        Session-scoped fixture that is a path (dir) to the test parquet files
    """
    dbname = str(metastore_db_dt)
    data_dir = str(pq_dir_dt)
    dataset = ds.dataset(data_dir, format="parquet", filesystem=fs.LocalFileSystem())
    pa_schema = dataset.schema

    metastore = Metastore(f"sqlite:///{dbname}", "test", pa_schema)
    update_check(metastore, is_datetime=False)


def query_check(metastore: Metastore, timestamp_type: str):
    """Query the metastore and check that expected results are found

    Parameters
    ----------
    metastore : Metastore
        Metastore to check
    timestamp_type : str
        Use 'ts' | 'tz' | 'dt' to specify whether the timestamp column in parquet
        files are datetime (naive), datetime (tz aware) , or date
    """
    queries = ["01", "22", "35", "4f", "70", "8a", "a0", "bf", "d1", "f0"]

    # Query for an id found in each of the 10 parquet files
    results = metastore.query(queries)
    assert len(results) == 10, f"results has {len(results)} != 10"
    for filepath, ids in results.items():
        filename = filepath.split("/")[-1]
        id_range = filename.split("_")[:2]
        min_id = id_range[0].split("-")[2]
        max_id = id_range[1]
        for id in ids:
            assert id >= min_id and id <= max_id, f"{id} outside [{min_id},{max_id}]"

    # Try optional where clauses using timestamp column
    # For the first parquet file, which holds "00", the naive timestamp holds
    # min(timestamp) = "2021-07-15 12:13:14"
    # max(timestamp) = "2021-08-15 12:13:14"
    if timestamp_type == "ts":
        min_ts_pq_0 = datetime.fromisoformat("2021-07-15T12:13:14")
        med_ts = datetime.fromisoformat("2021-08-01T12:34:56.123")
        max_ts_pq_0 = datetime.fromisoformat("2021-08-15T12:13:14")
    elif timestamp_type == "tz":
        tz = pytz.timezone("US/Eastern")
        min_ts_pq_0 = tz.localize(datetime.fromisoformat("2021-07-15T12:13:14"))
        med_ts = tz.localize(datetime.fromisoformat("2021-08-01T12:34:56.123"))
        max_ts_pq_0 = tz.localize(datetime.fromisoformat("2021-08-15T12:13:14"))
    elif timestamp_type == "dt":
        min_ts_pq_0 = date.fromisoformat("2021-07-15")
        med_ts = date.fromisoformat("2021-08-01")
        max_ts_pq_0 = date.fromisoformat("2021-08-15")
    else:
        raise ValueError(f"{timestamp_type=:} does not match 'ts' | 'tz' | 'dt'")

    results = metastore.query(queries, [("timestamp", "<", min_ts_pq_0)])
    assert len(results) == 0, f"test_query: Returned {len(results)} but should be 0"

    results = metastore.query(queries, [("timestamp", "<=", min_ts_pq_0)])
    assert len(results) == 1, f"test_query: Returned {len(results)} but should be 1"

    results = metastore.query(["00"], [("timestamp", ">", max_ts_pq_0)])
    assert len(results) == 0, f"test_query: Returned {len(results)} but should be 0"

    results = metastore.query(["00"], [("timestamp", ">=", max_ts_pq_0)])
    assert len(results) == 1, f"test_query: Returned {len(results)} but should be 1"

    results = metastore.query(["00"], [("timestamp", ">", min_ts_pq_0)])
    assert len(results) == 1, f"test_query: Returned {len(results)} but should be 1"

    results = metastore.query(["00"], [("timestamp", ">=", min_ts_pq_0)])
    assert len(results) == 1, f"test_query: Returned {len(results)} but should be 1"

    results = metastore.query(queries, [("timestamp", "<", max_ts_pq_0)])
    assert len(results) == 1, f"test_query: Returned {len(results)} but should be 1"

    results = metastore.query(queries, [("timestamp", "<=", max_ts_pq_0)])
    assert len(results) == 1, f"test_query: Returned {len(results)} but should be 1"

    results = metastore.query(queries, [("timestamp", ">=", med_ts)])
    assert len(results) == 10, f"test_query: Returned {len(results)} but should be 10"

    results = metastore.query(queries, [("timestamp", "<=", med_ts)])
    assert len(results) == 1, f"test_query: Returned {len(results)} but should be 1"


def test_query_ts(metastore_db_ts, pq_dir_ts):
    """
    Test querying the metastore

    Parameters
    ----------
    metastore_db_ts : Path
        Session-scoped fixture that is a path (file) to a test metastore
    pq_dir_ts : Path
        Session-scoped fixture that is a path (dir) to the test parquet files
    """
    dbname = str(metastore_db_ts)
    data_dir = str(pq_dir_ts)
    dataset = ds.dataset(data_dir, format="parquet", filesystem=fs.LocalFileSystem())
    pa_schema = dataset.schema

    metastore = Metastore(f"sqlite:///{dbname}", "test", pa_schema)
    query_check(metastore, "ts")


def test_query_tz(metastore_db_tz, pq_dir_tz):
    """
    Test querying the metastore

    Parameters
    ----------
    metastore_db_tz : Path
        Session-scoped fixture that is a path (file) to a test metastore
    pq_dir_tz : Path
        Session-scoped fixture that is a path (dir) to the test parquet files
    """
    dbname = str(metastore_db_tz)
    data_dir = str(pq_dir_tz)
    dataset = ds.dataset(data_dir, format="parquet", filesystem=fs.LocalFileSystem())
    pa_schema = dataset.schema

    metastore = Metastore(f"sqlite:///{dbname}", "test", pa_schema)
    query_check(metastore, "tz")


def test_query_dt(metastore_db_dt, pq_dir_dt):
    """
    Test querying the metastore

    Parameters
    ----------
    metastore_db_dt : Path
        Session-scopedfixture that is a path (file) to a test metastore
    pq_dir_dt : Path
        Session-scoped fixture that is a path (dir) to the test parquet files
    """
    dbname = str(metastore_db_dt)
    data_dir = str(pq_dir_dt)
    dataset = ds.dataset(data_dir, format="parquet", filesystem=fs.LocalFileSystem())
    pa_schema = dataset.schema

    metastore = Metastore(f"sqlite:///{dbname}", "test", pa_schema)
    query_check(metastore, "dt")
