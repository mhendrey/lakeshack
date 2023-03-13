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
from sqlalchemy import select

from .utils import write_parquet_files
from wherehouse.metastore import Metastore


@pytest.fixture(scope="session")
def pq_dir(tmp_path_factory: Path):
    """
    Create parquet files to be used during the testing session.
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
    parquet_dir = tmp_path_factory.mktemp("data")
    write_parquet_files(str(parquet_dir))

    return parquet_dir


@pytest.fixture(scope="session")
def metastore_db(tmp_path_factory: Path, pq_dir):
    """
    Create a metastore based upon the parquet files created for testing

    Parameters
    ----------
    tmp_path_factor : Path
        Session-scoped fixture which can be used to create arbitrary temporary
        directories from any other fixture or test
    pq_dir : Path
        User generated session-scoped fixture containing test parquet files to
        be added into the metastore

    Returns
    -------
    metastore_db : Path
        Path to the metastore database
    """
    data_dir = str(pq_dir)

    dbname = tmp_path_factory.mktemp("db") / "test.db"

    local_fs = fs.LocalFileSystem()
    dataset = ds.dataset(data_dir, format="parquet", filesystem=local_fs)
    pa_schema = dataset.schema

    metastore = Metastore(f"sqlite:///{dbname}", "test", pa_schema, "id", "timestamp")
    metastore.update(data_dir, local_fs)

    return dbname


def test_update(metastore_db, pq_dir):
    """
    Test that metastore gets updated properly

    Parameters
    ----------
    metastore_db : Path
        Session-scoped fixture that is a path (file) to a test metastore
    pq_dir : Path
        Session-scoped fixture that is a path (dir) to the test parquet files
    """
    dbname = str(metastore_db)
    data_dir = str(pq_dir)
    dataset = ds.dataset(data_dir, format="parquet", filesystem=fs.LocalFileSystem())
    pa_schema = dataset.schema

    metastore = Metastore(f"sqlite:///{dbname}", "test", pa_schema)

    with metastore.engine.connect() as conn:
        results = conn.execute(select(metastore.table).order_by("filepath")).fetchall()
        assert len(results) == 10, f"test_update failed {len(results)} != 10"
        for idx, (filepath, id_min, id_max, ts_min, ts_max) in enumerate(results):
            ts_min = ts_min.date().isoformat()
            ts_max = ts_max.date().isoformat()
            filename = filepath.split("/")[-1]
            fname = f"part-{idx:02}-{id_min}_{id_max}_{ts_min}_{ts_max}.gzip.parquet"
            assert fname == filename, f"test_update failed {filename} != {fname}"


def test_query(metastore_db, pq_dir):
    """
    Test querying the metastore

    Parameters
    ----------
    metastore_db : Path
        Session-scoped fixture that is a path (file) to a test metastore
    pq_dir : Path
        Session-scoped fixture that is a path (dir) to the test parquet files
    """
    dbname = str(metastore_db)
    data_dir = str(pq_dir)
    dataset = ds.dataset(data_dir, format="parquet", filesystem=fs.LocalFileSystem())
    pa_schema = dataset.schema

    metastore = Metastore(f"sqlite:///{dbname}", "test", pa_schema)

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
    # For the first parquet file, which holds "00"
    # timestamp_min = "2021-07-15 12:13:14"
    # timestamp_max = "2021-08-15 12:13:14"
    min_ts_pq_0 = datetime.fromisoformat("2021-07-15 12:13:14")
    med_ts = datetime.fromisoformat("2021-08-01 12:34:56.123")
    max_ts_pq_0 = datetime.fromisoformat("2021-08-15 12:13:14")

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


@pytest.fixture(scope="session")
def pq_dir_date(tmp_path_factory: Path):
    """
    Create parquet files to be used during the testing session which use dates instead
    of timestamps. You can then pass this fixture into test functions.

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
    parquet_dir_date = tmp_path_factory.mktemp("data-date")
    write_parquet_files(str(parquet_dir_date), use_date=True)

    return parquet_dir_date


@pytest.fixture(scope="session")
def metastore_date_db(tmp_path_factory: Path, pq_dir_date):
    """
    Create a metastore based upon the parquet files created for testing. This version
    uses date instead of timestamp

    Parameters
    ----------
    tmp_path_factor : Path
        Session-scoped fixture which can be used to create arbitrary temporary
        directories from any other fixture or test
    pq_dir_date : Path
        User generated session-scoped fixture containing test parquet files to
        be added into the metastore

    Returns
    -------
    metastore_date_db : Path
        Path to the metastore database
    """
    data_dir = str(pq_dir_date)

    dbname = tmp_path_factory.mktemp("db") / "test_date.db"

    local_fs = fs.LocalFileSystem()
    dataset = ds.dataset(data_dir, format="parquet", filesystem=local_fs)
    pa_schema = dataset.schema

    metastore = Metastore(f"sqlite:///{dbname}", "test", pa_schema, "id", "timestamp")
    metastore.update(data_dir, local_fs)

    return dbname


def test_update_date(metastore_date_db, pq_dir_date):
    """
    Test that metastore using dates gets updated properly

    Parameters
    ----------
    metastore_date_db : Path
        Session-scopedfixture that is a path (file) to a test metastore
    pq_dir_date : Path
        Session-scoped fixture that is a path (dir) to the test parquet files
    """
    dbname = str(metastore_date_db)
    data_dir = str(pq_dir_date)
    dataset = ds.dataset(data_dir, format="parquet", filesystem=fs.LocalFileSystem())
    pa_schema = dataset.schema

    metastore = Metastore(f"sqlite:///{dbname}", "test", pa_schema)

    with metastore.engine.connect() as conn:
        results = conn.execute(select(metastore.table).order_by("filepath")).fetchall()
        assert len(results) == 10, f"test_update_date failed {len(results)} != 10"
        for idx, (filepath, id_min, id_max, ts_min, ts_max) in enumerate(results):
            ts_min = ts_min.isoformat()
            ts_max = ts_max.isoformat()
            filename = filepath.split("/")[-1]
            fname = f"part-{idx:02}-{id_min}_{id_max}_{ts_min}_{ts_max}.gzip.parquet"
            assert fname == filename, f"test_update_date failed {filename} != {fname}"


def test_query_date(metastore_date_db, pq_dir_date):
    """
    Test querying the metastore

    Parameters
    ----------
    metastore_date_db : Path
        Session-scopedfixture that is a path (file) to a test metastore
    pq_dir_date : Path
        Session-scoped fixture that is a path (dir) to the test parquet files
    """
    dbname = str(metastore_date_db)
    data_dir = str(pq_dir_date)
    dataset = ds.dataset(data_dir, format="parquet", filesystem=fs.LocalFileSystem())
    pa_schema = dataset.schema

    metastore = Metastore(f"sqlite:///{dbname}", "test", pa_schema)

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
    # For the first parquet file, which holds "00"
    # timestamp_min = "2021-07-15 12:13:14"
    # timestamp_max = "2021-08-15 12:13:14"
    min_ts_pq_0 = date.fromisoformat("2021-07-15")
    med_ts = date.fromisoformat("2021-08-01")
    max_ts_pq_0 = date.fromisoformat("2021-08-15")

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
