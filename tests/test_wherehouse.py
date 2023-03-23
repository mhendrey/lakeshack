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
from datetime import date, datetime
import pandas as pd
from pathlib import Path
from pyarrow import fs
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pytest
import pytz

from .utils import write_parquet_files
from wherehouse.metastore import Metastore
from wherehouse.wherehouse import Wherehouse


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


def query_check(wherehouse: Wherehouse, timestamp_type: str):
    """Check that querying for cluster column values works as expected

    Parameters
    ----------
    wherehouse : Wherehouse
        Wherehouse to check
    timestamp_type : str
        Use 'ts' | 'tz' | 'dt' to specify whether the timestamp column in parquet
        files are datetime (naive), datetime (tz aware) , or date
    """
    queries = ["01", "22", "35", "4f", "70", "8a", "a0", "bf", "d1", "f0"]

    table = wherehouse.query("00")
    assert table.num_rows == 3, f"test_query: Returned {table.num_rows} instead of 3"

    table = wherehouse.query(queries, columns=["id", "x"])
    assert table.num_rows == 31, f"test_query: Returned {table.num_rows} instead of 31"
    assert (
        table.num_columns == 2
    ), f"test_query: Returned {table.num_columns} instead of 2"
    assert (
        "id" in table.column_names and "x" in table.column_names
    ), f"test_query: {table.column_names} is not ['id','x']"
    sum_x = pc.sum(table.column(1)).as_py()
    assert sum_x == 7108, f"test_query: Sum of x = {sum_x} instead of 7108"

    # use optional timestamp query
    if timestamp_type == "ts":
        timestamp = datetime.fromisoformat("2021-08-10T22:50:53")
    elif timestamp_type == "tz":
        tz = pytz.timezone("US/Eastern")
        timestamp = tz.localize(datetime.fromisoformat("2021-08-10T22:50:53"))
    elif timestamp_type == "dt":
        timestamp = date.fromisoformat("2021-08-10")
    else:
        raise ValueError(f"{timestamp=:} does not match 'ts', 'tz', or 'dt'")

    table = wherehouse.query(
        "00", optional_where_clauses=[("timestamp", "<=", timestamp)]
    )
    assert table.num_rows == 2, f"query_check: Returned {table.num_rows} instead of 2"
    sum_x = pc.sum(table.column(2)).as_py()
    assert sum_x == 609, f"query_check: {sum_x=:} instead of 609"


def test_query_ts(metastore_db_ts, pq_dir_ts):
    """
    Test querying the metastore when timestamp column is naive datetime

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
    wherehouse = Wherehouse(metastore, fs.LocalFileSystem())
    query_check(wherehouse, "ts")


def test_query_tz(metastore_db_tz, pq_dir_tz):
    """
    Test querying the metastore when timestamp column is tz aware datetime

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
    wherehouse = Wherehouse(metastore, fs.LocalFileSystem())
    query_check(wherehouse, "tz")


def test_query_dt(metastore_db_dt, pq_dir_dt):
    """
    Test querying the metastore when timestamp column is a date

    Parameters
    ----------
    metastore_db_dt : Path
        Session-scoped fixture that is a path (file) to a test metastore
    pq_dir_dt : Path
        Session-scoped fixture that is a path (dir) to the test parquet files
    """
    dbname = str(metastore_db_dt)
    data_dir = str(pq_dir_dt)
    dataset = ds.dataset(data_dir, format="parquet", filesystem=fs.LocalFileSystem())
    pa_schema = dataset.schema

    metastore = Metastore(f"sqlite:///{dbname}", "test", pa_schema)
    wherehouse = Wherehouse(metastore, fs.LocalFileSystem())
    query_check(wherehouse, "dt")


def test_batch_size_ts(metastore_db_ts, pq_dir_ts: str) -> None:
    """Test that returned results don't exceed n_records_max + batch_size
    'a9' has 9 records with the choosen random seed

    Parameters
    ----------
    metastore_db_ts :
        Path to a sqlite database storing the metastore table
    pq_dir_ts : str
        Directory containing the parquet files that have naive datetime
    """
    dbname = str(metastore_db_ts)
    data_dir = str(pq_dir_ts)
    dataset = ds.dataset(data_dir, format="parquet", filesystem=fs.LocalFileSystem())
    pa_schema = dataset.schema

    metastore = Metastore(f"sqlite:///{dbname}", "test", pa_schema)
    wherehouse = Wherehouse(metastore, fs.LocalFileSystem())

    table = wherehouse.query("a9")
    assert table.num_rows == 9, f"{table.num_rows=:}, expected this to be == 9"

    table = wherehouse.query("a9", batch_size=2, n_records_max=6)
    assert table.num_rows <= 8, f"{table.num_rows=:}, expected this to be <= 8"

    table = wherehouse.query("a9", batch_size=1, n_records_max=6)
    assert table.num_rows <= 7, f"{table.num_rows=:}, expected this to be <= 7"
