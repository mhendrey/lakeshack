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
import pandas as pd
from pathlib import Path
from pyarrow import fs
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pytest

from .utils import write_parquet_files
from wherehouse.metastore import Metastore
from wherehouse.wherehouse import Wherehouse


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

    metastore = Metastore(
        {"database": str(dbname)}, "test", pa_schema, "id", "timestamp"
    )
    metastore.update(data_dir, local_fs)

    return dbname


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

    metastore = Metastore({"database": dbname}, "test", pa_schema, "id", "timestamp")
    wherehouse = Wherehouse(metastore, fs.LocalFileSystem())

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
