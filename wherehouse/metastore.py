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

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import logging
import logging.config
import pyarrow as pa
from pyarrow import fs
import pyarrow.parquet as pq
import sqlite3
from typing import Dict, List, Tuple, Union

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            "format": "%(asctime)s %(name)s %(levelname)s: %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": "INFO",
            "formatter": "default",
            "stream": "ext://sys.stdout",
        },
        "file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "DEBUG",
            "formatter": "default",
            "filename": "metastore.log",
            "maxBytes": 10485760,
            "backupCount": 1,
        },
    },
    "loggers": {
        "metastore": {
            "level": "DEBUG",
            "handlers": ["console", "file"],
            "propagate": False,
        }
    },
}
logging.config.dictConfig(LOGGING_CONFIG)


class Metastore:
    """
    Base class for storing some of the parquet metadata for a set of parquet files.
    The base class uses SQLite for storing the relevant information.
    """

    def __init__(
        self,
        store_args: Dict,
        store_table: str,
        arrow_schema: pa.lib.Schema,
        cluster_column: str,
        *optional_columns: str,
    ) -> None:
        """
        Initialize a connection to the metastore.

        Example:

        ```
        import pyarrow.parquet as pq

        pq_file = pq.ParquetFile("some.parquet")
        metastore = Metastore(
            {"database": "some.db"},
            "some_table",
            pq_file.schema_arrow,
            "id",
        )
        ```

        which creates the table "some_table" in the database with columns
        |----------|--------|--------|
        | filepath | id_min | id_max |
        |----------|--------|--------|

        Parameters
        ----------
        store_args : Dict
            Arguments needed to connect to the backend storage
        store_table : str
            Name of the table storing the parquet metadata
        arrow_schema : pa.lib.Schema
            Arrow schema of the parquet files
        cluster_column : str
            Name of the column in the Arrow schema used for clustering the data
        *optional_columns : str
            Additional columns in the Arrow schema whose metadata in the metastore.
            NOTE: Not currently implemented, but serving as a placeholder for future
            code.
        """
        self.store_args = store_args
        self.store_table = store_table
        self.arrow_schema = arrow_schema
        self.cluster_column = cluster_column
        self.optional_columns = optional_columns
        self.logger = logging.getLogger("metastore")

        self.conn = self._get_store_conn()
        self.placeholder = "?"  # Override this if using other backend database

        # See if the table already exists
        try:
            cur = self.conn.cursor()
            cur.execute(f"SELECT * from {self.store_table} limit 1")
        except sqlite3.OperationalError:
            table_exists = False
        else:
            table_exists = True
            cur.close()

        # Create the table if it doesn't exist
        if not table_exists:
            self.logger.info(f"Creating {store_table} in the metastore")
            self._create_table()
            self._create_indices()
        else:
            self.logger.info(f"{store_table} already exists. Skipping table creation")
            try:
                cur = self.conn.cursor()
                cur.execute(
                    f"SELECT {cluster_column}_min, {cluster_column}_max "
                    + f"from {store_table} limit 1"
                )
                for col in optional_columns:
                    cur.execute(
                        f"SELECT {col}_min, {col}_max from {store_table} limit 1"
                    )
            except Exception as exc:
                self.logger.WARNING(
                    f"Threw {exc} when checking cluster_column & "
                    + "optional_columns exist in metastore."
                )

    def update(
        self,
        parquet_file_or_dir: str,
        file_system: fs.FileSystem = fs.LocalFileSystem(),
        n_workers: int = 16,
    ) -> None:
        """
        Add parquet file metadata to the metastore. If a directory is provided, then a
        recursive walk is done. Any non-parquet files are simply skipped and logged.

        Example:

        ```
        from pyarrow import fs

        s3 = fs.S3FileSystem(region="us-east-1")
        parquet_dir = "path/on/local/to/parquets/"
        metastore.update(parquet_dir, s3)
        ```

        Parameters
        ----------
        parquet_file_or_dir : str
            Provide the filepath to either a single parquet file or a directory that
            contains many parquet files.
        file_system : fs.FileSystem, optional
            PyArrow file system where parquet files are located.
            Default is fs.LocalFileSystem()
        n_workers : int, optional
            Size of the thread pool used to concurrently retrieve parquet file
            metadata. Default is 16
        
        Returns
        -------
        None
        """
        start = datetime.now()
        metadata = self._gather_metadata(parquet_file_or_dir, file_system, n_workers)

        if not metadata:
            self.logger.warning(f"No metadata found in {parquet_file_or_dir}")
            return None
        else:
            assert len(metadata[0]) == (
                3 + 2 * len(self.optional_columns)
            ), "gathered metadata length does not match number of database columns"

        cur = self.conn.cursor()

        # Take care of the filepaths & cluster columns
        placeholders = f"{self.placeholder},{self.placeholder},{self.placeholder}"
        # Add in the other_columns
        for _ in self.optional_columns:
            placeholders += f",{self.placeholder},{self.placeholder}"

        cur.executemany(
            f"INSERT INTO {self.store_table} VALUES({placeholders})", metadata
        )
        cur.close()
        self.conn.commit()
        end = datetime.now()
        self.logger.info(
            f"updating({parquet_file_or_dir}) added {len(metadata):,} "
            + f"records in {end-start}"
        )

    @staticmethod
    def _get_min_max(filepath: str, column_idxs: List[int], file_system: fs.FileSystem):
        try:
            metadata = pq.ParquetFile(file_system.open_input_file(filepath)).metadata
        except pa.ArrowException as exc:
            return {"error_msg": f"ERROR for {filepath}: {exc}", "data": ()}

        data = [filepath]
        for idx in column_idxs:
            col_min = metadata.row_group(0).column(idx).statistics.min
            col_max = metadata.row_group(0).column(idx).statistics.max
            for r in range(metadata.num_row_groups):
                rg_min = metadata.row_group(r).column(idx).statistics.min
                rg_max = metadata.row_group(r).column(idx).statistics.max
                if rg_min < col_min:
                    col_min = rg_min
                if rg_max > col_max:
                    col_max = rg_max
            data.append(col_min)
            data.append(col_max)

        return {"error_msg": f"SUCCESS for {filepath}", "data": tuple(data)}

    def _gather_metadata(
        self,
        parquet_file_or_dir: Union[str, List[str]],
        file_system: fs.FileSystem,
        n_workers: int,
    ) -> List[Tuple]:
        """
        Gather the metadata pertaining to the cluster column and any optional columns
        from either a single parquet file or a directory. If a directory, recursively
        walk the directory for files. This uses a ThreadPool to spin speed things up.

        Parameters
        ----------
        parquet_file_or_dir : str | List[str]
            Single parquet filepath or a directory containing parquet files
        file_system : fs.FileSystem
            pyarrow file system where parquet file(s) are stored
        n_workers : int
            Size of the threadpool to speed things up
        
        Returns
        -------
        List[Tuple]
            One Tuple for each parquet file found. Tuple contains
            (filepath, cluster_col_min, cluster_col_max,...) with min/max columns
            for each optional column specified at initialization.
        """
        if file_system.get_file_info(parquet_file_or_dir).is_file:
            filepaths = [parquet_file_or_dir]
        else:
            file_selector = fs.FileSelector(parquet_file_or_dir, recursive=True)
            filepaths = [
                f.path
                for f in file_system.get_file_info(file_selector)
                if f.type == fs.FileType.File
            ]

        # Gather the mapping from column names to column idx in the schema
        arrow_columns_idx = [self.arrow_schema.names.index(self.cluster_column)]
        for col in self.optional_columns:
            col_idx = self.arrow_schema.names.index(col)
            arrow_columns_idx.append(col_idx)

        metadata = []
        with ThreadPoolExecutor(n_workers) as ex:
            future_to_path = {
                ex.submit(
                    Metastore._get_min_max, filepath, arrow_columns_idx, file_system
                ): filepath
                for filepath in filepaths
            }
            for future in as_completed(future_to_path):
                filepath = future_to_path[future]
                try:
                    result = future.result()
                except Exception as exc:
                    self.logger.error(f"_gather_metadata {filepath} threw: {exc}")
                else:
                    data = result["data"]
                    if data:
                        metadata.append(data)
                    else:
                        if result["error_msg"].startswith("ERROR"):
                            self.logger.error(result["error_msg"])
                        else:
                            self.logger.info(result["error_msg"])

        return metadata

    def query(
        self, cluster_column_values: List, other_column_values: Dict = None,
    ) -> Dict[str, List[str]]:
        """
        Given the `cluster_column_values` return the filepaths of the parquet
        files whose min/max contains a cluster_column_value.

        NOTE: other_column_values is not implemented yet and will be ignored

        Parameters
        ----------
        cluster_column_values : List
            List of cluster column values of interest.
        other_column_values : Dict
            Future capability to also use any additional columns that have been
            gathered into the wherehouse metastore.

        Returns
        -------
        Dict[str, List[str]]
            Keys are the filepaths to the parquet files. Values are a list of
            the cluster column values associated with that parquet file
        """
        start = datetime.now()
        if other_column_values is not None:
            self.logger.warn("other_column_values not implemented yet. Ignoring")

        pq_files = defaultdict(list)

        cur = self.conn.cursor()
        cluster_min = f"{self.cluster_column}_min"
        cluster_max = f"{self.cluster_column}_max"
        for cluster_column_value in cluster_column_values:
            cur.execute(
                f"""
                SELECT filepath
                FROM {self.store_table}
                WHERE {self.placeholder} >= {cluster_min}
                AND   {self.placeholder} <= {cluster_max}
                ;
                """,
                (cluster_column_value, cluster_column_value),
            )
            for (filepath,) in cur.fetchall():
                pq_files[filepath].append(cluster_column_value)
        cur.close()
        end = datetime.now()
        self.logger.info(f"query returned {len(pq_files)} results in {end-start}")

        return pq_files

    def _get_store_conn(self):
        """
        Uses the store_args given to establish a connection to the metastore's backend
        storage.

        Override this for other backend storage
        """
        return sqlite3.connect(**self.store_args)

    @staticmethod
    def _map_pa_type(pa_type: pa.DataType):
        """
        Map arrow DataTypes to SQLite 'storage classes'.
        Note: Subclasses need to override this method

        Parameters
        ----------
        pa_type : pa.DataType

        Returns
        -------
        str
            Corresponding storage class for the database
        """
        if pa.types.is_string(pa_type) or pa.types.is_large_string(pa_type):
            return "TEXT"
        elif pa.types.is_integer(pa_type):
            return "INTEGER"
        elif pa.types.is_floating(pa_type):
            return "REAL"
        elif pa.types.is_date(pa_type):
            return "TEXT"
        elif pa.types.is_timestamp(pa_type):
            return "TEXT"

    def _create_table(self) -> None:
        """
        Create the table, store_table, based upon the arrow schema and column names
        stored in arrow_columns

        Returns
        -------
        None
        """
        sql_str = f"""
            CREATE TABLE {self.store_table} (
                filepath TEXT PRIMARY KEY,
        """
        # Add the cluster column
        db_type = Metastore._map_pa_type(
            self.arrow_schema.field(self.cluster_column).type
        )
        sql_str += f"{self.cluster_column}_min {db_type},"
        sql_str += f"{self.cluster_column}_max {db_type},"

        # Add optional columns to be stored in the database
        for col in self.optional_columns:
            db_type = Metastore._map_pa_type(self.arrow_schema.field(col).type)
            # Might not have defined the mapping between arrow & database
            if db_type is not None:
                sql_str += f"{col}_min {db_type},"
                sql_str += f"{col}_max {db_type},"
        # Remove the last "," & close parenthesis adding ";" for good measure
        sql_str = sql_str[:-1] + ");"

        cur = self.conn.cursor()
        cur.execute(sql_str)
        cur.close()
        self.conn.commit()

    def _create_indices(self) -> None:
        """
        Internal method to create indices for the various columns in the backend
        storage. Making a separate index for every column besides 'filepath'
        """
        cur = self.conn.cursor()
        for col in [self.cluster_column] + list(self.optional_columns):
            for ext in ["min", "max"]:
                index_name = f"{self.store_table}_{col}_{ext}_index"
                cur.execute(
                    f"CREATE INDEX {index_name} ON {self.store_table}"
                    + f"({col}_{ext});"
                )
        cur.close()
        self.conn.commit()

    def __del__(self):
        try:
            self.conn.close()
        except Exception:
            pass
