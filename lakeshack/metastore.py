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
import pytz
import sqlalchemy as sa
from typing import Dict, List, Tuple, Union, Any


class Metastore:
    """
    Store metadata from parquet files into a database using SQLAlchmey

    Parameters
    ----------
    store_url : str
        URL string to backend database. See sqlalchemy.create_engine() for more
        details and examples.
    store_table : str
        Name of the table storing the parquet metadata
    arrow_schema : pa.lib.Schema
        Arrow schema of the parquet files
    cluster_column : str, optional
        Name of the column in the Arrow schema used for clustering the data. If
        `None` (default), then expecting the table to already exist.
    \*optional_columns : str
        Additional columns in the Arrow schema whose metadata is to be stored in
        the metastore. These should have some clustering in order to be useful,
        even if less clustering than in the `cluster_column`
    \*\*store_kwargs : Any, optional
        Arguments to be passed to sqlalchemy.create_engine()

    Attributes
    ----------
    engine : sa.engine.base.Engine
        SQLAlchemy engine that connects to the database
    table : sa.sql.schema.Table
        SQLAlchemy table holding the metadata

    Example
    -------

    ::

        from pyarrow import fs
        import pyarrow.dataset as ds
        from lakeshack.metastore import Metastore

        s3 = fs.S3FileSystem(region="us-iso-east-1")
        s3_dir = "sales_data/2023/03/15/"
        dataset = ds.dataset(s3_dir, filesystem=s3, format="parquet")

        metastore = Metastore("sqlite:///:memory:", "sales_table", dataset.schema,
            "customer_id", "timestamp")
        metastore.update(s3_dir, filesystem=s3, n_workers=30)

    """

    def __init__(
        self,
        store_url: str,
        store_table: str,
        arrow_schema: pa.lib.Schema,
        cluster_column: str = None,
        *optional_columns: str,
        **store_kwargs,
    ) -> None:
        """
        Initialize a connection to the metastore.

        """
        self.store_url = store_url
        self.store_table = store_table
        self.arrow_schema = arrow_schema
        self.cluster_column = cluster_column
        self.optional_columns = optional_columns

        self.logger = logging.getLogger(__name__)

        self.engine = sa.create_engine(store_url, **store_kwargs)
        self.metadata_obj = sa.MetaData()
        # See if the table already exists
        try:
            self.metadata_obj.reflect(self.engine, only=[self.store_table])
        except:
            table_exists = False
        else:
            table_exists = True

        # Create the table if it doesn't exist
        if not table_exists:
            self.logger.info(f"Creating {store_table} in the metastore")
            self._create_table()
        else:
            self.logger.info(f"{store_table} already exists. Skipping table creation")
            self.table = self.metadata_obj.tables[self.store_table]
            # Get the cluster_column from the table if not given in init()
            if cluster_column is None:
                col_name_parts = self.table.columns[1].name.split("_min")[:-1]
                self.cluster_column = "".join(col_name_parts)
            # Get optional columns from the table if not given in init()
            if len(optional_columns) == 0:
                self.optional_columns = []
                for i in range(3, len(self.table.columns), 2):
                    col_name_parts = self.table.columns[i].name.split("_min")[:-1]
                    self.optional_columns.append("".join(col_name_parts))

            # Some quality checking to make sure that the table has the same format
            if len(self.table.columns) != (3 + 2 * len(self.optional_columns)):
                raise ValueError(f"Existing table columns don't match those given")

            ################ Check the individual columns
            # filepath column
            col = self.table.columns[0]
            if col.name != "filepath":
                raise ValueError(f"{col.name} != filepath")
            if not isinstance(col.type, sa.String):
                raise TypeError(
                    f"{col.name}, {col.type} is not an instance of {sa.String}"
                )

            # cluster_column_min
            col = self.table.columns[1]
            col_name = f"{self.cluster_column}_min"
            if col.name != col_name:
                raise ValueError(f"cluster_column mismatch: {col.name} != {col_name}")
            schema_name = "".join(col_name.split("_min")[:-1])
            schema_type = Metastore._map_pa_type(
                self.arrow_schema.field(schema_name).type
            )
            if not isinstance(col.type, schema_type):
                raise TypeError(
                    f"cluster_column type mismatch: "
                    + f"{col.type} is not an instance of {schema_type}"
                )

            # cluster_column_max
            col = self.table.columns[2]
            col_name = f"{self.cluster_column}_max"
            if col.name != col_name:
                raise ValueError(f"cluster_column mismatch: {col.name} != {col_name}")
            schema_name = "".join(col_name.split("_max")[:-1])
            schema_type = Metastore._map_pa_type(
                self.arrow_schema.field(schema_name).type
            )
            if not isinstance(col.type, schema_type):
                raise TypeError(
                    f"cluster_column type mismtach: "
                    + f"{col.type} is not an instance of {schema_type}"
                )

            # optional columns
            for i, optional_column in enumerate(optional_columns):
                # _min column
                col = self.table.columns[i * 2 + 3]
                col_name = f"{optional_column}_min"
                if col.name != col_name:
                    raise ValueError(
                        f"optional_column mismatch {col.name} != {col_name}"
                    )
                schema_type = Metastore._map_pa_type(
                    self.arrow_schema.field(optional_column).type
                )
                if not isinstance(col.type, schema_type):
                    raise TypeError(
                        f"optional_column type mismatch: {col_name}, "
                        + f"{col.type} is not an instance of {schema_type}"
                    )

                # _max column
                col = self.table.columns[(i * 2 + 1) + 3]
                col_name = f"{optional_column}_max"
                if col.name != col_name:
                    raise ValueError(
                        f"optional_column mismatch {col.name} != {col_name}"
                    )
                schema_type = Metastore._map_pa_type(
                    self.arrow_schema.field(optional_column).type
                )
                if not isinstance(col.type, schema_type):
                    raise TypeError(
                        f"optional_column type mismatch: {col_name}, "
                        + f"{col.type} is not an instance of {schema_type}"
                    )

    def update(
        self,
        parquet_file_or_dir: str,
        filesystem: fs.FileSystem = fs.LocalFileSystem(),
        n_workers: int = 16,
    ) -> None:
        """
        Add parquet file metadata to the metastore. If a directory is provided, then a
        recursive walk is done. Any non-parquet files are simply skipped and logged.

        Parameters
        ----------
        parquet_file_or_dir : str
            Provide the filepath to either a single parquet file or a directory that
            contains many parquet files.
        filesystem : fs.FileSystem, optional
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
        metadata = self._gather_metadata(parquet_file_or_dir, filesystem, n_workers)

        if not metadata:
            self.logger.warning(f"update() No metadata found in {parquet_file_or_dir}")
            return None
        else:
            assert len(metadata[0]) == (
                3 + 2 * len(self.optional_columns)
            ), "gathered metadata length does not match number of database columns"

        with self.engine.connect() as conn:
            # If filepath is already in the metastore, then skip it
            add_metadata = []
            existing_filepaths = [
                r[0]
                for r in conn.execute(
                    sa.select(self.table.c.filepath).where(
                        self.table.c.filepath.in_([a["filepath"] for a in metadata])
                    )
                ).fetchall()
            ]
            for m in metadata:
                if m["filepath"] not in existing_filepaths:
                    add_metadata.append(m)
                else:
                    self.logger.warning(
                        f"update() {m['filepath']} already in metastore. Skipping it"
                    )
            try:
                if add_metadata:
                    conn.execute(
                        sa.insert(self.table),
                        add_metadata,
                    )
                    conn.commit()
            except sa.exc.IntegrityError as exc:
                self.logger.error(f"update() threw {exc}")
                add_metadata = []

        end = datetime.now()
        self.logger.info(
            f"update({parquet_file_or_dir}) added {len(add_metadata):,} "
            + f"records in {end-start}"
        )

    @staticmethod
    def _get_min_max(
        filepath: str, column_idxs: List[int], filesystem: fs.FileSystem
    ) -> Dict:
        """
        Worker function used by a thread pool to retrieve min/max values from a given
        parquet file

        Parameters
        ----------
        filepath : str
            Filepath to a parquet file
        column_idxs : List[int]
            Column ids from the arrow schema from which to gather min/max values
        filesystem : fs.FileSystem
            File system storing `filepath`

        Returns
        -------
        Dict
            {"error_msg": msg, "data": Dict[str, Any]}
        """
        try:
            pq_file = pq.ParquetFile(filesystem.open_input_file(filepath))
            metadata = pq_file.metadata
            arrow_schema = pq_file.schema_arrow
        except pa.ArrowException as exc:
            return {"error_msg": f"ERROR for {filepath}: {exc}", "data": {}}

        data = {"filepath": filepath}
        for idx in column_idxs:
            col_name = metadata.schema[idx].name
            col_min = metadata.row_group(0).column(idx).statistics.min
            col_max = metadata.row_group(0).column(idx).statistics.max
            # Parquet metadata seems to cast timezones to UTC. This casts them back
            if pa.types.is_timestamp(arrow_schema[idx].type):
                if arrow_schema[idx].type.tz is not None:
                    col_min = col_min.astimezone(
                        tz=pytz.timezone(arrow_schema[idx].type.tz)
                    )
                    col_max = col_max.astimezone(
                        tz=pytz.timezone(arrow_schema[idx].type.tz)
                    )
            for r in range(metadata.num_row_groups):
                rg_min = metadata.row_group(r).column(idx).statistics.min
                rg_max = metadata.row_group(r).column(idx).statistics.max
                if pa.types.is_timestamp(arrow_schema[idx].type):
                    if arrow_schema[idx].type.tz is not None:
                        rg_min = rg_min.astimezone(
                            tz=pytz.timezone(arrow_schema[idx].type.tz)
                        )
                        rg_max = rg_max.astimezone(
                            tz=pytz.timezone(arrow_schema[idx].type.tz)
                        )
                if rg_min < col_min:
                    col_min = rg_min
                if rg_max > col_max:
                    col_max = rg_max
            data[f"{col_name}_min"] = col_min
            data[f"{col_name}_max"] = col_max

        return {"error_msg": f"SUCCESS for {filepath}", "data": data}

    def _gather_metadata(
        self,
        parquet_file_or_dir: Union[str, List[str]],
        filesystem: fs.FileSystem,
        n_workers: int,
    ) -> List[Tuple]:
        """
        Gather the metadata pertaining to the cluster column and any optional columns
        from either a single parquet file or a directory. If a directory, recursively
        walk the directory for files. This uses a ThreadPool to speed things up.

        Parameters
        ----------
        parquet_file_or_dir : str | List[str]
            Single parquet filepath or a directory containing parquet files
        filesystem : fs.FileSystem
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
        if filesystem.get_file_info(parquet_file_or_dir).is_file:
            filepaths = [parquet_file_or_dir]
        else:
            file_selector = fs.FileSelector(parquet_file_or_dir, recursive=True)
            filepaths = [
                f.path
                for f in filesystem.get_file_info(file_selector)
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
                    Metastore._get_min_max, filepath, arrow_columns_idx, filesystem
                ): filepath
                for filepath in filepaths
            }
            for future in as_completed(future_to_path):
                filepath = future_to_path[future]
                try:
                    result = future.result()
                except Exception as exc:
                    self.logger.error(f"_gather_metadata({filepath}) threw: {exc}")
                else:
                    data = result["data"]
                    if data:
                        metadata.append(data)
                    else:
                        if result["error_msg"].startswith("ERROR"):
                            self.logger.error(
                                f'_gather_metadata() {result["error_msg"]}'
                            )
                        else:
                            self.logger.info(
                                f'_gather_metadata() {result["error_msg"]}'
                            )

        return metadata

    def query(
        self,
        cluster_column_values: List[Any],
        optional_where_clauses: List[Tuple] = [],
    ) -> Dict[str, List[Any]]:
        """
        Given the `cluster_column_values` return the filepaths of the parquet
        files whose min/max contains a cluster_column_value.

        If `optional_where_clauses` are provide, then further restrict the filepaths
        to return so they match these conditions too.

        Parameters
        ----------
        cluster_column_values : List[Any]
            List of cluster column values of interest
        optional_where_clauses : List[Tuple], optional
            List of optional columns to further restrict the parquet files to be
            queried. Each tuple is three values column_name,
            comparision operator [>=, >, =, ==, <, <=], and value

        Returns
        -------
        Dict[str, List[Any]]
            Keys are the filepaths to the parquet files. Values are a list of
            the cluster column values associated with that parquet file
        """
        start = datetime.now()
        pq_files = defaultdict(list)

        col_cluster_min = self.table.columns[f"{self.cluster_column}_min"]
        col_cluster_max = self.table.columns[f"{self.cluster_column}_max"]
        for cluster_column_value in cluster_column_values:
            stmt = sa.select(self.table.c.filepath).where(
                sa.and_(
                    col_cluster_min <= cluster_column_value,
                    cluster_column_value <= col_cluster_max,
                )
            )
            for col, op, value in optional_where_clauses:
                if col not in self.optional_columns:
                    continue
                col_min = self.table.columns[f"{col}_min"]
                col_max = self.table.columns[f"{col}_max"]
                if op == ">=":
                    stmt = stmt.where(value <= col_max)
                elif op == ">":
                    stmt = stmt.where(value < col_max)
                elif op == "=" or op == "==":
                    stmt = stmt.where(sa.and_(col_min <= value, value <= col_max))
                elif op == "<":
                    stmt = stmt.where(value > col_min)
                elif op == "<=":
                    stmt = stmt.where(value >= col_min)
                else:
                    self.logger.error(
                        f"query() optional_where_clause {op} is not a valid comparision"
                    )
                    raise ValueError(f"{op} is not a valid comparision")

            with self.engine.connect() as conn:
                for (filepath,) in conn.execute(stmt):
                    pq_files[filepath].append(cluster_column_value)

        end = datetime.now()
        self.logger.info(f"query() returned {len(pq_files)} results in {end-start}")

        return pq_files

    @staticmethod
    def _map_pa_type(pa_type: pa.DataType):
        """
        Map arrow DataTypes to SQLAlchemy data types

        Parameters
        ----------
        pa_type : pa.DataType

        Returns
        -------
        Corresponding storage class for the database
        """
        if pa.types.is_string(pa_type) or pa.types.is_large_string(pa_type):
            return sa.String
        elif pa.types.is_integer(pa_type):
            return sa.BigInteger
        elif pa.types.is_floating(pa_type):
            return sa.Float
        elif pa.types.is_date(pa_type):
            return sa.Date
        elif pa.types.is_timestamp(pa_type):
            return sa.DateTime
            # if pa_type.tz is not None:
            #    return sa.DateTime(
            #        timezone=False
            #    )  # Should be true; but reflection doesn't pick up the timezone=True
            # else:
            #    return sa.DateTime(timezone=False)

    def _create_table(self) -> None:
        """
        Create the table, store_table, based upon the arrow schema and column names
        stored in arrow_columns

        Returns
        -------
        None
        """
        # Specify the 'filepath' column as a string and the primary key for the table
        columns = [sa.Column("filepath", sa.String, primary_key=True)]

        # Specify the cluster columns and corresponding type
        db_type = Metastore._map_pa_type(
            self.arrow_schema.field(self.cluster_column).type
        )
        columns.append(
            sa.Column(f"{self.cluster_column}_min", db_type, nullable=False, index=True)
        )
        columns.append(
            sa.Column(f"{self.cluster_column}_max", db_type, nullable=False, index=True)
        )

        # Add optional columns to be stored in the database
        for col in self.optional_columns:
            arrow_type = self.arrow_schema.field(col).type
            db_type = Metastore._map_pa_type(arrow_type)
            # Might not have defined the mapping between arrow & database
            if db_type is not None:
                columns.append(
                    sa.Column(f"{col}_min", db_type, nullable=False, index=True)
                )
                columns.append(
                    sa.Column(f"{col}_max", db_type, nullable=False, index=True)
                )
            else:
                self.logger.warning(
                    f"_create_table() {col} with {arrow_type=:} failed to map to "
                    + f"database type. Not adding {col}_min or {col}_max to database "
                    + "table"
                )

        self.table = sa.Table(
            self.store_table,
            self.metadata_obj,
            *columns,
        )
        self.table.create(self.engine, checkfirst=True)
