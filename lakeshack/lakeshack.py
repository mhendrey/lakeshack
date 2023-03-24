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

import boto3
from botocore.exceptions import ClientError
from concurrent.futures import as_completed, ThreadPoolExecutor
from datetime import datetime
import json
import logging
import logging.config
import pandas as pd
import pyarrow as pa
from pyarrow import fs
import pyarrow.compute as pc
import pyarrow.dataset as ds
import sqlalchemy as sa
from typing import Any, List, Tuple

from lakeshack.metastore import Metastore


class Lakeshack:
    """
    Retrieve records stored in Parquet files by first checking the Metastore to
    determine which Parquet files might have the requested records and then only
    querying these files.
    """

    def __init__(
        self, metastore: Metastore, filesystem: fs.FileSystem = fs.LocalFileSystem()
    ):
        """
        Instantiate a Lakeshack that will use the metastore & filesystem to speed
        up retrieval of records from the parquet files stored in the filesystem.

        Example:

        ```
        import pyarrow.dataset as ds
        from pyarrow import fs

        from lakeshack.metastore import Metastore
        from lakeshack.lakeshack import Lakeshack

        s3 = fs.S3FileSystem(region="us-iso-east-1")
        dataset = ds.dataset(
            "path/in/s3/to/parquets/",
            format="parquet",
            filesystem=s3,
        )
        metastore = Metastore(
            "sqlite:///some.db",
            "some_table",
            dataset.schema,
            "id",
        )
        metastore.update("path/in/s3/to/parquets/", s3, n_workers=20)
        lakeshack = Lakeshack(metastore, s3)
        pa_table = lakeshack.query_s3_select("some_id", n_workers=20)
        ```

        Parameters
        ----------
        metastore : Metastore
            Metastore containing the parquet file metadata that lakeshack will use
        filesystem : fs.FileSystem, optional
            pyarrow file system. Use fs.S3FileSystem(region='your_region') for
            connecting to S3. Default is fs.LocalSystem()
        """
        self.metastore = metastore
        self.filesystem = filesystem
        self.logger = logging.getLogger(__name__)

        columns = []
        for col_arrow in self.metastore.arrow_schema:
            col_name = col_arrow.name
            db_type = Metastore._map_pa_type(col_arrow.type)
            columns.append(sa.Column(col_name, db_type, quote=True))
        self.s3_table = sa.Table("s3object", sa.MetaData(), *columns).alias("s")

    def _construct_sql_statement(
        self,
        cluster_values: List[Any],
        optional_where_clauses: List[Tuple[str, str, Any]] = [],
        columns: List[str] = None,
    ) -> str:
        """
        Create a sql statement that can be given to s3's select_object_content().
        When doing column projections with Parquet as the InputSerialization, I've
        found that you need to have the column names in double quotes.

        Parameters
        ----------
        cluster_values : List[Any]
            List of cluster values that you want included in the WHERE clause as
            `cluster_column in (cv0, cv1, cv2)`
        optional_where_clauses : List[Tuple[str, str, Any]], optional
            List of optional columns to further restrict the parquet files to be
            queried. Each tuple is three values column_name,
            comparision operator [>=, >, =, ==, <, <=], value. This will be logically
            connected using "AND" between each in the list and with the
            cluster_column_values. Default is []
        columns : List[str]
            Specify the subset of columns to select. Default is None which is select *

        Returns
        -------
        sql_statement : str
            SQL statement to pass to s3.select_object_content()
        """
        if columns:
            proj_cols = []
            for col in columns:
                proj_cols.append(self.s3_table.columns[col])
            stmt = sa.select(*proj_cols)
        else:
            stmt = sa.select(self.s3_table)
        stmt = stmt.where(
            self.s3_table.columns[self.metastore.cluster_column].in_(cluster_values)
        )

        # Handle optional where clauses
        for (col, op, value) in optional_where_clauses:
            table_col = self.s3_table.columns[col]
            # Handle datetime and date the same for s3.select
            if isinstance(table_col.type, sa.DateTime) or isinstance(
                table_col.type, sa.Date
            ):
                value = sa.func.TO_TIMESTAMP(value.isoformat())
            if op == ">=":
                stmt = stmt.where(table_col >= value)
            elif op == ">":
                stmt = stmt.where(table_col > value)
            elif op == "=" or op == "==":
                stmt = stmt.where(table_col == value)
            elif op == "<":
                stmt = stmt.where(table_col < value)
            elif op == "<=":
                stmt = stmt.where(table_col <= value)
            else:
                self.logger.error(
                    f"_construct_sql_statement() optional_where_clause {op} is not "
                    + "a valid comparision"
                )
                raise ValueError(f"{op} is not a valid comparision")

        sql_statement = str(stmt.compile(compile_kwargs={"literal_binds": True}))

        return sql_statement

    def _select_worker(
        self,
        filepath: str,
        cluster_values: List[Any],
        optional_where_clauses: List[Tuple[str, str, Any]] = [],
        columns: List[str] = None,
    ):
        """Use s3.select to retrieved records matching the cluster values and any
        optional where clauses from the given filepath in S3.

        Parameters
        ----------
        filepath : str
            S3 filepath to run the query against
        cluster_values : List[Any]
            List of cluster values that you want included in the WHERE clause as
            `cluster_column in (cv0, cv1, cv2)`
        optional_where_clauses : List[Tuple[str, str, Any]], optional
            List of optional columns to further restrict the parquet files to be
            queried. Each tuple is three values column_name,
            comparision operator [>=, >, =, ==, <, <=], value. This will be logically
            connected using "AND" between each in the list and with the
            cluster_column_values. Default is []
        columns : List[str], optional
            Specify the subset of columns to select. Default is None which is select *

        Returns
        -------
        Dict
            Dictionary containing 'data', 'BytesScanned', 'BytesProcessed', and
            'BytesReturned'
        """

        start = datetime.now()
        s3_client = boto3.session.Session().client(
            "s3", region_name=self.filesystem.region
        )
        bucket = filepath.split("/")[0]
        key = "/".join(filepath.split("/")[1:])

        sql_statement = self._construct_sql_statement(
            cluster_values, optional_where_clauses, columns
        )

        try:
            response = s3_client.select_object_content(
                Bucket=bucket,
                Key=key,
                Expression=sql_statement,
                ExpressionType="SQL",
                InputSerialization={"Parquet": {}},
                OutputSerialization={"JSON": {"RecordDelimiter": "\n"}},
            )
        except ClientError as exc:
            self.logger.error(f"_select_worker() {filepath} threw: {exc}")
            return {
                "data": [],
                "BytesScanned": 0,
                "BytesProcessed": 0,
                "BytesReturned": 0,
            }

        result = ""
        bytes_scanned = 0
        bytes_processed = 0
        bytes_returned = 0
        for event in response["Payload"]:
            if "Records" in event:
                result += event["Records"]["Payload"].decode("utf-8")
            elif "Stats" in event:
                statsDetails = event["Stats"]["Details"]
                bytes_scanned += statsDetails["BytesScanned"]
                bytes_processed += statsDetails["BytesProcessed"]
                bytes_returned += statsDetails["BytesReturned"]

        records = []
        for s in result.split("\n"):
            if s:
                try:
                    record = json.loads(s)
                except Exception as exc:
                    self.logger.error(
                        f"_select_worker() Failed to json load {s}: {exc}"
                    )
                else:
                    records.append(record)
        end = datetime.now()

        self.logger.debug(
            f"_select_worker() {filepath} took {end-start} for {len(records)} "
            + f"scanned={bytes_scanned/1024**2:.0f}MB, "
            + f"processed={bytes_processed/1024**2:.0f}MB "
            + f"returned={bytes_returned/1024:.0f}KB"
        )
        return {
            "data": records,
            "BytesScanned": bytes_scanned,
            "BytesProcessed": bytes_processed,
            "BytesReturned": bytes_returned,
        }

    def query_s3_select(
        self,
        cluster_column_values,
        optional_where_clauses: List[Tuple] = [],
        columns: List[str] = None,
        n_records_max: int = 2000000,
        n_workers: int = 20,
    ) -> pa.lib.Table:
        """
        Retrieve records from the parquet files stored in S3 using S3 Select using a
        threadpool to speed up the queries.

        Parameters
        ----------
        cluster_column_values : str, List[str]
            Provide a single cluster column value or a list of them whose
            records you want to retrieve
        optional_where_clauses : List[Tuple], optional
            List of optional columns to further restrict the parquet files to be
            queried. Each tuple is three values column_name,
            comparision operator [>=, >, =, ==, <, <=], value. This will be logically
            connected using "AND" between each in the list and with the
            cluster_column_values
        columns : List[str]
            Specify the subset of columns to select. Default is None which is select *
        n_records_max : int, optional
            Once this many results have been exceed, stop retrieving results and return
            what has been retrieved so far. Thus, the Table.num_rows returned may be
            greater than n_records_max. Default is 2M
        n_workers : int, optional
            Number of workers in the ThreadPool to launch parallel calls to S3 Select.
            Default is 20.

        Returns
        -------
        pa.lib.Table
        """
        start = datetime.now()
        if not isinstance(self.filesystem, fs.S3FileSystem):
            raise TypeError(f"filesystem must be S3. You have {self.filesystem}")

        if not isinstance(cluster_column_values, list):
            cluster_column_values = [cluster_column_values]

        # optional_where_clauses = self._cast_to_pyarrow_schema(optional_where_clauses)
        filepaths_to_cluster_values = self.metastore.query(
            cluster_column_values, optional_where_clauses
        )

        if columns is None:
            columns = self.metastore.arrow_schema.names
        query_fields = []
        query_fields_safe = []
        for column in columns:
            pa_field = self.metastore.arrow_schema.field(column)
            query_fields.append(pa_field)
            if pa.types.is_timestamp(pa_field.type) or pa.types.is_date(pa_field.type):
                pa_field = pa.field(column, pa.string())
            query_fields_safe.append(pa_field)
        query_schema_safe = pa.schema(query_fields_safe)
        query_schema = pa.schema(query_fields)

        batches = []
        n_records = 0
        bytes_scanned = 0
        bytes_processed = 0
        bytes_returned = 0
        with ThreadPoolExecutor(n_workers) as executor:
            future_to_file = {}
            for filepath, cluster_values in filepaths_to_cluster_values.items():
                future = executor.submit(
                    self._select_worker,
                    filepath,
                    cluster_values,
                    optional_where_clauses,
                    columns,
                )
                future_to_file[future] = filepath

            check = False
            for future in as_completed(future_to_file):
                filepath = future_to_file[future]
                try:
                    data = future.result()
                except Exception as exc:
                    self.logger.error(f"query_s3_select() {filepath} threw: {exc}")
                else:
                    if data["data"]:
                        n_records += len(data["data"])
                        batches.append(
                            pa.RecordBatch.from_pylist(data["data"], query_schema_safe)
                        )
                    bytes_scanned += data["BytesScanned"]
                    bytes_processed += data["BytesProcessed"]
                    bytes_returned += data["BytesReturned"]
                    if n_records > n_records_max:
                        check = True
                        break

            while check:
                check = False
                for future in future_to_file:
                    if not future.done():
                        future.cancel()
                        check = True

        table = pa.Table.from_batches(batches, schema=query_schema_safe)

        # Ideally we should be able to use pyarrow to cast back the timestamp and/or
        # date fields from the strings returned by S3.  But pyarrow currently doesn't
        # seem to have full support.

        # For example: https://issues.apache.org/jira/browse/ARROW-12539
        # shows that casting a date string to pa.date32() throws an error.
        # $ pa.array(["2022-01-02"], pa.string()).cast(pa.date32())
        #   ArrowNotImplementedError: Unsupported cast from string to date32...
        # But
        # $ pa.array(["2022-01-02"], pa.string()).cast(pa.timestamp("s"))
        #   [2022-01-02 00:00:00]

        # For timestamps
        # $ pa.array(["2022-01-02T12:34:56.78"],pa.string()).cast(pa.timestamp("us"))
        #   [2022-01-02 12:34:56.780000]
        # But
        # $ pa.array(["2022-01-02T12:34:56.78Z"],pa.string()).cast(pa.timestamp("us"))
        #   ArrowInvalid: Failed to parse string....expected no zone offset
        # Yet
        # $ pa.array(["2022-01-02T12:34:56.78Z"], pa.string()).cast(pa.timestamp("us", "Z"))
        #   [2022-01-02 12:34:56.780000]

        # Unfortunately, s3 select returns timestamps as 2022-01-02T12:34:56.78Z
        # and that "Z" causes the ArrowInvalid error.
        try:
            table = table.cast(query_schema)
            self.logger.warning(
                "query_s3_select() pyarrow fixed their date/timestamp casting"
            )
        except Exception:
            df = table.to_pandas()
            for column, pa_type in zip(query_schema.names, query_schema.types):
                if pa.types.is_timestamp(pa_type) or pa.types.is_date(pa_type):
                    df[f"{column}"] = pd.to_datetime(df[f"{column}"])
            try:
                table = pa.Table.from_pandas(
                    df, schema=query_schema, preserve_index=False
                )
            except Exception as exc:
                self.logger.error(
                    "query_s3_select() Failed to cast back to original schema:"
                    + f"{exc}. Leaving date/timestamp as strings."
                )

        end = datetime.now()

        n_queries = len(cluster_column_values)
        n_files = len(filepaths_to_cluster_values)
        elapsed_time = end - start
        self.logger.info(
            "query_s3_select() FINISHED "
            + f"{n_queries=:},"
            + f"{n_files=:},"
            + f"{n_records=:},"
            + f"{elapsed_time=:},"
            + f"{bytes_scanned=:},"
            + f"{bytes_processed=:},"
            + f"{bytes_returned=:}"
        )

        return table

    def query(
        self,
        cluster_column_values,
        optional_where_clauses: List[Tuple] = [],
        columns: List[str] = None,
        batch_size: int = 131_072,
        n_records_max: int = 2_000_000,
    ) -> pa.lib.Table:
        """
        Retrieve records from the parquet files using pyarrow's
        isin(cluster_column_values) in batches of batch_size. Stop returning results
        if n_records_max have already been returned.

        Parameters
        ----------
        cluster_column_values : Union[str, List]
            cluster_column value you want to match on. Provide one or a list of many
        optional_where_clauses : List[Tuple], optional
            List of optional columns to further restrict the parquet files to be
            queried. Each tuple is three values column_name,
            comparision operator [>=, >, =, ==, <, <=], value. This will be logically
            connected using "AND" between each in the list and with the
            cluster_column_values
        columns : List[str], optional
            The columns to return from parquet files. Default is None (all of them)
        batch_size : int, optional
            Retrieve batches of this size from the dataset. Lower this value to manage
            RAM if needed. Default is 128 * 1024, same as pyarrow.Dataset.to_batches()
        n_records_max : int, optional
            Once this many results have been exceed, stop retrieving results and return
            what has been retrieved so far. Thus, the Table.num_rows returned will be
            less than or equal to n_records_max + batch_size.

        Returns
        -------
        pa.lib.Table
        """
        start = datetime.now()
        if not isinstance(cluster_column_values, list):
            cluster_column_values = [cluster_column_values]

        batch_fields = []
        if columns is not None:
            for col_name in columns:
                batch_fields.append(self.metastore.arrow_schema.field(col_name))
            batch_schema = pa.schema(batch_fields)
        else:
            batch_schema = self.metastore.arrow_schema

        filepaths_to_cluster_values = self.metastore.query(
            cluster_column_values, optional_where_clauses
        )
        filepaths = list(filepaths_to_cluster_values.keys())

        # Construct the cluster_column values filter
        ds_filter = ds.field(self.metastore.cluster_column).isin(cluster_column_values)

        # Add in the optional_where_clauses
        for col_name, compare_op, value in optional_where_clauses:
            if compare_op == "==" or compare_op == "=":
                op_filter = ds.field(col_name) == value
            elif compare_op == "<":
                op_filter = ds.field(col_name) < value
            elif compare_op == "<=":
                op_filter = ds.field(col_name) <= value
            elif compare_op == ">":
                op_filter = ds.field(col_name) > value
            elif compare_op == ">=":
                op_filter = ds.field(col_name) >= value
            ds_filter = ds_filter & (op_filter)

        n_records = 0
        batches = []
        if filepaths:
            dataset = ds.dataset(
                filepaths, format="parquet", filesystem=self.filesystem
            )
            record_batches = dataset.to_batches(
                columns=columns,
                filter=ds_filter,
                batch_size=batch_size,
            )

            for record_batch in record_batches:
                if record_batch.num_rows > 0:
                    n_records += record_batch.num_rows
                    batches.append(record_batch)
                if n_records > n_records_max:
                    break

        end = datetime.now()

        n_queries = len(cluster_column_values)
        n_files = len(filepaths_to_cluster_values)
        elapsed_time = end - start
        self.logger.info(
            "query() FINISHED "
            + f"{n_queries=:},"
            + f"{n_files=:},"
            + f"{n_records=:},"
            + f"{elapsed_time=:}"
        )

        return pa.Table.from_batches(batches, schema=batch_schema)
