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
import pyarrow.dataset as ds
from typing import List

from wherehouse.metastore import Metastore

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
            "filename": "wherehouse.log",
            "maxBytes": 10485760,
            "backupCount": 1,
        },
    },
    "loggers": {
        "wherehouse": {
            "level": "DEBUG",
            "handlers": ["console", "file"],
            "propagate": False,
        }
    },
}
logging.config.dictConfig(LOGGING_CONFIG)


class Wherehouse:
    """
    To speed up querying for records in Parquet files using a Metastore which contains
    Parquet file metadata to speed things up.
    """

    def __init__(self, metastore: Metastore, file_system: fs.FileSystem):
        """
        Instantiate a Wherehouse that will use the metastore & file_system to speed
        up retrieval of records from the parquet files stored in the file_system.

        Example:

        ```
        import pyarrow.parquet as pq
        from pyarrow import fs

        from wherehouse.metastore import Metastore

        pq_file = pq.ParquetFile("some.parquet")
        metastore = Metastore(
            {"database": "some.db"},
            "some_table",
            pq_file.schema_arrow,
            "id",
        )
        wherehouse = Wherehouse(metastore, fs.LocalFileSystem())
        ```

        Parameters
        ----------
        metastore : Metastore
            Metastore containing the parquet file metadata that wherehouse will use
        file_system : fs.FileSystem
            pyarrow file system. E.g., fs.S3FileSystem or fs.LocalSystem
        """
        self.metastore = metastore
        self.file_system = file_system
        self.logger = logging.getLogger("wherehouse")

    def _construct_sql_statement(
        self, cluster_values: List, columns: List[str] = None, where_clause: str = None,
    ):
        """
        Create a sql statement that can be given to s3's select_object_content().
        When doing column projections with Parquet as the InputSerialization, I've
        found that you need to have the column names in double quotes.

        Parameters
        ----------
        cluster_values : List
            List of cluster values that you want include in the WHERE clause. These
            will be set to equality and combined with "OR"
        columns : List[str]
            Specify the subset of columns to select. Default is None which is select *
        where_clause : str
            Any additional where clause that will be "AND" with cluster_values.
            Default is None
        
        Returns
        -------
        sql_statement : str
            SQL statement to pass to s3.select_object_content()
        """
        if columns:
            proj_col = ""
            for i, col_name in enumerate(columns):
                if i == 0:
                    proj_col += f'"{col_name}"'
                else:
                    proj_col += f', "{col_name}"'
        else:
            proj_col = "*"

        where_statement = "("
        for i, cluster_value in enumerate(cluster_values):
            if i == 0:
                where_statement += f"{self.metastore.cluster_column}='{cluster_value}'"
            else:
                where_statement += (
                    f" or {self.metastore.cluster_column}='{cluster_value}'"
                )
        where_statement += ")"

        if where_clause:
            where_statement += f" and ({where_clause})"

        sql_statement = f"""
            SELECT {proj_col}
            FROM s3object
            WHERE {where_statement}
        """

        return sql_statement

    def _select_worker(self, filepath, cluster_values, columns=None, where_clause=None):
        start = datetime.now()
        s3_client = boto3.session.Session().client(
            "s3", region_name=self.file_system.region
        )
        bucket = filepath.split("/")[0]
        key = "/".join(filepath.split("/")[1:])

        sql_statement = self._construct_sql_statement(
            cluster_values, columns, where_clause
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
            self.logger.error(f"_select_worker: {filepath} threw: {exc}")
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
                    self.logger.error(f"Failed to json load {s}: {exc}")
                else:
                    records.append(record)
        end = datetime.now()

        self.logger.debug(
            f"_select_worker: {filepath} took {end-start} for {len(records)} "
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
        columns: List[str] = None,
        where_clause: str = None,
        n_records_max: int = 2000000,
        n_workers: int = 20,
    ) -> pa.lib.Table:
        """
        Retrieve records from the parquet files stored in S3 using S3 Select

        Parameters
        ----------
        cluster_column_values : str, List[str]
            Provide a single cluster column value or a list of them whose
            records you want to retrieve
        columns : List[str]
            Specify the subset of columns to select. Default is None which is select *
        where_clause : str
            Any additional where clause that will be "AND" with cluster_values.
            Default is None
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
        if not isinstance(self.file_system, fs.S3FileSystem):
            raise TypeError(f"file_system must be S3. You have {self.file_system}")

        if not isinstance(cluster_column_values, list):
            cluster_column_values = [cluster_column_values]

        filepaths_to_cluster_values = self.metastore.query(cluster_column_values)

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
                    columns,
                    where_clause,
                )
                future_to_file[future] = filepath

            check = False
            for future in as_completed(future_to_file):
                filepath = future_to_file[future]
                try:
                    data = future.result()
                except Exception as exc:
                    self.logger.error(f"query_s3_select: {filepath} threw: {exc}")
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

        table = pa.Table.from_batches(batches)

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

        # Unfortunately, s3 select returns timestamps as 2022-01-02T12:34:56.78Z
        # and that "Z" causes the ArrowInvalid error.
        try:
            table = table.cast(query_schema)
            self.logger.warning(
                "query_s3_select-pyarrow fixed their date/timestamp casting"
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
                    "query_s3_select-Failed to cast back to original schema:"
                    + f"{exc}. Leaving date/timestamp as strings."
                )

        end = datetime.now()

        n_queries = len(cluster_column_values)
        n_files = len(filepaths_to_cluster_values)
        elapsed_time = end - start
        self.logger.info(
            "query_s3_select-FINISHED "
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
        columns: List[str] = None,
        batch_size: int = 131072,
        n_records_max: int = 2000000,
    ) -> pa.lib.Table:
        """
        Retrieve records from the parquet files where
            "cluster_column == cluster_column_value"
        in batches of batch_size. Stop returning results if n_records_max have already
        been returned

        Parameters
        ----------
        cluster_column_values : Union[str, List]
            cluster_column value you want to match on. Provide one or a list of many
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

        filepaths_to_cluster_values = self.metastore.query(cluster_column_values)
        filepaths = list(filepaths_to_cluster_values.keys())

        # Construct the cluster_column values filter. "or" the results
        ds_filter = ds.field(self.metastore.cluster_column) == cluster_column_values[0]
        for cluster_value in cluster_column_values[1:]:
            ds_filter = ds_filter | (
                ds.field(self.metastore.cluster_column) == cluster_value
            )

        dataset = ds.dataset(filepaths, format="parquet", filesystem=self.file_system)
        record_batches = dataset.to_batches(
            columns=columns, filter=ds_filter, batch_size=batch_size,
        )

        n_records = 0
        batches = []
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
            "query-FINISHED "
            + f"{n_queries=:},"
            + f"{n_files=:},"
            + f"{n_records=:},"
            + f"{elapsed_time=:}"
        )

        return pa.Table.from_batches(batches)
