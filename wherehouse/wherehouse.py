# import mysql.connector as mysql
# import psycopg2
import boto3
from botocore.exceptions import ClientError
from concurrent.futures import as_completed, ThreadPoolExecutor
import json
import logging
import pyarrow as pa
from pyarrow import fs
import pyarrow.dataset as ds
from typing import Dict, List, Union

from wherehouse.metastore import Metastore


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

    def _construct_sql_statement(
        self, cluster_values: List, columns: List[str] = None, where_clause: str = None,
    ):
        if columns:
            proj_col = ""
            for i, col_name in enumerate(columns):
                if i == 0:
                    proj_col += f"s.{col_name}"
                else:
                    proj_col += f", s.{col_name}"
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
            FROM s3object s
            WHERE {where_statement}
        """

        return sql_statement

    def _select_worker(self, filepath, cluster_values, columns=None, where_clause=None):
        s3_client = boto3.session.Session("s3", region_name=self.file_system.region)
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
            logging.error(f"_select_worker threw :{exc}")
            return {"data": None, "error_msg": f"ERROR: {filepath} gave {exc}"}

        records = []
        for event in response["Payload"]:
            if "Records" in event:
                result = event["Records"]["Payload"].decode("utf-8")
                for s in result.split("\n"):
                    if s:
                        try:
                            record = json.loads(s)
                        except Exception as exc:
                            logging.error(f"Failed to json load {s}: {exc}")
                        else:
                            records.append(record)
            # TODO: Log the stats
            elif "Stats" in event:
                statsDetails = event["Stats"]["Details"]
                bytes_scanned = statsDetails["BytesScanned"]
                bytes_processed = statsDetails["BytesProcessed"]

        return records

    def query_s3_select(
        self,
        cluster_column_values,
        columns: List[str] = None,
        where_clause: str = None,
        n_max_results: int = 2000000,
        n_threads: int = 20,
    ):
        if not isinstance(self.file_system, fs.S3FileSystem):
            raise TypeError(f"file_system must be S3. You have {self.file_system}")

        if not isinstance(cluster_column_values, list):
            cluster_column_values = [cluster_column_values]

        filepaths_to_cluster_values = self.metastore.query(cluster_column_values)

        results = []
        with ThreadPoolExecutor(n_threads) as executor:
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
                    logging.ERROR(f"{filepath} threw {exc}")
                else:
                    results += data
                    if len(results) > n_max_results:
                        check = True
                        break
            while check:
                check = False
                for future in future_to_file:
                    if not future.done():
                        future.cancel()
                        check = True

        return results

    def query(
        self,
        cluster_column_values,
        columns: List[str] = None,
        batch_size: int = 131072,
        n_max_results: int = 2000000,
    ):
        """
        Retrieve records from the parquet files where
            "cluster_column == cluster_column_value"
        in batches of batch_size. Stop returning results if n_max_results have already
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
        n_max_results : int, optional
            Once this many results have been exceed, stop retrieving results and return
            what has been retrieved so far. Thus, the Table.num_rows returned will be
            less than or equal to n_max_results + batch_size.

        Returns
        -------
        pa.lib.Table
        """
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

        n_results = 0
        batches = []
        for record_batch in record_batches:
            if record_batch.num_rows > 0:
                n_results += record_batch.num_rows
                batches.append(record_batch)
            if n_results > n_max_results:
                break

        return pa.Table.from_batches(batches)
