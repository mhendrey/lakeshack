# import mysql.connector as mysql
# import psycopg2
import boto3
from botocore.exceptions import ClientError
from collections import defaultdict
from concurrent.futures import as_completed, ThreadPoolExecutor
import logging
import pyarrow as pa
from pyarrow import fs
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import sqlite3
from typing import Dict, List, Union


class Wherehouse:
    """
    Default Wherehouse class that uses a SQLite database as the backend storage of the
    parquet metadata
    """

    def __init__(
        self,
        file_system: fs.FileSystem,
        store_args: Dict,
        store_table: str,
        arrow_schema: pa.lib.Schema = None,
        cluster_column: str = None,
        *optional_columns,
    ):
        """
        Establish a connection to the wherehouse. If store_table already exists in the
        wherehouse then arrow_schema, cluster_column, and optional_columns are ignored.
        If the store_table does not already exist, then you must provide arrow_schema
        and cluster_column in order to create it. Any additional columns provided will
        also be created along with the cluster_column in store_table. For best
        performance the optional columns should also be reasonably well clustered.

        Example:

        ```
        import pyarrow.parquet as pd
        from pyarrow import fs

        pq_file = pyarrow.parquet.ParquetFile("some.parquet")
        wherehouse = Wherehouse(
            fs.LocalFileSystem(),
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
        file_system : fs.FileSystem
            pyarrow file system. E.g., fs.S3FileSystem or fs.LocalSystem
        store_args : Dict
            Dictionary arguments for connecting to the metastore.
        store_table : str
            Table name
        arrow_schema : pa.lib.Schema, optional
            Arrow schema of the parquet files that will be addes to the metastore
        cluster_column : str, optional
            Name of the column in the Arrow schema used for clustering the data
        *optional_columns : str,
            Optional column names to included in the metastore

        """
        self.file_system = file_system
        self.store_args = store_args
        self.store_table = store_table
        self.placeholder = "?"

        self.conn = self._get_store_conn()

        try:
            # store_table already exists in the metastore
            self.arrow_columns = self._get_arrow_columns()

            if arrow_schema is not None:
                print(
                    f"WARNING: Table {store_table} already exists. "
                    + "Ignoring arrow_schema"
                )
            if cluster_column is not None and self.arrow_columns[0] != cluster_column:
                print(f"WARNING: Using existing cluster_column {self.arrow_columns[0]}")
        except NameError:
            # store_table does not exist in the metastore. Create it
            self.conn.close()
            self.conn = self._get_store_conn()
            self.arrow_columns = [cluster_column] + list(optional_columns)

            # Quality checks
            if cluster_column is None or arrow_schema is None:
                raise TypeError(
                    "Must provide both arrow_schema & cluster_column "
                    + "when creating new metastore"
                )
            if cluster_column not in arrow_schema.names:
                raise ValueError(f"{cluster_column=:} is not listed in arrow_schema")
            for col_name in optional_columns:
                if col_name not in arrow_schema.names:
                    raise ValueError(
                        f"Optional column, {col_name}, is not listed in arrow_schema"
                    )

            self._create_table(arrow_schema)
            self._create_indices()

    def update(self, parquet_file_or_dir: str, n_threads: int = 16):
        """
        Add file level metadata to the metastore. If you provide a directory, then a
        recursive walk is done. Any non-parquet files are simply skipped and logged.

        Example:
        ```
        parquet_dir = "path/on/local/to/parquets/"
        metastore.update(parquet_dir)
        ```

        Parameters
        ----------
        parquet_file_or_dir : str
            Provide the filepath to either a single parquet file or a directory that
            contains many parquet files.
        n_threads : int, optional
            Size of the thread pool used to concurrently retrieve parquet file
            metadata. Default is 16
        """
        metadata = self._gather_metadata(parquet_file_or_dir, n_threads)

        if not metadata:
            print(f"No metadata found in {parquet_file_or_dir}")
            return None

        cur = self.conn.cursor()
        placeholders = ",".join([self.placeholder] * (1 + 2 * len(self.arrow_columns)))
        cur.executemany(
            f"INSERT INTO {self.store_table} VALUES({placeholders})", metadata
        )
        cur.close()
        self.conn.commit()

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
                where_statement += f"{self.arrow_columns[0]}='{cluster_value}'"
            else:
                where_statement += f" or {self.arrow_columns[0]}='{cluster_value}'"
        where_statement += ")"

        if where_clause:
            where_statement += f" and ({where_clause})"

        sql_statement = f"""
            SELECT {proj_col}
            FROM s3object s
            WHERE {where_statement}
        """

        return sql_statement

    def _select_worker(
        self, s3_client, filepath, cluster_values, columns=None, where_clause=None
    ):
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

        # I have no idea what "records" is.  Supposedly newline delimited
        # JSON, but no idea if this is giving me an array or just some nasty
        # string (which is what I suspect) with no idea how to transform that
        # until I see it.
        for event in response["Payload"]:
            if "Records" in event:
                records = event["Records"]["Payload"].decode("utf-8")
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

        s3 = boto3.client(region_name=self.file_system.region)

        filepaths_to_cluster_values = self.get_parquet_files(cluster_column_values)

        results = []
        with ThreadPoolExecutor(n_threads) as executor:
            future_to_file = {}
            for filepath, cluster_values in filepaths_to_cluster_values.items():
                future = executor.submit(
                    self._select_worker,
                    s3,
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

    def get_parquet_files(
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
        if other_column_values is not None:
            print("WARNING: other_column_values not implemented yet. Ignoring")

        pq_files = defaultdict(list)

        cur = self.conn.cursor()
        cluster_min = f"{self.arrow_columns[0]}_min"
        cluster_max = f"{self.arrow_columns[0]}_max"
        for cluster_column_value in cluster_column_values:
            cur.execute(
                f"""
                SELECT filepath
                FROM {self.store_table}
                WHERE {self.placeholder} >= {cluster_min}
                AND {self.placeholder} <= {cluster_max}
                ;
                """,
                (cluster_column_value, cluster_column_value),
            )
            for (filepath,) in cur.fetchall():
                pq_files[filepath].append(cluster_column_value)
        cur.close()
        return pq_files

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

        filepaths_to_cluster_values = self.get_parquet_files(cluster_column_values)
        filepaths = list(filepaths_to_cluster_values.keys())

        # Construct the cluster_column values filter. "or" the results
        ds_filter = ds.field(self.arrow_columns[0]) == cluster_column_values[0]
        for cluster_value in cluster_column_values[1:]:
            ds_filter = ds_filter | (ds.field(self.arrow_columns[0]) == cluster_value)

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

    def __del__(self):
        try:
            self.conn.close()
        except Exception:
            pass

    def _get_store_conn(self):
        """
        Internal method to return a connection to the metastore.
        Note: Subclasses need to override this method
        """
        return sqlite3.connect(**self.store_args)

    def _get_arrow_columns(self):
        """
        Internal method to return the column names corresponding to the Arrow schema
        Note: Subclasses need to override this method
        """
        cur = self.conn.cursor()
        cur.execute(f"SELECT name FROM pragma_table_info('{self.store_table}');")
        columns = []
        for (name,) in cur.fetchall():
            if name.endswith("_min"):
                columns.append(name[:-4])  # Remove the "_min"
        if not columns:
            raise NameError(f"Table '{self.store_table}' does not exist in metastore")

        return columns

    def _map_pa_type(self, pa_type: pa.DataType):
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
        # TODO: Add in date/time mappings

    def _create_table(self, arrow_schema: pa.lib.Schema):
        """
        Create the table, store_table, based upon the arrow schema and column names
        stored in arrow_columns

        Parameters
        ----------
        arrow_schema : pa.lib.Schema,
            Arrow schema of the parquet files

        Returns
        -------
        None
        """
        db_types = []
        for col in self.arrow_columns:
            pa_type = arrow_schema.field(col).type
            db_types.append(self._map_pa_type(pa_type))

        sql_str = f"""
            CREATE TABLE {self.store_table} (
                filepath TEXT PRIMARY KEY,
        """
        for col, db_type in zip(self.arrow_columns, db_types):
            sql_str += f"{col}_min {db_type},"
            sql_str += f"{col}_max {db_type},"
        sql_str = sql_str[:-1] + ");"  # Remove the last "," & close parenthesis

        cur = self.conn.cursor()
        cur.execute(sql_str)
        cur.close()
        self.conn.commit()

    def _create_indices(self):
        cur = self.conn.cursor()
        for col_name in self.arrow_columns:
            for ext in ["min", "max"]:
                index_name = f"{self.store_table}_{col_name}_{ext}_index"
                cur.execute(
                    f"CREATE INDEX {index_name} ON {self.store_table}"
                    + f"({col_name}_{ext});"
                )
        cur.close()
        self.conn.commit()

    def _get_min_max(self, filepath, column_idxs):
        try:
            metadata = pq.ParquetFile(
                self.file_system.open_input_file(filepath)
            ).metadata
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

    def _gather_metadata(self, parquet_dir, n_threads: int = 16):
        if self.file_system.get_file_info(parquet_dir).is_file:
            filepaths = [parquet_dir]
        else:
            file_selector = fs.FileSelector(parquet_dir, recursive=True)
            filepaths = [
                f.path
                for f in self.file_system.get_file_info(file_selector)
                if f.type == fs.FileType.File
            ]

        # Gather the mapping from column names to column idx in Parquet file
        arrow_columns_idx = []
        try:
            pq_file = pq.ParquetFile(self.file_system.open_input_file(filepaths[0]))
        except pa.ArrowException as exc:
            print(f"ERROR for {filepaths[0]}, {exc}")
            return []

        for column in self.arrow_columns:
            arrow_columns_idx.append(pq_file.schema_arrow.names.index(column))

        metadata = []
        with ThreadPoolExecutor(n_threads) as ex:
            future_to_path = {
                ex.submit(self._get_min_max, filepath, arrow_columns_idx): filepath
                for filepath in filepaths
            }
            for future in as_completed(future_to_path):
                filepath = future_to_path[future]
                try:
                    result = future.result()
                except Exception as exc:
                    print(f"ERROR: {filepath} threw error: {exc}")
                else:
                    data = result["data"]
                    if data:
                        metadata.append(data)
                    else:
                        print(result["error_msg"])

        return metadata
