# import mysql.connector as mysql
# import psycopg2
from concurrent.futures import as_completed, ThreadPoolExecutor
import pyarrow as pa
from pyarrow import fs
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import sqlite3
from typing import Dict, List, Tuple


class Metastore:
    """
    Default Metastore class that uses a SQLite database as the metastore
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
        Establish a connection to the metastore. If store_table already exists in the
        metastore then arrow_schema, cluster_column, and optional_columns are ignored.
        If the store_table does not already exist, then you must provide arrow_schema
        and cluster_column in order to create it. Any additional columns provided will
        also be created along with the cluster_column in store_table. For best
        performance the optional columns should also be reasonably well clustered.

        Example:

        ```
        pq_file = pyarrow.parquet.ParquetFile("some.parquet")
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
        Add file level metadata to the metastore. If you provide a dirctory, then a
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

    def query(
        self,
        cluster_column_value,
        batch_size: int = 131072,
        n_max_results: int = 2000000,
    ):
        cur = self.conn.cursor()
        cluster_min = f"{self.arrow_columns[0]}_min"
        cluster_max = f"{self.arrow_columns[0]}_max"
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
        filepaths = [f[0] for f in cur.fetchall()]
        cur.close()

        dataset = ds.dataset(filepaths, format="parquet", filesystem=self.file_system)
        record_batches = dataset.to_batches(
            filter=ds.field(self.arrow_columns[0]) == cluster_column_value,
            batch_size=batch_size,
        )

        n_results = 0
        for record_batch in record_batches:
            if record_batch.num_rows > 0:
                n_results += record_batch.num_rows
                yield pa.Table.from_batches([record_batch])
            if n_results > n_max_results:
                return None

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
        if pa_type == pa.large_string() or pa_type == pa.string():
            return "TEXT"
        elif pa_type in [
            pa.int64(),
            pa.int32(),
            pa.int16(),
            pa.int8(),
            pa.uint64(),
            pa.uint32(),
            pa.uint16(),
            pa.uint8(),
        ]:
            return "INTEGER"
        elif pa_type in [pa.float64(), pa.float32(), pa.float64()]:
            return "REAL"
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

    @staticmethod
    def _get_min_max(filepath, column_idxs, file_system):
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

    def _gather_metadata(self, parquet_dir, n_threads: int = 16):
        if self.file_system.get_file_info(parquet_dir).is_file:
            filepaths = [parquet_dir]
        else:
            file_selector = fs.FileSelector(parquet_dir, recursive=True)
            filepaths = [f.path for f in self.file_system.get_file_info(file_selector)]

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
                ex.submit(
                    Metastore._get_min_max,
                    filepath,
                    arrow_columns_idx,
                    self.file_system,
                ): filepath
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

