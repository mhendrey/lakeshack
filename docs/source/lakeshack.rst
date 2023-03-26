Lakeshack
=========

The Basics
----------
Let's assume that you have Parquet files in S3 that contains sales data with columns:

* customer_id (int)
* timestamp (datetime)
* item_id (int)
* store_id (int)
* price (float)


The data is stored in S3 with the following folder structure,

* sales_data/<YYYY>/<MM>/<DD>/


Within a given folder, we have the following files:

* part-000000-009999.parquet.gz
* part-010000-019999.parquet.gz
* part-020000-029999.parquet.gz
* ...
* part-990000-999999.parquet.gz


where the first number represents the minimum customer_id in that file and the second
number represents the maximum customer_id in that file (see Partitioning & Clustering
Strategy for more details).

Usage
-----

::

    from datetime import datetime
    from pyarrow import fs
    import pyarrow.dataset as ds
    from lakeshack.metastore import Metastore
    from lakeshack.lakeshack import Lakeshack

    s3_dir = "sales_data/"
    s3 = fs.S3FileSystem(region="us-iso-east-1")
    # Only need a small amount of data to specify the schema
    s3_day = "sales_data/2023/03/15/"
    dataset = ds.dataset(s3_day, format="parquet", filesystem=s3)

    # Create a metastore backed by SQLite
    metastore = Metastore(
        "sqlite:///sales.db",  # SQLAlchemy URL to database
        "sales_table",         # Table name in the database
        dataset.schema,        # pyarrow schema of the data
        "customer_id",         # Column name used for the cluster column
        "timestamp",           # Optional column that can also be used to filter
    )
    # Add in Parquet metadata
    metastore.update(
        s3_dir,
        filesystem=s3,
        n_workers=50,
    )

    # Create lakeshack
    lakeshack = Lakeshack(metastore, filesystem=s3)

    ## Query using native pyarrow.dataset using local machine
    ## Results returned as a pyarrow.Table

    # Query for customer_id = 55 using native pyarrow.dataset
    customer_id = 55
    query_result = lakeshack.query(customer_id)

    # Query for multiple customer_ids at once
    customers = [55, 12345]
    query_result = lakeshack.query(customers)

    # Query for customer_id, but only within a date range
    # and only get the customer_id, timestamp, and price columns
    min_ts = datetime(2022, 11, 15, 0, 0, 0)  # 2022-11-15T00:00:00
    max_ts = datetime(2023, 1, 1, 0, 0, 0)    # 2023-01-01T00:00:00
    query_result = lakeshack.query(
        customers,
        optional_where_clauses=[
            ("timestamp", ">=", min_ts),
            ("timestamp", "<", max_ts),
        ]
        columns=["customer_id", "timestamp", "price"],
    )

    ## Query using S3 Select
    query_result = lakeshack.query_s3_select(
        customers,
        optional_where_clauses=[
            ("timestamp", ">=", min_ts),
            ("timestamp", "<", max_ts),
        ]
        columns=["customer_id", "timestamp", "price"],
    )


Partitioning & Clustering Strategy
----------------------------------
The key to optimizing the performance of either a fully featured lakehouse or this
simple lakeshack is having a partitioning & clustering strategy that matches the main
querying pattern that will be utilized. A partitioning & clustering strategy specifies
how records should be arranged across a set of Parquet files. A partitioning &
clustering strategy has three parts which specify how:

1. Parquet files are split across a directory structure
2. Records are split across a set of Parquet files within a single directory
3. Records are ordered within a single Parquet file

The first is the partitioning part and the second and third combined are the clustering
part, which together form a complete partitioning & clustering strategy. Unfortunately,
people often only implement the partitioning part which leaves a lot of performance
optimization on the table.

Partitioning
^^^^^^^^^^^^
A good partitioning strategy which makes the ETL process simpler is to partition based
upon the date. This can either be the "load" date, i.e, when the data was collected or
processed or the "record" date. To keep the ETL process simple and more efficient, once
a directory has been written it should be immutable. This has several advantages

* Downstream users of the data don't need to be informed of any changes to a directory
* No need to rerun the clustering piece which can be expensive computationally

To illustrate this, let's imagine the following scenario that does not make the
partitions (aka directories) immutable. Let's assume that we pick the record date for
partitioning our Parquet files and that we choose to use a YYYY/mm/dd/ folder
structure. Today is 2023-03-25 and as the data streams in, we accumulate Parquet files
for today and at 12:15 AM on 2023-03-26 we process the just completed day.  We run a
Spark job that implements the clustering strategy which sorts the data by the
customer_id across all of the Parquet files for that day.

But now it is 2023-03-26 and some data with a record date of 2023-03-25 comes trickling
in.  So we need to put these few records back in the 2023/03/25/ partition, but where
to put the new data?  If we just put them in a new file, then they don't comport with
the clustering strategy which will affect the performance of your queries. You could
rerun the Spark job to redo the clustering strategy, but that is a lot of shuffling of
the data. Besides what happens if a few more records come in tomorrow?  You'll have to
redo that Spark job again. In addition, if data comes trickling in, you will need to
notify downstream consumers of the data that there are new records showing up in an old
partition. This can be done but requires a lot of coordination with downstream users.

Having immutable partitions prevents the need to reprocess and reduces coordination
with downstream users. It should be noted, that the lakehouses typically have tools for
handling these issues, but it increases the level of complexity for your data
engineering team.

Thus my recommondation is to partition by the load date to keep the ETL simple. If
users still want to query based upon record date that is still possible. Hopefully the
record date and load date are highly correlated. So even if the data comes in over a
few load date partitions, it just means that you end up querying across a few load dates
instead of just one.  So this does reduce the efficiency of your queries a bit, but
greatly simplifies the ETL. That is likely a trade-off that you may be happy to make.
If the data comes in spread over a month, then perhaps you decide that isn't a trade-off
you want and choose to use record date.

Clustering
^^^^^^^^^^
In addition to the partitioning strategy, a clustering strategy also needs to be
implemented. The clustering strategy specifies how the data will be ordered within a
given partition with the goal of needing to read the least amount of data as possible
from disk to retrieve the requested records. The column(s) that are used to cluster
the records should align with the most common query pattern.

For Parquet, the row group (just a chunk of records) is the basic unit of data that
gets operated upon. A Parquet file may have one or more row groups. There are a few
things to note about row groups that are important for the clustering strategy to work:

* The row group is the smallest chunk of data that can be read from disk by Parquet.
* Parquet stores metadata about each row group. In particular, the minimum and
  maximum value in the row group for each column is stored.
* When querying a Parquet file, the row group metadata is first checked to see if that
  particular row group needs to be read. If your query does not fall within the min/max
  for that row group, then the row group is skipped. This can lead to significant
  improvements in query performance.
* When using compression, Parquet compresses each column in each row group
  independently.

Given these facts, the ideal clustering will put all the records for a given
customer_id into as few row groups as possible and ideally in just one.  In addition,
there should be no overlap between one row group's minimum/maximum customer_id metadata
and the next. To do this, you want to sort the records within a given partition by the
customer_id across **ALL** the Parquet files in that partition.

To illustrate this, let's look at the following table:

.. list-table:: Parquet Row Group For a Given YYYY-mm-dd Partition
    :widths: 20 20 30 30
    :header-rows: 1

    * - Filename
      - Row Group
      - Minimum customer_id
      - Maximum customer_id
    * - 0.parquet
      - 0
      - 000
      - 012
    * -
      - 1
      - 012
      - 024
    * -
      - 2
      - 024
      - 036
    * -
      - 3
      - 036
      - 048
    * - 1.parquet
      - 0
      - 049
      - 060
    * -
      - 1
      - 060
      - 072
    * -
      - 2
      - 072
      - 084
    * -
      - 3
      - 084
      - 096
    * - 2.parquet
      - 0
      - 097
      - 108
    * -
      - 1
      - 108
      - 120
    * -
      - 2
      - 120
      - 132

Notice that we have no overlap between the Parquet files and that only the end points
overlap between the row groups. Given this, if we want to query for the records for
customer_id = 008, then only we only need to read filename=0.parquet, row_group=0
from disk. If we happen to need customer_id = 084, then we need to read
filename=1.parquet, row_group=2 & row_group=3. With this strategy, each partition will
have just one file that **might** have your records.

In Spark, this can be easily achieved with the following snippet of code:

::
    
    df = spark.read.parquet("sales_data/2023/03/15/")

    # Orders the data by customer_id and splits cleanly into n_files such that a given
    # customer_id appears in just one file. But does not order **within** a given file
    df = df.repartitionByRange(n_files, "customer_id")

    # To sort within a file
    df = df.sortWithinPartitions("customer_id")

By doing this we also can get a bonus effect. Because we have sorted the records by the
customer_id column, then we will get really good compression in that column by the fact
that the data is sorted within a given row group. Now, if the column used in the
clustering strategy is also highly correlated with the other columns, then those columns
will also get much better compression.  With better compression, that means fewer bytes
need to be read from disk when retrieving any given row group.

Metastore
---------
A good partitioning & clustering strategy can go a long way to speeding up querying
data from Parquet files. This will help if you use something like Spark or Trino as
a compute engine since both will take advantage of the Parquet metadata information to
skip processing unneeded row groups.  However, for large data sets with lots of files
and even more row groups, you will find that the vast majority of your querying time is
spent reading the Parquet metadata. So this is the genesis for gathering up the
Parquet metadata and storing it centrally; either in a database or at least in a
separate Parquet file. With the centralized metadata, a query will first check the
metadata to determine which subset of Parquet files may have the requested files and
only execute the query against this subset of files.

For Lakeshack, the Metastore class is used to specify the database and to add the
appropriate metadata to the database. The Metastore class utilizes SQLAlchemy in order
to provide greater flexibility on the backend database that may be used.

Initialize
^^^^^^^^^^
To initialize a Metastore, you provide the SQLAlchemy URL that will be used to
establish a connection to the backend database and create the specified table. For this
example, let's create use SQLite as the backend database.

::

    from lakeshack.metastore import Metastore
    from pyarrow import fs
    import pyarrow.dataset as ds

    s3 = fs.S3FileSystem(region="us-iso-east-1")
    s3_dir = "sales_data/2023/"
    # We only need to get the pyarrow schema, so let's use a small bit of data
    dataset = ds.dataset(f"{s3_dir}03/15/", format="parquet", filesystem=s3)

    metastore = Metastore(
        "sqlite:///sales.db",   # Use file sales.db to store data
        "sales",                # Table name in sales.db
        dataset.schema,         # pyarrow schema
        "customer_id",          # Column that was used to cluster the data
        "timestamp",            # Optional column whose metadata we want in Metastore
    )

Metastore will create the table and map the Parquet's pyarrow schema to database types.
This will create the "sales" table in the "sales.db" SQLite database. In this example,
the customer_id was used to cluster the data within a partition. You need to specify
this column and this column must be used whenever you want to query the data. We also
passed in the "timestamp" field as an optional column. This tells Metastore to also
gather up the min/max values for this column and store them into the Metastore. This
allows you to optionally filter the data using this field.

The structure of the "sales" table looks like the following with "filepath" always
being the column that gives you the location of the file:

.. list-table:: sales table
    :widths: 20 20 20 20 20
    :header-rows: 0

    * - filepath
      - customer_id_min
      - customer_id_max
      - timestamp_min
      - timestamp_max

Update
^^^^^^
To add Parquet file metadata to the Metastore database, simply call the :code:`update()`
which will use a thread pool of workers to get the min/max values for the specified
column(s) (customer_id & timestamp in this example) for each Parquet file. Notice that
the min/max values are gathered at the Parquet file level and not for each row group.
This minimizes the number of rows in the Metastore database to just one entry for
each file.  Besides, the querying engines will check the row group information when
the query gets executed so no need to do that explicitly. In the code below, all the
data for 2023 will be added to the Metastore.

::

    metastore.update(
        s3_dir,            # Recursively add data from this directory
        filesystem = s3,   # File system holding the files
        n_workers = 50,    # Size of the thread pool to use
    )

Query
^^^^^
Finally, if you want to see what files **might** contain your requested data you
simply use the :code:`query()` function. There is little need for you to call this
method explicitly (it gets called by :code:`Lakeshack`) when needed, but it may be
useful to check how good your clustering performed. The method returns a dictionary
whose keys are the filepaths and whose values are which customer_id values might have
records in that file based upon the specified query. The example below is looking for
records for customer_id 0, 1, and 55123 that occurred on the first day of January 2023.

::

    from datetime import datetime
    filepath_dict = metastore.query(
        cluster_column_values = [0, 1, 55123],
        optional_where_clauses = [
            ("timestamp", ">=", datetime(2023, 1, 1, 0, 0, 0)),
            ("timestamp", "<" , datetime(2023, 2, 1, 0, 0, 0)),
        ],
    )
    print(filepath_dict)
    # {
    #   "sales_data/2023/01/01/part-000000-009999.gz.parquet": [0, 1],
    #   "sales_data/2023/01/01/part-055000-059999.gz.parquet": [55123],
    # }

Lakeshack
---------
With a :code:`Metastore` instantiated and metadata data added to it, we are ready to
instantiate a :code:`Lakeshack` to be able to retrieve records from the Parquet files.

Initialize
^^^^^^^^^^
We initialize a :code:`Lakeshack` by giving it the corresponding :code:`Metastore` and
the filesystem where the Parquet files are located.

::

    from lakeshack.lakeshack import Lakeshack

    lakeshack = Lakeshack(metastore, s3)

Query
^^^^^
The :code:`query()` will use pyarrow.dataset to query the underlying Parquet files.
Again this will be begin by querying the :code:`Metastore` to determine which Parquet
files **might** have the requested data. In the example that we have been working, we
should expect that for a given customer_id there will be one file in each of the
YYYY/mm/dd partitions.  This list of Parquet files will be used to instantiate a
pyarrow.dataset. This dataset will then process the data in batches until either all
the data has been returned or :code:`n_records_max` has been reached. The results
are returned as a pyarrow Table.

To further restrict the records that come back, you may pass in optional "where"
clauses that take advantage of any optional columns us put into the :code:`Metastore`.
In this example, we will further restric the query by giving a "timestamp" range 
that we want. In addition, since we are only interested in the price information,
let's pull back just the columns used to filter (customer_id & timestamp) and the price
column. It is always best practice to pull the least amount of data as needed.

::

    # Query for customer_id = 55 during January 2023 and only interested in the
    # pricing information
    pa_table = lakeshack.query(
        55,
        optional_where_clauses = [
            ("timestamp", ">=", datetime(2023, 1, 1, 0, 0, 0)),
            ("timestamp", "<" , datetime(2023, 2, 1, 0, 0, 0)),
        ],
        columns = ["customer_id", "timestamp", "price"],
    )
    # Now we can calculate some statistics for this customer
    df = pa_table.to_pandas()
    print(df.price.mean(), df.price.sum())

Query S3 Select
^^^^^^^^^^^^^^^
We can do the exact same query, but this time instead of using the local machine to
do the query, let's utilize S3 Select to do the work for you. S3 Select, like all of
the other Spark, Trino, lakehouses, etc., takes advantage of the Parquet metadata to
only retrieve the needed row groups. However, S3 Select only operates on a single
Parquet file at a time, but this isn't a problem for us!  We have already figured out
exactly which Parquet files we need to query thanks to the :code:`Metastore`.

Because S3 is doing the compute for you, we can use a thread pool to launch multiple
S3 Select queries in parallel which will speed things. This means that we can query
a huge amount of data without using **ANY** Spark/Trino cluster!

**NOTE** when using S3 Select, you pay per bytes scanned.  However, since we have
clustered our data, this means that we should be reading just :math:`1 + \epsilon`
row groups on average per partition per customer_id.

::

    pa_table = lakeshack.query_s3_select(
        55,
        optional_where_clauses = [
            ("timestamp", ">=", datetime(2023, 1, 1, 0, 0, 0)),
            ("timestamp", "<" , datetime(2023, 2, 1, 0, 0, 0)),
        ],
        columns = ["customer_id", "timestamp", "price"],
    )