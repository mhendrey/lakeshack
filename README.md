# Lakeshack

![lakeshack logo](/images/lakeshack_128.png)

A simplified data lakehouse, more of a data shack, optimized for retrieving filtered records
from Parquet files. Lakeshack gathers up the min/max values for specified columns from
each Parquet file and stores them into a database. When you want to query for a set of
records, it first checks the database to get the list of Parquet files that might have
the desired records, and then only queries those parquet files using either native
pyarrow or S3 Select.

## Installation
Lakeshack can be install using pip:

```pip install lakeshack```

## Basic Usage

```
from pyarrow import fs
import pyarrow.dataset as ds
from lakeshack.metastore import Metastore
from lakeshack.lakeshack import Lakeshack

s3_dir = "my_bucket/my_project/"
s3 = fs.S3FileSystem("us-iso-east-1")
dataset = ds.dataset(s3_dir, format="parquet", file_system=s3)

# Create a metastore backed by SQLite and add in parquet file metadata
metastore = Metastore("sqlite:///my_project.db", "my_table", dataset.schema, "id")
metastore.update(s3_dir, file_system=s3, n_workers=16)

# Create lakeshack
lakeshack = Lakeshack(metastore, file_system=s3)

# Query for id = "abc" using pyarrow.dataset
table_pa = lakeshack.query("abc")

# Query for id = "abc" using S3 Select using 20 workers in a threadpool
table_s3 = lakeshack.query_s3_select("abc", n_workers=20)
```

## Details
Let's assume there are parquet files in S3. In addition, that you have applied a
partitioning strategy to organize the data for faster querying. Most people will do
a "major" partitioning which simply splits the parquet files into an organized folder
structure in S3. For example, one such major partitioning that simplifies the ETL
process will use the "load" date, that is the date the data was processed.

But within each load date, you also want to do a "minor" partitioning (sometimes
also referred to as "clustering"). For example, you want to sort the records, across
all the parquet files, on the "id" field. In addition, you want to do this so that the
records for a given "id" are in one and only one of the parquet files in this load
date directory. For example, you might have the following files in s3 where the name
reflects the min/max values for the "id" field in that parquet file.

* my_bucket/my_project/2023/03/15/part-0000_3333.gz.parquet
* my_bucket/my_project/2023/03/15/part-3334_7777.gz.parquet
* my_bucket/my_project/2023/03/15/part-7777_bbbb.gz.parquet
* my_bucket/my_project/2023/03/15/part-bbbc_ffff.gz.parquet
* my_bucket/my_project/2023/03/16/part-0000_3333.gz.parquet
* my_bucket/my_project/2023/03/16/part-3334_7777.gz.parquet
* my_bucket/my_project/2023/03/16/part-7777_bbbb.gz.parquet
* my_bucket/my_project/2023/03/16/part-bbbc_ffff.gz.parquet

Now if these are added to the `Metastore`, then when you the query lakeshack for
id="1234" the metastore is first queried to see which files might have records with
id="1234". In this case the file path for 2023/03/15/part-0000_3333.gz.parquet and
2023/03/16/part-0000_3333.gz.parquet are returned. Then only these files will be
checked for the requested records, using either:

* pyarrow.dataset to filter the two parquet files for the desired records
* S3 select to filter the two parquet files for the desired records
