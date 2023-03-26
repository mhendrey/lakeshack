Lakeshack
=========
.. image:: images/lakeshack_128.png
    :align: center
    :alt: A small rustic shack on the shores of a big lake
.. image:: ../../images/lakeshack_128.png
    :align: center
    :alt: A small rustic shack on the shores of a big lake

====================

A simplified data lakehouse, more of a data lakeshack, optimized for retrieving
filtered records from Parquet files. Similar to the various lakehouse solutions
(Iceberg, Hudi, Delta Lake), Lakeshack gathers up the min/max values for specified
columns from each Parquet file and stores them into a database (Metastore). When you
want to query for a set of records, it first check the Metastore to get the list of
Parquet files that **might** have the desired records, and then only queries those
Parquet files. The files may be stored locally or in S3. You may query using either
native pyarrow or leverage S3 Select.

To achieve optimal performance, a partitioning strategy for how records are to be
written to the Parquet files must align with the main query pattern must be
implemented.  See the documentation for more information on this.

Installation
============
Lakeshack may be install using pip::

    pip install lakeshack

Documentation
=============
Documentation can be found at https://mhendrey.github.io/lakeshack
