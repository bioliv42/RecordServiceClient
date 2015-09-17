This repo contains Spark examples of applications built using RecordService client APIs.

- `Query1`/`Query2`: simple examples demonstrate how to use `RecordServiceRDD` to query
  a Parquet table.

- `WordCount`: a Spark application that counts the words in a directory. This demonstrates
  how to use the `SparkContext.recordServiceTextfile`.

- `SchemaRDDExample`: a example demonstrate how to use `RecordServiceSchemaRDD` to query tables.

- `TeraChecksum`: a terasort example that ported to Spark. It can be run with or without
  RecordService.

- `TpcdsBenchmark`: driver that can be used to run [TPC-DS](http://www.tpc.org/tpcds/) benchmark
  queries. It shows how to use SparkSQL and RecordService to execute the queries.

- `DataFrameExample`: a simple example demonstrate how to use DataFrame with RecordService by
  setting the data source.


