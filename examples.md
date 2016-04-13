---
layout: article
title: 'RecordService Examples'
share: false
---

You can find the source code for RecordService examples in the RecordService Client GitHub repository.

[https://github.com/cloudera/RecordServiceClient/tree/master/java](https://github.com/cloudera/RecordServiceClient/tree/master/java) 

Instructions for running the examples are stored in the repository with the source code.

{% include toc.html %}

## Hadoop and MapReduce Examples

| Example | Description |
|:--------|:--------|
| RSCat | This example shows how you can output tabular data for any dataset readable by RecordService. It demonstrates a standalone Java application built on the core libraries without using a computation framework such as MapReduce or Spark. |
| SumQueryBenchmark | This demonstrates running a sum over a column and pushing the scan to RecordService. It shows how you can use RecordService to accelerate scan-intensive operations.
| Terasort | This is a port of the Hadoop [Terasort](https://hadoop.apache.org/docs/current/api/org/apache/hadoop/examples/terasort/package-summary.html) benchmark test ported to RecordService. See README in the Terasort package for more details. This also demonstrates how to implement a custom InputFormat using the RecordService APIs. |
| MapredColorCount / MapreduceAgeCount / MapReduceColorCount | These examples are ported from Apache Avro. They demonstrate the steps required to port an existing Avro-based MapReduce job to use RecordService. |
| RecordCount/WordCount | More MapReduce applications that demonstrate some other InputFormats included in the client library, including TextInputFormat and RecordServiceInputformat. Useful for existing MapReduce jobs already using TextInputFormat. |
|Reading data from a View, and Enforcing Sentry Permissions  (uses RecordCount) | This example demonstrates how an MR job can now read data even when the user only has permission to see part of the data in a file (table). See [https://github.com/cloudera/RecordServiceClient/tree/master/java/examples#how-to-enforce-sentry-permissions-with-mapreduce](https://github.com/cloudera/RecordServiceClient/tree/master/java/examples#how-to-enforce-sentry-permissions-with-mapreduce) |
| com.cloudera.recordservice.examples.avro | Unmodified from the Apache Avro examples, these utilities help you generate sample data. |

## RecordService Spark Examples

The following examples can be found at [https://github.com/cloudera/RecordServiceClient/tree/master/java/examples-spark](https://github.com/cloudera/RecordServiceClient/tree/master/java/examples-spark).

| Example | Description |
|:--------|:--------|
| Query1/Query2 | Examples that demonstrate RecordService native Resilient Distributed Dataset (RDD) integration using RecordServiceRDD. |
| WordCount | The Hadoop WordCount example built on top of the RecordService equivalent of `textFile()`. |
| SchemaRDDExample | An example of native RDD integration using SchemaRecordServiceRDD. |
| TeraChecksum | Uses `hadoopFile()` with RecordService InputFormats. This is a port of the TeraChecksum MapReduce job, written in Spark.|
| TpcdsBenchmark | Demonstrates the SparkSQL integration running a portion of the tpcds benchmark. |
| DataFrameExample | Demonstrates DataFrames and RecordService working together. |
| How to use RecordService with Spark shell | Examples of using spark-shell to interact with RecordService in a variety of ways.|
| Reading Data from a View and Enforcing Sentry Permissions | Demonstrates how a MapReduce job can read data even when the user only has permission to see part of the data in a file (table). See [ReadMe.md](https://github.com/cloudera/RecordServiceClient/blob/master/java/examples-spark/README.md#how-to-enforce-sentry-permissions-with-spark) |

## Using HCatalog and Pig with RecordService

RecordService supports applications that use HCatalog and Pig. The supporting source code is available on [GitHub](http://github.mtv.cloudera.com/CDH/RecordServiceClient/tree/master/java/hcatalog-pig-adapter).

For example, consider the following script in Pig:

```bash
A = LOAD 'tpch.nation' USING org.apache.hive.hcatalog.pig.HCatLoader();
DUMP A;
```

To enable RecordService, you add a few lines in the script:

```bash
register /path/to/recordservice-hcatalog-pig-adapter-${version}-jar-with-dependencies.jar
set recordservice.planner.hostports <planner-hostports>
A = LOAD 'tpch.nation' USING com.cloudera.recordservice.pig.HCatRSLoader();
DUMP A;
```

Note the changes:

1. Register the JAR file that contains all the classes required to use HCatalog/Pig with RecordService, including the `HCatRSLoader` class.
1. Specify the address for RecordService planners by setting the `recordservice.planner.hostports` property or by setting the
   `recordservice.zookeeper.connectString` to use the planner auto discovery feature.
1. Replace `HCatLoader` with the `HCatRSLoader`.

Specify any additional RecordService properties using the `set` command.

For example, in a cluster with Kerberos, you can set the Kerberos principal with the command:

`set recordservice.kerberos.principal ${primary}/_HOST@${REALM}`

RecordService also supports column projection:

```bash
register /path/to/recordservice-hcatalog-pig-adapter-${version}-jar-with-dependencies.jar
set recordservice.planner.hostports <planner-hostports>
A = LOAD 'select n_nationkey, n_name from tpch.nation' USING com.cloudera.recordservice.pig.HCatRSLoader();
DUMP A;
```

This selects only the columns `n_nationkey` and `n_name` from the `tpch.nation` table. As with MapReduce or Spark, you can use Sentry privileges to enforce restrictions on data that also apply to HCatalog and Pig jobs.