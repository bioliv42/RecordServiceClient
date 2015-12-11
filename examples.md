---
layout: article
title: 'RecordService Examples'
share: false
---

You can find the source code for RecordService examples in the RecordService Client GitHub repository.

[https://github.com/cloudera/RecordServiceClient/tree/master/java/examples](https://github.com/cloudera/RecordServiceClient/tree/master/java/examples) 

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
| Query1/Query2 | Examples that demonstrate RecordService native RDD Resilient Distributed Dataset (RDD) integration using RecordServiceRDD. |
| WordCount | The Hadoop WordCount example built on top of the RecordService equivalent of textFile(). |
| SchemaRDDExample | Another example of the native RDD integration, this time using SchemaRecordServiceRDD. |
| TeraChecksum | This example uses hadoopFile() with the RecordService InputFormats. This is a port of the TeraChecksum MapReduce job, written in Spark.|
| TpcdsBenchmark | This demonstrates the SparkSQL integration running a portion of the tpcds benchmark. |
| DataFrameExample | An example that demonstrates DataFrames and RecordService working together. |
| How to use RecordService with Spark shell | Examples of using spark-shell to interact with RecordService in a variety of ways.|
| Reading Data from a View and Enforcing Sentry Permissions | This example demonstrates how an MR job may now read data even when the user only has permission to see part of the data in a file (table). See [ReadMe.md](https://github.com/cloudera/RecordServiceClient/blob/master/java/examples-spark/README.md#how-to-enforce-sentry-permissions-with-spark) |

