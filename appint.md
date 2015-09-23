---
layout: page
title: 'Application Integration'
---

## MapReduce Input Formats

RecordService is designed to integrate well with existing MapReduce InputFormats. The RecordService client contains InputFormats that are common for most applications. They implement the standard MR1 and MR2 InputFormat API, and should be usable by any existing application or library that expects InputFormats. The examples also contain cases of custom InputFormats that can be easily built on top of the lower level RecordService client APIs.
Unlike many standard Hadoop InputFormats, the RecordService format is independent of the underlying storage format. For example, you can use the AvroInputFormat even if the underlying data is a CSV file. The InputFormat specifies the Java object model used by the application (for example, Avro objects when using AvroInputFormat, hadoop.io.Text when using TextInputFormat, and so on). 

## Drop-in Replacements for TextInputFormat and AvroInputFormat

To assist with application migration, RecordService provides two InputFormats that closely mimic the API of their existing counterparts. There is an implementation of TextInputFormat (identical class name but in our package as Hadoop’s TextInputFormat) and also variants of the Avro InputFormats (again, identical class name but in the RecordService package). These are drop-in replacements. As demonstrated in some of the examples, you can migrate applications by changing the package names for those classes.

## Spark RDD

RecordService integrates with Spark in several ways. You can use the InputFormats discussed above through the Spark APIs (SparkContext.hadoopFile()). In some cases, RecordService achieves native Spark integration by implementing the RDD interface directly (as opposed to the InputFormat approach which implements a MapReduce API that Spark knows how to use). The SchemaRDDExample in examples-spark demonstrates this.

## Spark SQL Support and DataFrames

RecordService integrates with SparkSQL (and by extension DataFrames) by implementing the SparkSQL DataSources API. RecordService supports projection and predicate push down.