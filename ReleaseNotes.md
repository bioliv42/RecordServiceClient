---
layout: page
title: 'RecordService Beta Release Notes'
---

RecordService provides an abstraction layer between compute frameworks and data storage. It provides row- and column-level security, and other advantages as well.

## Platform/Hardware Support

RecordService supports the following software and hardware configurations:
* CDH 5.4
* Server support: RHEL5-7, Ubuntu LTS, SLES, Debian
* Intel Nehalem (or later) or AMD  Bulldozer (or later) processor
* 64GB memory
* For optimal performance, run with 12 or more disks, or use SSD.

## Storage/File Format Support

RecordService supports reading HDFS or S3 of file format:
* Parquet
* Text
* Sequence File
* RC
* Avro

## Data Type Support
RecordService supports the following data types:
* INT (8-64 bits)
* CHAR/VARCHAR
* BOOL
* FLOAT
* DOUBLE
* DECIMAL
* STRING
* TIMESTAMP

RecordService does not support the following data types:
* BLOB/CLOB
* Nested Types


## Known Issues

### digest-md5 library not installed on cluster, breaking delegation tokens

The digest-md5 library is not installed by default.

#### Workaround
To install the library on RHEL6 use the following command line instruction:

```
sudo yum install cyrus-sasl-md5
```

### Short circuit reads not enabled

#### Workaround

In Cloudera Manager, open the HDFS configuration page and search for _shortcircuit_. There are two configurations named **Enable HDFS Short Circuit Read**. One defaults to _true_ and one to _false_. Set both values to _true_.

### Path requests do not work with multiple planners

The RecordService planner creates a temporary table. The name of the table collides between RecordService planners. On a single planner, it is properly protected by a lock. On the Hive Metastore Server, collisions are likely to occur.

#### Workaround

For the beta release, run only one instance of the RecordService planner.

## Limitations

### Security Limitations

* Only supports simple single-table views (no joins or aggregations).
* SSL support has not been tested.
* Oozie integration has not been tested.

### Storage/File Format Limitations

* No support for write path.
* Unable to read from Kudu or HBase.

### Operation and Administration Limitations

* No diagnostic bundle support.
* No metrics available in CM.

### Application Integration Limitations

* Spark DataFrame not well tested.


[overview]: {{site.baseurl}}/

