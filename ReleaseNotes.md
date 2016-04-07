---
layout: article
title: 'RecordService Beta 0.3.0 Release Notes'
share: false
---
This is the documentation for RecordService Beta 0.3.0.

For RecordService Beta 0.2.0 documentation, see [RecordService_0.2.0.pdf]({{site.baseurl}}/RecordService_0.2.0.pdf).

For RecordService Beta 0.1.0 documentation, see [RecordService_0.1.0.pdf]({{site.baseurl}}/RecordService_0.1.0.pdf).

This release of RecordService is a public beta and should not be run on production clusters. During the public beta period, RecordService is supported through the mailing list <a href="mailto:RecordService-user@googlegroups.com">RecordService-user@googlegroups.com</a>, not through the Cloudera Support Portal.

As you use RecordService during the public beta period, keep in mind the following:

* The RecordService team responds to beta issues as quickly as possible, but cannot commit to issue-resolution or bug-fix delivery times during the public beta period.

* There is no guarantee that a bug will be fixed in a future release.

* The RecordService team does not provide patches for beta releases, and cannot guarantee upgrades from this release to later releases.

* Although multiple releases of beta code might be planned, the contents are not guaranteed. There is no schedule for future beta code releases. Any releases are announced to the user group as they occur.

{% include toc.html %}

## New Features

### New Features in RecordService Beta 0.3.0

* **Planner Auto Discovery**: You can now use the recordservice.zookeeper.connectString property to specify planner/worker membership and other information. See [Configuring RecordService: Planner Auto Discovery Configuration]({{site.baseurl}}/rsConfig/#planner-auto-discovery-configuration)

* **Dynamic Fetch Size Adjustment**: RecordService can now automatically adjust fetch size according to available capacity and workloads on the RecordServiceWorker. This allows for optimal memory management. This helps multi-tenancy workloads to succeed when resources are under contention. RecordService can scale performance up or down based on resource availability. Further tuning is available through use of the new properties `rs_adjust_fetch_size`, `rs_compressed_max_fetch_size`, `rs_compressed_max_fetch_size`, `rs_fetch_size_increase_factor`, `rs_min_fetch_size`, and `rs_spare_capacity_correction_factor`. See [Configuring RecordService]({{site.baseurl}}/rsConfig/#dynamic-fetch-size-adjustment).

* **CSD User Experience Improvements for Sentry Configuration**: The name of the Sentry configuration field has change from *Configuration Snippet (Safety Valve) for sentry-site.xml* to *Sentry Advanced Configuration Snippet (Safety Valve)*. See [Sentry Table Configuration]({{site.baseurl}}/rsConfig/#sentry-table-configuration).

* **HCatalog Support**: RecordService supports the use of HCatalog and Pig. See [Using HCatalog and Pig with RecordService]({{site.baseurl}}/examples/#using-hcatalog-and-pig-with-recordservice).

### New Features in RecordService Beta 0.2.0

* Support for CDH 5.5, including:
    * Sentry Column-Level Authorization.
    * Spark 1.5.
* CSD user experience improvements for Spark and Sentry configuration.
* Performance improvements for loading metadata.

## Fixed Issues

### Issues Fixed in RecordService Beta 0.3.0

* Short circuit reads not enabled 
    * **Bug:** [RS-114](https://issues.cloudera.org/browse/RS-114)
    * Short circuit reads are enabled by default in RecordService 0.3.0.

### Issues Fixed in RecordService Beta 0.2.0
* Fix support for multiple planners with path requests.
* Path requests do not contain the connected user in some cases, causing requests to fail with authorization errors.
* `SpecificMutableRow Exception` while running spark-shell with RecordService.
* Port conflict when two `recordserviceds` are running on the same host.
* Update `task_size` to use total bytes of scan ranges.
* Fail plan request when worker membership is empty.

## RecordService VM Requirements

RecordService VM requires VirtualBox version 4.3 or 5. You can download a free copy of VirtualBox at [https://www.virtualbox.org/wiki/Downloads](https://www.virtualbox.org/wiki/Downloads).

## Platform and Hardware Support

RecordService supports the following software and hardware configurations when running on your own Hadoop cluster:

* CDH 5.4 and higher
* Server support: RHEL5 and RHEL6, Ubuntu LTS, SLES, and Debian
* Intel Nehalem (or later) or AMD  Bulldozer (or later) processor
* 64 GB memory
* For optimal performance, run with 12 or more disks, or use SSD.

## Storage and File Format Support

RecordService supports reading HDFS or S3 of the following file formats:

* Parquet 
* Text
* Sequence file
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

**Error creating zk znode path for planner/worker membership**

Bug: [RS-121](https://issues.cloudera.org/browse/RS-121).

To take advantage of security improvements in Beta 0.3.0, Cloudera recommends that you delete existing ZooKeeper znodes when you stop RecordService.

**Workaround**

To delete ZooKeeper nodes:
<ol>
<li>Create a <i>jaas.conf</i> file with the following content (the principal username is <i>impala</i> at the moment, but might change to <i>recordservice</i> in the future.)

<pre>
Client {
  com.sun.security.auth.module.Krb5LoginModule required
  useKeyTab=true
  keyTab="/path/to/record_service.keytab"
  storeKey=true
  useTicketCache=false
  principal="impala/hostname@realm";
};
</pre>
</li>
<li>
Set the environment variable <i>CLIENT_JVMFLAGS</i>:

<pre>
export JVMFLAGS="-Djava.security.auth.login.config=/path/to/jaas.conf"
</pre>
</li>
<li>
Launch the ZooKeeper client on the leader node, and specify the leader node in the <i>-server</i> parameter. Note: this does not work on follower nodes.

<pre>
zookeeper-client -server <i>leader-node-hostname</i>
</pre>
</li>
<li>
In ZooKeeper console, delete the old ZooKeeper nodes
<i>rmr /recordservice</i>.
The user who launches the ZooKeeper client must have the right permissions to  <i>jaas.conf</i> and <i>keytab</i>.
</li>
</ol>

**Saving machine state and restarting the VM can result in no registered workers**

After restarting the VM from a saved state, you might receive the following message when attempting to run RecordService applications.

<pre>
Exception in thread "main" java.io.IOException: 
com.cloudera.recordservice.core.RecordServiceException: 
TRecordServiceException(code:INVALID_REQUEST, message:Worker membership is 
empty. Please ensure all RecordService Worker nodes are running.
</pre>

You can verify that the membership is 0 by looking at `http://quickstart.cloudera:11050/membership`.

**Workaround**

Restart RecordService by running the following command on the VM: 

```
sudo service recordservice-server restart
```

**RecordService client configurations are not properly propagated to Spark jobs**

RecordService configuration options are not propagated to Spark jobs using the RecordService custom service descriptor (CSD). All configuration options must be specified in the job or through Cloudera Manager safety valves for Spark.

**Workaround**

* Apply configuration options using the Spark configuration safety valve: 
    **Spark** -> **Configuration** -> **Spark (Standalone) Client Advanced Configuration Snippet (Safety Valve) for spark-conf/spark-defaults.con**

```
spark.recordservice.planner.hostports=<comma separated list of planner host:ports>
```

* If the cluster is Kerberized, also set:

```
spark.recordservice.kerberos.principal=<Kerberos principal> 
```

* Save changes and deploy the client configuration.

A properties file is generated in `/etc/recordservice/conf`. 
When you run a Spark job, add the following instruction to your command: 

`--properties-file=/etc/recordservice/conf/spark.conf`

**digest-md5 library not installed on cluster, breaking delegation tokens**

The digest-md5 library is not installed by default in parcel deployments.

**Workaround**

To install the library on RHEL 6, use the following command-line instruction:

```
sudo yum install cyrus-sasl-md5
```

## Limitations

**Security Limitations**

* RecordService only supports simple single-table views (no joins or aggregations).
* SSL support has not been tested.
* Oozie integration has not been tested.

**Storage/File Format Limitations**

* No support for write path.
* Unable to read from Kudu or HBase.

**Operation and Administration Limitations**

* No diagnostic bundle support.
* No metrics available in Cloudera Manager.

**Application Integration Limitations**

* Spark DataFrame is not well tested.
