---
layout: article
title: 'Installing RecordService on Your CDH Cluster'
category: install
share: false
---

Use these instructions to install RecordService in an existing Hadoop cluster. You can also use the RecordService VM if you want to quickly install and explore the features of RecordService.

See [Download and Install RecordService VM]({{site.baseurl}}/vm.html).

{% include toc.html %}

## Download RecordService

Use these instructions to install RecordService. Cloudera recommends that you install Cloudera Manager on your cluster, so that you can download and install RecordService as an add-on service in Cloudera Manager.

## Installing RecordService as an Add-on Service in Cloudera Manager

You can install RecordService as an [Add-on Service](http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/cm_mc_addon_services.html) in Cloudera Manager. First, install both the custom service descriptor (CSD) and the parcel in Cloudera Manager; then, add RecordService as an add-on service from the home page in Cloudera Manager.
See [http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/cm_mc_addon_services.html](http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/cm_mc_addon_services.html).

### Installing the RecordService CSD

Follow these steps to install the RecordService CSD.

1. Download the CSD from [http://archive.cloudera.com/beta/recordservice/csd](http://archive.cloudera.com/beta/recordservice/csd).
1. Upload the CSD to `/opt/cloudera/csd` in the Cloudera Manager server.
1. Change the owner and group for the JAR using the following comamand-line instruction:<br/>
```
chown cloudera-scm:cloudera-scm /opt/cloudera/csd/RECORD_SERVICE-0.2.0.jar
```
1. Update the permissions on the file using the following command-line instruction:<br/>
```
chmod 644 /opt/cloudera/csd/RECORD_SERVICE-0.2.0.jar
```
1. Restart the Cloudera Manager server:
    1. As the root user on the Cloudera Manager server, run `service cloudera-scm-server restart`.
    1. Log in to the Cloudera Manager Admin Console and restart the Cloudera Manager Service.
1. Check whether the CSD successfully installed in `http://{cm-server}:7180/cmf/csd/refresh`. Search for the following entry:

<pre>
{ csdName: "RECORD_SERVICE-0.2.0",
  serviceType: "RECORD_SERVICE",
  source: "/opt/cloudera/csd/RECORD_SERVICE-0.2.0.jar",
  isInstalled: true
}
</pre>

See [http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/cm_mc_addon_services.html](http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/cm_mc_addon_services.html).

### Installing RecordService as a Parcel

Follow these steps to install the RecordService Parcel.

1. Go to `http://{cm-server}:7180/cmf/parcel/status`.
1. If there is no RecordService parcel listed on the status page, click **Edit Settings** and add the RecordService repository URL `http://archive.cloudera.com/beta/recordservice/parcels/latest` to **Remote Parcel Repository URLs**.
1. **Download** the RecordService parcel.
1. **Distribute** the parcel.
1. **Activate** the parcel. Cloudera Manager asks you to restart the entire cluster, but you only need to start/restart RecordService.

See [http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/cm_ig_parcels.html](http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/cm_ig_parcels.html)

### Deploying RecordService Using Cloudera Manager

Follow these steps to start RecordService for a cluster from Cloudera Manager.

1. Add a service from the Cloudera Manager home page.
1. Customize role: 
    * RecordService Planner and Worker: Select hosts with both the role of DN and NN.
    * RecordService Planner: Select hosts with the role of NN.
    * RecordService Worker: Select hosts with the role of DN.<br/>
    **Note:** Only one role is allowed on a single node.
1. Review and modify configuration settings, such as *log dir* and *planner port*.
    * If Sentry is enabled in the cluster, add the sentry configuration to the field **Configuration Snippet (Safety Valve) for sentry-site.xml**. You can find your Sentry configurations either from the Cloudera Manager Sentry process or Sentry process directory (/var/run/cloudera-scm-agent/process/*-sentry-SENTRY_SERVER/sentry-site.xml). Here is a sample:
        <pre>
&lt;property>
     &lt;name>sentry.service.server.principal&lt;/name>
     &lt;value>sentry/_HOST@principal&lt;/value>
&lt;/property>
&lt;property>
    &lt;name>sentry.service.security.mode&lt;/name>
    &lt;value>kerberos&lt;/value>
&lt;/property>
&lt;property>
    &lt;name>sentry.service.client.server.rpc-address&lt;/name>
    &lt;value>hostname&lt;/value>
&lt;/property>
&lt;property>
    &lt;name>sentry.service.client.server.rpc-port&lt;/name>
    &lt;value>portnum&lt;/value>
&lt;/property>
&lt;property>
    &lt;name>hive.sentry.server&lt;/name>
    &lt;value>server1&lt;/value>
&lt;/property>
        </pre>
1. Start the service.
1. Go to the debug page hostname:11050 to verify that the service started properly. 
1. Go to the RecordService cluster page in Cloudera Manager.
1. From **Actions** choose **Deploy Client Configuration**.
1. Run the following command on the host where you run the RecordService client:

```
export HADOOP_CONF_DIR=/etc/recordservice/conf
```
 
See [http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/cm_mc_add_service.html?scroll=cmug_topic_5_1](http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/cm_mc_add_service.html?scroll=cmug_topic_5_1).

### Deploying Recommended RecordService Roles

**RecordService Planner**: Clients (MapReduce and Spark) connect to the Planner to submit requests. The Planner performs authorization checks and generates all tasks to execute for the overall request. For the beta release, run only one Planner node located on a non-DataNode.

**RecordService Worker**: Read data from the underlying storage layer (HDFS/S3) and construct records. Worker roles cannot directly process client requests. Run a Worker on every DataNode in the cluster to ensure locality.

**RecordService Planner Worker**: Acts as both a Planner and a Worker. Provides flexibility for small clusters. 

**RecordService Gateway**: Contains configuration information, such as the location of the RecordService Planner. Deploy to all Gateway nodes, and any other nodes that run client jobs that do not already have a RecordService Planner or Worker role deployed.

### Running a Job Using RecordService

You can verify the RecordService server installation by running examples from the client JAR.

1. Download the client library and example tarball. [http://archive.cloudera.com/beta/recordservice/client-dist/recordservice-client-0.2.0-cdh5.5.x-bin.tar.gz](http://archive.cloudera.com/beta/recordservice/client-dist/recordservice-client-0.2.0-cdh5.5.x-bin.tar.gz)
    * You can also build the client library yourself from the client repository. [https://github.com/cloudera/RecordServiceClient](https://github.com/cloudera/RecordServiceClient).
    * Client libraries are also available directly from the Cloudera public Maven repository.
1. To verify the server installation, run client examples in your clusters.

<li>Log in to one of the nodes in your cluster, and load test data:</li>
    
<pre>
> wget -q --no-clobber \ 
  https://s3-us-west-1.amazonaws.com/recordservice-vm/tpch.tar.gz 
> tar -xzf tpch.tar.gz
> hadoop fs -mkdir -p /test-warehouse/tpch.nation
> hadoop fs -put -f tpch/nation/* /test-warehouse/tpch.nation/ 
> impala-shell -f create-tbls.sql
</pre>

See  [https://github.com/cloudera/RecordServiceClient/blob/master/tests/create-tbls.sql](https://github.com/cloudera/RecordServiceClient/blob/master/tests/create-tbls.sql).

<li>Run a MapReduce job for RecordCount on tpch.nation:</li>

<pre>
> hadoop jar /path/to/recordservice-examples-0.2.0-cdh5.5.x.jar \
  com.cloudera.recordservice.examples.mapreduce.RecordCount \
  "SELECT * FROM tpch.nation" "/tmp/recordcount_output"
</pre>

<li>Start spark-shell with the RecordService JAR:</li>

<pre>
> path/to/spark/bin/spark-shell \
  --conf spark.recordservice.planner.hostports=planner_host:planner_port \
  --jars /path/to/recordservice-spark-0.2.0-cdh5.5.x.jar
> scala> import com.cloudera.recordservice.spark._
  import com.cloudera.recordservice.spark._
> scala> val data = sc.recordServiceRecords("select * from tpch.nation") \
  data: org.apache.spark.rdd.RDD[Array[org.apache.hadoop.io.Writable]] = \
  RecordServiceRDD[0] at RDD at RecordServiceRDDBase.scala:57
> scala> data.count()
res0: Long = 25
</pre>

See [{{site.examplesSparkUrl}}]({{site.examplesSparkUrl}})

## Installing RecordService from a Package (Not Recommended)

 Follow these steps to install RecordService from a package.

<ol>
<li>Download the package from <a href="http://archive.cloudera.com/beta/recordservice">http://archive.cloudera.com/beta/recordservice</a>.</li>
<li>Install Hadoop, Hive, Impala, Sentry, ZooKeeper, and any other application you want to use.</li>
<li>Install the RecordServicePlanner using the following command-line instruction:<br/><br/>
<pre>
./recordserviced -hostname=hostname -recordservice_planner_port=12050 \
-recordservice_worker_port=0 -recordservice_webserver_port=11050 \
-webserver_doc_root=path/to/package/lib/recordservice \
-log_dir=path/to/log/dir -abort_on_config_error=false \
-lineage_event_log_dir=path/to/log/dir \
-audit_event_log_dir=path/to/log/dir -profile_log_dir=path/to/log/dir \
-v=1 -mem_limit=8G
</pre>
</li>
<br/>
<li>Install the worker using the following command-line instruction.
<br/><br/>
<pre>
./recordserviced  -hostname=hostname -recordservice_planner_port=0 \
-recordservice_worker_port=13050 -recordservice_webserver_port=11050 \
-webserver_doc_root=path/to/package/lib/recordservice \
-log_dir=path/to/log/dir -abort_on_config_error=false \
-lineage_event_log_dir=path/to/log/dir \
-audit_event_log_dir=path/to/log/dir -profile_log_dir=path/to/log/dir \
-v=1 -mem_limit=8G
</pre>
</li>
<br/>
<li>Install both planner and worker on one node:
<br/><br/>
<pre>
./recordserviced -hostname=hostname -recordservice_planner_port=12050 \
-recordservice_worker_port=13050 -recordservice_webserver_port=11050 \
-webserver_doc_root=path/to/package/lib/recordservice \
-log_dir=path/to/log/dir -abort_on_config_error=false \
-lineage_event_log_dir=path/to/log/dir \
-audit_event_log_dir=path/to/log/dir -profile_log_dir=path/to/log/dir \
-v=1 -mem_limit=8G
</pre>
</li>
<br/>
<li>Set additional parameters:
<ul>
<li>Add the following parameters if the cluster is Kerberized:
<pre>
-principal=kerberos_principle -keytab_file=the/path/to/record_service.keytab
</pre></li>
<li>Add the Sentry configuration file, if applicable:
<br/><pre>
-sentry_config=path/to/sentry-site.xml/
</pre></li></ul></ol>
    