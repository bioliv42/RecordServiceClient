---
layout: page
title: 'Download and Install RecordService'
---

## Download RecordService

These are the ways you can install RecordService. Cloudera recommends that you install Cloudera Manager (CM) on your cluster, so that you can install RecordService as an add-on service in CM.

## Installing RecordService as an Add-on Service in CM

You can install RecordService as an [Add-on Service](http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/cm_mc_addon_services.html) in CM. First, install both the CSD and the Parcel in CM; then, add RecordService as an add-on service from the home page in CM.

### Installing the RecordService CSD

These are the steps to install the RecordService CSD.

1. Download the CSD from **NEED CORRECT URL**.
1. Upload the CSD to `/opt/cloudera/csd` in the CM server.
1. Change the owner and group for the JAR using the following comamand line instruction:<br/>
```
chown cloudera-scm:cloudera-scm /opt/clouder/csd/RECORD_SERVICE-1.0.jar
```
1. Update the permissions on the file using the following command line instruction:<br/>
```
chmod 644 /opt/cloudera/csd/RECORD_SERVICE-1.0.jar
```
1. Restart the CM server
    1. As the root user in the CM server, then run `service cloudera-scm-server restart`.
    1. Log in to the CM Admin Console and restart the CM Service.
1. Check whether the CSD successfully installed in `http://{cm-server}:7180/cmf/csd/refresh`. Search for the following entry:

```
{
csdName: "RECORD_SERVICE-1.0",
serviceType: "RECORD_SERVICE",
source: "/opt/cloudera/csd/RECORD_SERVICE-1.0.jar",
isInstalled: true
}
```

See [http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/cm_mc_addon_services.html](http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/cm_mc_addon_services.html).

### Parcel Installation

These are the steps to install the RecordService Parcel.

1. Add the Remote Parcel Repository URL to the CM Parcel Settings page. **NEED CORRECT URL.**
1. Go to `http://{cm-server}::7180/cmf/parcel/status`.
1. Click **Edit Settings**.
1. Add the RecordService parcel URL.
1. Open the CM Parcel status page, `http://{cm-server}::7180/cmf/parcel/status`. 
1. **Download** the RecordService parcel.
1. **Distribute** the parcel.
1. **Activate** the parcel. CM asks you to restart the entire cluster, but you only need to start/restart RecordService.

See [http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/cm_ig_parcels.html](http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/cm_ig_parcels.html)

### Start up RecordService in a Cluster from CM

There are the steps to start up RecordService for a cluster from CM.

1. Add a service from CM home page.
1. Run a RecordService Worker on each DataNode (DN) and one RecordService Planner. 
    * If the cluster is so small that every node is running the DN, run a RecordService Planner and Worker role.
    * If the cluster has 1 dedicated NN, *N* DNs, run: 1 Planner, *N* Workers.
    * If the cluster has *N* nodes, with 1 node having the NN and DN, run 1 Planner and Worker, and *N*-1 Workers.
1. Customize Role: 
    * RecordService Planner and Worker: Select nodes with both role of DN and NN.
    * RecordService Planner: Select nodes with role of NN.
    * RecordService Worker: Select nodes with role of DN.
1. Review and modify configuration settings, such as log dir and planner port.
1. Start the service.
1. Visit the debug page hostname:35000 to verify it started properly. 
1. Go to the RecordService cluster page in CM.
1. Choose **Deploy Client Configuration** from *Actions*.
 
See [http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/cm_mc_add_service.html?scroll=cmug_topic_5_1](http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/cm_mc_add_service.html?scroll=cmug_topic_5_1).

### Experiment with Client JARs
You can verify the RecordService server installation by running examples from the client JAR.

1. Download the client JAR. 
    * You can also build client JAR by yourself from the client repo.
    * You can import it as a lib in your project, or run it directly from Hadoop, Spark-shell, and so on.
1. To verify the server installation, you can run our client examples in your clusters.
    * Log in to one of the nodes in your cluster, and load test data:
    
```
    # wget -q --no-clobber \ 
      http://util-1.ent.cloudera.com/impala-test-data/tpch.tar.gz 
    # tar -xzf tpch.tar.gz
    # hadoop fs -mkdir -p /test-warehouse/tpch.nation
    # hadoop fs -put -f tpch/nation/* /test-warehouse/tpch.nation/ 
    # impala-shell -f create-tbls.sql
```
    
    * Start up spark-shell with the RecordService JAR: 
    
```
    # path/to/spark/bin/spark-shell --jars \
         /path/to/recordservice-examples-spark-0.1.jar
    # scala> import com.cloudera.recordservice.spark._
    import com.cloudera.recordservice.spark._
    # scala> val data = sc.recordServiceRecords("select * from tpch.nation") \
         data: org.apache.spark.rdd.RDD[Array[org.apache.hadoop.io.Writable]] = \
         RecordServiceRDD[0] at RDD at RecordServiceRDDBase.scala:57
    # scala> data.count()
    res0: Long = 25
```
  
See [http://github.mtv.cloudera.com/CDH/RecordServiceClient/blob/master/java/examples-spark/README.md](http://github.mtv.cloudera.com/CDH/RecordServiceClient/blob/master/java/examples-spark/README.md)

### Package Installation (Not Recommended)

These are the steps to install RecordService from a package.

<ol>
<li>Download package from <b>NEED CORRECT URL</b></li>
<li>Install Hadoop, Hive, Impala, Sentry, ZooKeeper and any other application you want to use.</li>
<li>Install the RecordServicePlanner using the following command line instruction:<br/><br/>
<pre>
./recordserviced -hostname=hostname -recordservice_planner_port=40000 \ 
-recordservice_worker_port=0 -recordservice_webserver_port=35000 \ 
-webserver_doc_root=path/to/package/lib/recordservice \
-log_dir=path/to/log/dir -abort_on_config_error=false \ 
-lineage_event_log_dir=path/to/log/dir \ 
-audit_event_log_dir=path/to/log/dir -profile_log_dir=path/to/log/dir \ 
-v=1 -mem_limit=8G
</pre>
</li>
<br/>
<li>Install the worker using the following command line instruction.
<br/><br/>
<pre>
./recordserviced  -hostname=hostname -recordservice_planner_port=0 \
-recordservice_worker_port=40100 -recordservice_webserver_port=35000 \
-webserver_doc_root=path/to/package/lib/recordservice \ 
-log_dir=path/to/log/dir -abort_on_config_error=false \ 
-lineage_event_log_dir=path/to/log/dir \ 
-audit_event_log_dir=path/to/log/dir -profile_log_dir=path/to/log/dir \ 
-v=1 -mem_limit=8G
</pre>
</li>
<br/>
<li>Install both planner and worker in one node:
<br/><br/>
<pre>
./recordserviced  -hostname=hostname -recordservice_planner_port=40000 \ 
-recordservice_worker_port=40100 -recordservice_webserver_port=35000 \ 
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
<li>Add the following params if the cluster is kerberized:
<pre>
-principal=kerberos_principle -keytab_file=the/path/to/record_service.keytab
</pre></li>
<li>Add the Sentry configuration file, if applicable:
<br/><br/><pre>
-sentry_config=path/to/sentry-site.xml/
</pre></li></ul></ol>
    