---
layout: article
title: 'Configuring RecordService'
share: false 
---

{% include toc.html %}

## Client Configurations

Although you should not need to change the default configuration, you can modify RecordService properties.

To change any of the setting listed below:

<ol>
<li>In Cloudera Manager, navigate to the RecordService configuration page.
</li>
<li>
Search for <code>Safety Valve</code>.
</li>
<li>In the search results, look for <b>RecordService (Beta) Client Advanced Configuration Snippet (Safety Valve) for recordservice-conf/recordservice-site.xml</b>. 
</li>
<li> Add or change the value in the field provided. For example, to change <code>recordservice.task.fetch.size</code> to <code>1000</code>, add the following code:
<br/><pre>
&lt;property>
  &lt;name>recordservice.task.fetch.size&lt;/name>
  &lt;value>1000&lt;/value>
&lt;/property>
</pre>
</li>
<li>Click <b>Save Changes</b>.
</li>
<li>From the <b>Actions</b> menu, choose <b>Deploy Client Configuration</b>.
</li>
</ol>

For more information, see <a href="http://www.cloudera.com/content/www/en-us/documentation/enterprise/latest/topics/cm_mc_mod_configs.html">Modifying Configuration Properties Using Cloudera Manager</a>.

You can adjust the following configuration settings in your RecordService instance.

<table border="1">    
<tr><th>CATEGORY</th><th>PARAMETER</th><th>DESCRIPTION</th><th> DEFAULT VALUE </th></tr>
<tr align="left"><td style="vertical-align:top">Connectivity</td><td style="vertical-align:top">recordservice.planner.hostports</td><td style="vertical-align:top">Comma separated list of planner service host/ports.</td><td style="vertical-align:top">localhost:12050</td></tr>

<tr align="left"><td style="vertical-align:top">Connectivity</td><td style="vertical-align:top">recordservice.kerberos.principal</td><td style="vertical-align:top">Kerberos principal for the planner service. Required if using Kerberos.</td><td style="vertical-align:top"></td></tr>

<tr align="left"><td style="vertical-align:top">Connectivity</td><td style="vertical-align:top">recordservice.planner.retry.attempts</td><td style="vertical-align:top">Maximum number of attempts to retry RecordService RPCs with Planner.</td><td style="vertical-align:top">3</td></tr>

<tr align="left"><td style="vertical-align:top">Connectivity</td><td style="vertical-align:top">recordservice.planner.retry.sleepMs</td><td style="vertical-align:top">Sleep between retry attempts with Planner in milliseconds.</td><td style="vertical-align:top">5000</td></tr>

<tr align="left"><td style="vertical-align:top">Connectivity</td><td style="vertical-align:top">recordservice.planner.connection.timeoutMs</td><td style="vertical-align:top">Timeout when connecting to the Planner service in milliseconds.</td><td style="vertical-align:top">30000</td></tr>

<tr align="left"><td style="vertical-align:top">Connectivity</td><td style="vertical-align:top">recordservice.planner.rpc.timeoutMs</td><td style="vertical-align:top">Timeout for Planner RPCs in milliseconds.</td><td style="vertical-align:top">120000</td></tr>

<tr align="left"><td style="vertical-align:top">Connectivity</td><td style="vertical-align:top">recordservice.worker.retry.attempts</td><td style="vertical-align:top">Maximum number of attempts to retry RecordService RPCs with a worker.</td><td style="vertical-align:top">3</td></tr>

<tr align="left"><td style="vertical-align:top">Connectivity</td><td style="vertical-align:top">recordservice.worker.retry.sleepMs</td><td style="vertical-align:top">Sleep in milliseconds between retry attempts with  worker.</td><td style="vertical-align:top">5000</td></tr>

<tr align="left"><td style="vertical-align:top">Connectivity</td><td style="vertical-align:top">recordservice.worker.connection.timeoutMs</td><td style="vertical-align:top">Timeout when connecting to the worker in milliseconds.</td><td style="vertical-align:top">10000</td></tr>

<tr align="left"><td style="vertical-align:top">Connectivity</td><td style="vertical-align:top">recordservice.worker.rpc.timeoutMs</td><td style="vertical-align:top">Timeout for Worker RPCs in milliseconds.</td><td style="vertical-align:top">120000</td></tr>

<tr align="left"><td style="vertical-align:top">Performance</td><td style="vertical-align:top">recordservice.task.fetch.size</td><td style="vertical-align:top">Configures the maximum number of records returned when fetching results from the RecordService. If not set, the server default is used. <br/><br/>
You might need to adjust this value according to the type of workload (for example, MapReduce or Spark), due to differences in data processing speed.</td><td style="vertical-align:top">5000</td></tr>

<tr align="left"><td style="vertical-align:top">Resource Management</td><td style="vertical-align:top">recordservice.task.memlimit.bytes</td><td style="vertical-align:top">Maximum memory the server uses per task. Tasks exceeding this limit are aborted.  If not set, the server process limit is used.</td><td style="vertical-align:top">-1 (Unlimited)</td></tr>

<tr align="left"><td style="vertical-align:top">Resource Management</td><td style="vertical-align:top">recordservice.task.plan.maxTasks</td><td style="vertical-align:top">Hint for maximum number of tasks to generate per PlanRequest. This is not strictly enforced by the server, but is used to determine if task combining should occur. This value might need to be set for large datasets.</td><td style="vertical-align:top">-1 (Unlimited)</td></tr>

<tr align="left"><td style="vertical-align:top">Resource Management (Advanced)</td><td style="vertical-align:top">recordservice.task.records.limit</td><td style="vertical-align:top">Maximum number of records returned per task.</td><td style="vertical-align:top">-1 (Unlimited)</td></tr>

<tr align="left"><td style="vertical-align:top">Logging (Advanced)</td><td style="vertical-align:top">recordservice.worker.server.enableLogging</td><td style="vertical-align:top">Enable server logging (logging level from Log4j).</td><td style="vertical-align:top">FALSE</td></tr>
</table>

## Server Configurations

The properties listed on the Cloudera Manager RecordService Configuration page are the ones Cloudera considers the most reasonable to change. However, adjusting these values should not be necessary. Very advanced administrators might consider making minor adjustments.

### Dynamic Fetch Size

The following properties allow you to adjust dynamic fetch size on the server. 

<table border="1">
<tr><th>CATEGORY</th><th>PARAMETER</th><th>DESCRIPTION</th><th> DEFAULT VALUE </th></tr>
<tr align="left">
<td style="vertical-align:top">
Resource Management
</td>
<td style="vertical-align:top">
rs_compressed_max_fetch_size
</td>
<td style="vertical-align:top">
Maximum fetch size when scanning compressed text files.
</td>
<td style="vertical-align:top">
1000
</td>
</tr>

<tr align="left">
<td style="vertical-align:top">
Resource Management
</td>
<td style="vertical-align:top">
rs_fetch_size_decrease_factor
</td>
<td style="vertical-align:top">
Correction factor to decrease fetch size; must be >= 1.
</td>
<td style="vertical-align:top">
1.5
</td>
</tr>

<tr align="left">
<td style="vertical-align:top">
Resource Management
</td>
<td style="vertical-align:top">
rs_fetch_size_increase_factor
</td>
<td style="vertical-align:top">
Correction factor to increase fetch size; must be > 0 and <= 1.
</td>
<td style="vertical-align:top">
0.001
</td>
</tr>

<tr align="left">
<td style="vertical-align:top">
Resource Management
</td>
<td style="vertical-align:top">
rs_min_fetch_size
</td>
<td style="vertical-align:top">
The minimum fetch size for the scanner thread.
</td>
<td style="vertical-align:top">
500
</td>
</tr>

<tr align="left">
<td style="vertical-align:top">
Resource Management
</td>
<td style="vertical-align:top">
rs_spare_capacity_correction_factor
</td>
<td style="vertical-align:top">
Correction factor for spare capacity; must be > 0 and <= 1. 
</td>
<td style="vertical-align:top">
0.8
</td>
</tr>

</table>

### Kerberos Configuration

No special configuration is required through Cloudera Manager. Enabling Kerberos on the cluster configures everything required.

### Sentry Table Configuration

Sentry is configured for you in the RecordService VM. This section describes how to configure Sentry in a  non-VM deployment.

#### Prerequisite

Follow CDH documentation to install Sentry and enable it for Hive (and Impala, if applicable).

See [http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/sg_sentry_service_install.html](http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/sg_sentry_service_install.html).

#### Configure Sentry with RecordService

<ol>
<li>Enable RecordService to read policy metadata from Sentry:
    <ol type="a">
    <li>In Cloudera Manager, navigate to the <b>Sentry Configuration</b> page.</li>
    <li>In <b>Admin Groups</b>, add the user <i>recordservice</i>.</li>
    <li>In <b>Allowed Connecting Users</b>, add the user <i>recordservice</i>.</li>
    </ol></li>
<li>Save changes.</li>
<li>Enable Sentry for RecordService.
    <ol type="a">
    <li>In Cloudera Manager, navigate to <b>RecordService Configuration</b>.</li>
    <li>Select the <b>Sentry-1</b> service.</li>
    <li>If you are using Cloudera Manager 5.7 with RecordService 0.3.0, enter the following settings in the <b>Sentry Advanced Configuration Snippet (Safety Valve)</b> field.</li>
    </ol>
<pre>
&lt;property&gt;
    &lt;name&gt;hive.sentry.server&lt;/name&gt;
    &lt;value>server1&lt;/value&gt;
&lt;/property&gt;
</pre>
<p>If you are using a version of Cloudera Manager lower than 5.7, enter the following settings in the <b>Sentry Advanced Configuration Snippet (Safety Valve)</b> field.
</p>
<pre>
&lt;property&gt;
    &lt;name&gt;sentry.service.server.principal&lt;/name&gt;
    &lt;value&gt;sentry/_HOST@principal&lt;/value&gt;
&lt;/property&gt;

&lt;property&gt;
    &lt;name&gt;sentry.service.security.mode&lt;/name&gt;
    &lt;value&gt;kerberos&lt;/value&gt;
&lt;/property&gt;

&lt;property&gt;
    &lt;name&gt;sentry.service.client.server.rpc-address&lt;/name&gt;
    &lt;value&gt;hostname&lt;/value&gt;
&lt;/property&gt;

&lt;property&gt;
    &lt;name&gt;sentry.service.client.server.rpc-port&lt;/name&gt;
    &lt;value&gt;portnum&lt;/value&gt;
&lt;/property&gt;

&lt;property&gt;
    &lt;name&gt;hive.sentry.server&lt;/name&gt;
    &lt;value&gt;server1&lt;/value&gt;
&lt;/property&gt;
</pre>
</li>
<li>Save your changes.</li>
<li>Restart the Sentry and RecordService services.</li>
</ol>

### Delegation Token Configuration

No special configuration is required with Cloudera Manager. This is enabled automatically if the cluster is Kerberized.

RecordService persists state in ZooKeeper, by default, under the `/recordservice` ZooKeeper directory. If this directory is already in use, you can configure the directory with `recordservice.zookeeper.znode`. This is a Hadoop XML configuration that you can add to the advanced service configuration snippet.

### Planner Auto Discovery Configuration

RecordService 0.3.0 and higher includes the Planner Auto Discovery feature. 
You do not need to specify a list of planner host/ports for your RecordService clients through the configuration property `recordservice.planner.hostports`. Instead, you can use the property `recordservice.zookeeper.connectString`, which specifies the connection string to the ZooKeeper session used to keep store information about planner/worker membership (as well as other information, such as delegation tokens). Both the client and the server use this property.

Planner Auto Discovery allows client-side applications independent of changes in the planner configuration. Planners might come and go in the cluster, but the client-side application uses the same configuration settings.

If you use Cloudera Manager to manage the cluster, this property is automatically populated to the client side configurations through the CSD.

Setting the property enables Planner Auto Discovery. A RecordService job first contacts ZooKeeper to fetch a list of available RecordService planners, and then uses those resources for planning. If this step fails, the job reads `recordservice.planner.hostports` and uses static membership information.

Additional properties provide tuning options.

| Property | Description |
|--- |--- |
|`recordservice.zookeeper.connectTimeoutMillis` | Specifies a timeout when initiating a ZooKeeper connection |
|`recordservice.zookeeper.znode` | Specifies the root ZooKeeper directory. Default is  `/recordservice` |

Both the client and the server use these properties.

