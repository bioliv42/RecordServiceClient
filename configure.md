---
layout: page
title: 'Configure RecordService'
---

## Client Configurations

While it should not be necessary to change the default configuration, you have the option of modifying any of these settings.

<table border="1">          
<tr align="left" valign="top"><th> PROPERTY </th><th> DESCRIPTION </th><th> PARAMETER </th><th> VALUE </th><th> SIGNIFICANCE </th></tr>
<tr align="left" valign="top"><td> FETCH_SIZE_CONF </td><td> Option for performance tuning that configures the max number of records returned when fetching results from the RecordService. If not set, server default will be used. </td><td> recordservice.task.fetch.size </td><td>  </td><td>  </td></tr>
<tr align="left" valign="top"><td> MEM_LIMIT_CONF </td><td> Maximum memory the server uses per task </td><td> recordservice.task.memlimit.bytes </td><td>  </td><td>  </td></tr>
<tr align="left" valign="top"><td> RECORDS_LIMIT_CONF </td><td> Maximum number of records returned per task </td><td> recordservice.task.records.limit </td><td>  </td><td>  </td></tr>
<tr align="left" valign="top"><td> PLANNER_REQUEST_MAX_TASKS </td><td> Maximum number of tasks to generate per PlanRequest </td><td> recordservice.task.plan.maxTasks </td><td>  </td><td>  </td></tr>
<tr align="left" valign="top"><td> PLANNER_RETRY_ATTEMPTS_CONF </td><td> Maximum number of attempts to retry RecordService RPCs with Planner </td><td> recordservice.planner.retry.attempts </td><td>  </td><td>  </td></tr>
<tr align="left" valign="top"><td> PLANNER_RETRY_SLEEP_MS_CONF </td><td> Sleep between retry attempts with Planner in milliseconds </td><td> recordservice.planner.retry.sleepMs </td><td>  </td><td>  </td></tr>
<tr align="left" valign="top"><td> PLANNER_CONNECTION_TIMEOUT_MS_CONF </td><td> Timeout when connecting to the Planner service </td><td> recordservice.planner.connection.timeoutMs </td><td>  </td><td>  </td></tr>
<tr align="left" valign="top"><td> PLANNER_RPC_TIMEOUT_MS_CONF </td><td> Timeout for Planner RPCs </td><td> recordservice.planner.rpc.timeoutMs </td><td>  </td><td>  </td></tr>
<tr align="left" valign="top"><td> WORKER_RETRY_ATTEMPTS_CONF </td><td> Maximum number of attempts to retry RecordService RPCs with Worker </td><td> recordservice.worker.retry.attempts </td><td>  </td><td>  </td></tr>
<tr align="left" valign="top"><td> WORKER_RETRY_SLEEP_MS_CONF </td><td> Sleep in milliseconds between retry attempts with Worker </td><td> recordservice.worker.retry.sleepMs </td><td>  </td><td>  </td></tr>
<tr align="left" valign="top"><td> WORKER_CONNECTION_TIMEOUT_MS_CONF </td><td> Timeout when connecting to the Worker service </td><td> recordservice.worker.connection.timeoutMs </td><td>  </td><td>  </td></tr>
<tr align="left" valign="top"><td> WORKER_RPC_TIMEOUT_MS_CONF </td><td> Timeout for Worker RPCs   </td><td> recordservice.worker.rpc.timeoutMs </td><td>  </td><td>  </td></tr>
<tr align="left" valign="top"><td> WORKER_ENABLE_SERVER_LOGGING_CONF </td><td> Enable server logging (logging level from Log4j)   </td><td> recordservice.worker.server.enableLogging </td><td>  </td><td>  </td></tr>
</table>

## Server Configurations

The properties listed on the CM RecordService Configuration page are the ones Cloudera considers the most reasonable to change. However, adjusting these values should not be necessary. Very advanced administrators might consider making minor adjustments.

### Kerberos Configuration

No special configuration is required via CM. Enabling Kerberos on the cluster configures everything.

## Sentry Table Configuration

Sentry is configured with standard installation. No special configuration is required.

## Delegation Token Configuration

No special configuration is required with CM. This is enabled automatically if the cluster is kerberized.

RecordService persists state in Zookeeper, by default, under the /recordservice Zookeeper directory. If this directory is already in use, you can configure the directory with `recordservice.zookeeper.znode`. This is a Hadoop style XML configuration that you can add to the advanced service configuration snippet.
