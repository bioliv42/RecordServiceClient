/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.recordservice.spark

import com.cloudera.recordservice.core._
import com.cloudera.recordservice.mr.{RecordServiceConfig, PlanUtil}
import org.apache.spark._
import org.apache.spark.rdd.RDD

import java.net.InetAddress

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag
import scala.collection.JavaConversions.asScalaBuffer
import scala.util.Random

private class RecordServicePartition(rddId: Int, idx: Int,
                                     h:Seq[String], p:Seq[Int],
                                     w: Seq[NetworkAddress], t: Task, s: Schema,
                                     token: DelegationToken)
    extends Partition {
  override def hashCode(): Int = 41 * (41 + rddId) + idx
  override val index: Int = idx
  val task: Task = t
  val schema: Schema = s
  val hosts: Seq[String] = h
  val ports: Seq[Int] = p
  val globalHosts: Seq[NetworkAddress] = w
  val delegationToken: DelegationToken = token
}

/**
 * RDD that is backed by the RecordService. This is the base class that handles some of
 * common Spark and RecordService interactions.
 *
 * Security: currently, if kerberos is enabled, the planner request will get a delegation
 * token that is stored in the partition object. The RDD authenticates with the worker
 * using this token.
 * TODO: is there a more proper way to do this in Spark? It doesn't look like SparkContext
 * has a way to do this.
 */
abstract class RecordServiceRDDBase[T:ClassTag](@transient sc: SparkContext)
    extends RDD[T](sc, Nil) with Logging {

  // Metrics from the RecordServiceServer
  val recordsReadAccum = sc.accumulator(0L, "RecordsRead")
  val recordsReturnedAccum = sc.accumulator(0L, "RecordsReturned")
  val serializeTimeAccum = sc.accumulator(0L, "SerializeTimeMs")
  val clientTimeAccum = sc.accumulator(0L, "ClientTimeMs")
  val decompressTimeAccum = sc.accumulator(0L, "DecompressTimeMs")
  val bytesReadAccum = sc.accumulator(0L, "BytesRead")
  val bytesReadLocalAccum = sc.accumulator(0L, "BytesReadLocal")

  // Configs that we use when we execute the task. These come from SparkContext
  // but we do not serialize the entire context. Instead these are populated in
  // the client (i.e. planning phase).
  val memLimit = sc.getConf.getLong(RecordServiceConfig.MEM_LIMIT_CONF, -1);
  val limit = sc.getConf.getLong(RecordServiceConfig.RECORDS_LIMIT_CONF, -1);
  val maxAttempts = sc.getConf.getInt(
      RecordServiceConfig.WORKER_RETRY_ATTEMPTS_CONF, -1);
  val taskSleepMs = sc.getConf.getInt(
      RecordServiceConfig.WORKER_RETRY_SLEEP_MS_CONF, -1);
  val connectionTimeoutMs = sc.getConf.getInt(
      RecordServiceConfig.WORKER_CONNECTION_TIMEOUT_MS_CONF, -1);
  val rpcTimeoutMs = sc.getConf.getInt(
      RecordServiceConfig.WORKER_RPC_TIMEOUT_MS_CONF, -1);

  // Request to make
  @transient var request:Request = null

  var plannerHostPorts:java.util.List[NetworkAddress] =
      RecordServiceConf.getPlannerHostPort(sc)

  // Result schema (after projection)
  var schema:Schema = null

  def setStatement(stmt:String) = {
    verifySetRequest()
    request = Request.createSqlRequest(stmt)
    this
  }

  def setTable(table:String) = {
    verifySetRequest()
    request = Request.createTableScanRequest(table)
    this
  }

  def setPath(path:String) = {
    verifySetRequest()
    request = Request.createPathRequest(path)
    this
  }

  def setRequest(req:Request) = {
    verifySetRequest()
    request = req
    this
  }

  def getSchema(): Schema = {
    if (schema == null) {
      // TODO: this is kind of awkward. Introduce a new plan() API?
      throw new SparkException("getSchema() can only be called after getPartitions")
    }
    schema
  }

  /**
   * Sets the planner host/port to connect to. Default pulls from configs.
   */
  def setPlannerHostPort(host:String, port:Int): Unit = {
    plannerHostPorts.clear()
    plannerHostPorts.add(new NetworkAddress(host, port))
  }

  protected def verifySetRequest() = {
    if (request != null) {
      throw new SparkException("Request is already set.")
    }
  }

  /**
   * Returns the worker host to connect to, attempting to schedule for locality. If
   * any of the hosts matches this hosts address, use that host. Otherwise connect
   * to a host at random.
   */
  def getWorkerToConnectTo(partition: RecordServicePartition): (String, Int) = {
    val tid = partition.task.taskId
    val hostAddress = InetAddress.getLocalHost.getHostName()
    for (i <- 0 until partition.hosts.size) {
      if (partition.hosts(i) == hostAddress) {
        logInfo(s"Both data and RecordServiceWorker are availablelocally for task $tid")
        return (partition.hosts(i), partition.ports(i))
      }
    }

    // Check if the current host is running a RecordServiceWorker. If so, schedule the
    // task locally. Otherwise, choose a random host from the global membership
    // for the task.
    val addr = partition.globalHosts.find((_.hostname == hostAddress)) match {
      case Some(addr) => {
        logInfo(s"RecordServiceWorker is available locally for task $tid")
        addr
      }
      case None => {
        val addr = partition.globalHosts(Random.nextInt(partition.globalHosts.size))
        logInfo(s"Neither RecordServiceWorker nor data is available locally " +
          s"for task $tid. Randomly selected host ${addr.hostname} to execute it")
        addr
      }
    }
    (addr.hostname, addr.port)
  }

  /**
   * Executes 'stmt' and returns the worker and Records object associated with it.
   */
  protected def execTask(partition: RecordServicePartition) = {
    var worker:RecordServiceWorkerClient = null
    var records:Records = null
    try {
      val builder = new RecordServiceWorkerClient.Builder()
      builder.setDelegationToken(partition.delegationToken)

      if (memLimit != -1) builder.setMemLimit(memLimit);
      if (limit != -1) builder.setLimit(limit);
      if (maxAttempts != -1) builder.setMaxAttempts(maxAttempts);
      if (taskSleepMs != -1 ) builder.setSleepDurationMs(taskSleepMs);
      if (connectionTimeoutMs != -1) builder.setConnectionTimeoutMs(connectionTimeoutMs);
      if (rpcTimeoutMs != -1) builder.setRpcTimeoutMs(rpcTimeoutMs);

      val (hostname, port) = getWorkerToConnectTo(partition)
      worker = builder.connect(hostname, port)
      records = worker.execAndFetch(partition.task)
      (worker, records)
    } catch {
      case e:RecordServiceException => logError("Could not exec request: " + e.message)
        throw new SparkException("RecordServiceRDD failed", e)
    } finally {
      if (records == null && worker != null) worker.close()
    }
  }

  // Returns a simplified schema for 'schema'. The RecordService supports richer types
  // than Spark so collapse types.
  protected def simplifySchema(schema:Schema) : Array[Schema.Type] = {
    val result = new Array[Schema.Type](schema.cols.size())
    for (i <- 0 until schema.cols.size()) {
      val t = schema.cols.get(i).`type`.typeId
      if (t == Schema.Type.VARCHAR || t == Schema.Type.CHAR) {
        result(i) = Schema.Type.STRING
      } else {
        result(i) = t
      }
    }
    result
  }

  /**
   * Updates the counters (accumulators) from records. This is *not* idempotent
   * and can only be called once per task, at the end of the task.
   */
  protected def updateCounters(records:Records) = {
    val stats = records.getStatus.stats
    recordsReadAccum += stats.numRecordsRead
    recordsReturnedAccum += stats.numRecordsReturned
    serializeTimeAccum += stats.serializeTimeMs
    clientTimeAccum += stats.clientTimeMs
    decompressTimeAccum += stats.decompressTimeMs
    bytesReadAccum += stats.bytesRead
    bytesReadLocalAccum += stats.bytesReadLocal
  }

  /**
   * Returns the list of preferred hosts to run this partition.
   */
  override def getPreferredLocations(split: Partition): Seq[String] = {
    val partition = split.asInstanceOf[RecordServicePartition]
    partition.hosts
  }

  /**
   * Plans the request for 'stmt'. Returns the plan request and the spark list of
   * partitions.
   */
  protected def planRequest = {
    if (request == null) {
      throw new SparkException(
          "Request not set. Must call setStatement(), setTable() or setPath()")
    }

    var planner: RecordServicePlannerClient = null
    val (planResult, delegationToken) = try {
      // TODO: for use cases like oozie, we need to authenticate with the planner via
      // delegationToken as well. How is this done for spark? How do we get at the
      // credentials object.
      val principal = RecordServiceConf.getKerberosPrincipal(sc)
      val connectionTimeoutMs =
          sc.getConf.getInt(RecordServiceConfig.PLANNER_CONNECTION_TIMEOUT_MS_CONF, -1)
      val rpcTimeoutMs =
          sc.getConf.getInt(RecordServiceConfig.PLANNER_RPC_TIMEOUT_MS_CONF, -1)
      val maxAttempts =
          sc.getConf.getInt(RecordServiceConfig.PLANNER_RETRY_ATTEMPTS_CONF, -1)
      val sleepDurationMs =
          sc.getConf.getInt(RecordServiceConfig.PLANNER_RETRY_SLEEP_MS_CONF, -1)
      val maxTasks =
          sc.getConf.getInt(RecordServiceConfig.PLANNER_REQUEST_MAX_TASKS, -1)

      val builder = new RecordServicePlannerClient.Builder()
      if (connectionTimeoutMs != -1) builder.setConnectionTimeoutMs(connectionTimeoutMs);
      if (rpcTimeoutMs != -1) builder.setRpcTimeoutMs(rpcTimeoutMs);
      if (maxTasks != -1) builder.setMaxAttempts(maxAttempts);
      if (sleepDurationMs != -1) builder.setSleepDurationMs(sleepDurationMs);
      if (maxAttempts != -1) builder.setMaxTasks(maxTasks);
      planner = PlanUtil.getPlanner(builder, plannerHostPorts, principal, null)
      val result = planner.planRequest(request)
      if (principal != null) {
        (result, planner.getDelegationToken(""))
      } else {
        (result, null)
      }
    } catch {
      case e:RecordServiceException => logError("Could not plan request: " + e.message)
        throw new SparkException("RecordServiceRDD failed", e)
    } finally {
      if (planner != null) planner.close()
    }

    val partitions = new Array[Partition](planResult.tasks.size())
    for (i <- 0 until planResult.tasks.size()) {
      val hosts = ListBuffer[String]()
      val ports = ListBuffer[Int]()
      for (j <- 0 until planResult.tasks.get(i).localHosts.size()) {
        hosts += planResult.tasks.get(i).localHosts.get(j).hostname
        ports += planResult.tasks.get(i).localHosts.get(j).port
      }
      val globalHosts = asScalaBuffer(planResult.hosts).toList
      partitions(i) = new RecordServicePartition(id, i, hosts.seq, ports.seq,
        globalHosts, planResult.tasks.get(i), planResult.schema, delegationToken)
    }
    schema = planResult.schema
    (planResult, partitions)
  }
}
