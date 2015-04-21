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

import com.cloudera.recordservice.client._
import com.cloudera.recordservice.thrift._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.thrift.TException

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

private class RecordServicePartition(rddId: Int, idx: Int,
                                     h:Seq[String], t: TTask, s: TSchema)
  extends Partition {
  override def hashCode(): Int = 41 * (41 + rddId) + idx
  override val index: Int = idx
  val task: TTask = t
  val schema: TSchema = s
  val hosts: Seq[String] = h
}

/**
 * RDD that is backed by the RecordService. This is the base class that handles some of
 * common Spark and RecordService interactions.
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

  // Request to make
  @transient var request:Request = null

  var (plannerHost, plannerPort) = RecordServiceConf.getPlannerHostPort(sc)

  // Result schema (after projection)
  var schema:TSchema = null

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

  def getSchema(): TSchema = {
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
    plannerHost = host
    plannerPort = port
  }

  protected def verifySetRequest() = {
    if (request != null) {
      throw new SparkException("Request is already set.")
    }
  }

  /**
   * Executes 'stmt' and returns the worker and Records object associated with it.
   */
  protected def execTask(partition: RecordServicePartition) = {
    try {
      // Always connect to localhost. This assumes that on each node, we have
      // a RecordServiceWorker running and that Spark has scheduled for locality
      // using getPreferredLocations.
      // TODO: we need to support the case where there is not a worker running on
      // each host, in which case this needs to talk to get the list of all workers
      // and pick one randomly.
      // TODO: this port should come from the task description.
      val worker = new RecordServiceWorkerClient.Builder()
          .connect("localhost", RecordServiceConf.WORKER_PORT)
      val records = worker.execAndFetch(partition.task)
      (worker, records)
    } catch {
      case e:TRecordServiceException => logError("Could not exec request: " + e.message)
        throw new SparkException("RecordServiceRDD failed", e)
      case e:TException => logError("Could not exec request: " + e.getMessage())
        throw new SparkException("RecordServiceRDD failed", e)
    }
  }

  // Returns a simplified schema for 'schema'. The RecordService supports richer types
  // than Spark so collapse types.
  protected def simplifySchema(schema:TSchema) : Array[TTypeId] = {
    val result = new Array[TTypeId](schema.cols.size())
    for (i <- 0 until schema.cols.size()) {
      val t = schema.cols.get(i).getType.type_id
      if (t == TTypeId.VARCHAR || t == TTypeId.CHAR) {
        result(i) = TTypeId.STRING
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
    recordsReadAccum += stats.num_records_read
    recordsReturnedAccum += stats.num_records_returned
    serializeTimeAccum += stats.serialize_time_ms
    clientTimeAccum += stats.client_time_ms
    decompressTimeAccum += stats.decompress_time_ms
    bytesReadAccum += stats.bytes_read
    bytesReadLocalAccum += stats.bytes_read_local
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

    val planResult = try {
      RecordServicePlannerClient.planRequest(plannerHost, plannerPort, request)
    } catch {
      case e:TRecordServiceException => logError("Could not plan request: " + e.message)
        throw new SparkException("RecordServiceRDD failed", e)
      case e:TException => logError("Could not plan request: " + e.getMessage())
        throw new SparkException("RecordServiceRDD failed", e)
    }

    val partitions = new Array[Partition](planResult.tasks.size())
    for (i <- 0 until planResult.tasks.size()) {
      val hosts = ListBuffer[String]()
      for (j <- 0 until planResult.tasks.get(i).local_hosts.size()) {
        hosts += planResult.tasks.get(i).local_hosts.get(j).hostname
      }
      partitions(i) = new RecordServicePartition(id, i, hosts.seq,
        planResult.tasks.get(i), planResult.schema)
    }
    schema = planResult.schema
    (planResult, partitions)
  }
}
