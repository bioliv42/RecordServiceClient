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

import com.cloudera.recordservice.client.{RecordServiceWorkerClient, RecordServicePlannerClient}
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
abstract class RecordServiceRDDBase[T:ClassTag](sc: SparkContext, plannerHost: String)
  extends RDD[T](sc, Nil) with Logging {

  val PLANNER_PORT: Int = 40000
  val WORKER_PORT: Int = 40100

  var stmt:String = null

  def setStatement(stmt:String) = {
    this.stmt = stmt
    this
  }

  def setTable(table:String) = {
    this.stmt = "SELECT * from " + table
    this
  }

  /**
   * Executes 'stmt' and returns the worker and rows object associated with it.
   */
  protected def execTask(partition: RecordServicePartition) = {
    try {
      // Always connect to localhost. This assumes that on each node, we have
      // a RecordServiceWorker running and that Spark has scheduled for locality
      // using getPreferredLocations.
      // TODO: we need to support the case where there is not a worker running on
      // each host, in which case this needs to talk to get the list of all workers
      // and pick one randomly.
      val worker = new RecordServiceWorkerClient()
      worker.connect("localhost", WORKER_PORT)
      val rows = worker.execAndFetch(partition.task.task)
      (worker, rows)
    } catch {
      case e:TRecordServiceException => logError("Could not exec request: " + e.message)
        throw new SparkException("RecordServiceRDD failed", e)
      case e:TException => logError("Could not exec request: " + e.getMessage())
        throw new SparkException("RecordServiceRDD failed", e)
    }
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
    if (stmt == null) {
      throw new SparkException(
          "Statement not set. Must call setStatement() or setTable()")
    }

    logInfo("Running request: " + stmt)
    var planner:RecordServicePlannerClient = null
    val planResult = try {
      planner = new RecordServicePlannerClient()
      planner.connect(plannerHost, PLANNER_PORT)
      planner.planRequest(stmt)
    } catch {
      case e:TRecordServiceException => logError("Could not plan request: " + e.message)
        throw new SparkException("RecordServiceRDD failed", e)
      case e:TException => logError("Could not plan request: " + e.getMessage())
        throw new SparkException("RecordServiceRDD failed", e)
    } finally {
      planner.close()
    }

    val partitions = new Array[Partition](planResult.tasks.size())
    for (i <- 0 until planResult.tasks.size()) {
      val hosts = ListBuffer[String]()
      for (j <- 0 until planResult.tasks.get(i).hosts.size()) {
        hosts += planResult.tasks.get(i).hosts.get(j)
      }
      partitions(i) = new RecordServicePartition(id, i, hosts.seq,
        planResult.tasks.get(i), planResult.schema)
    }
    (planResult, partitions)
  }
}
