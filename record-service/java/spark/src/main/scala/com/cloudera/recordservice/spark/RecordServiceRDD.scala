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

import com.cloudera.recordservice.thrift._
import org.apache.hadoop.io._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.thrift.TException
import org.apache.thrift.protocol.{TProtocol, TBinaryProtocol}
import org.apache.thrift.transport.{TSocket}

import scala.collection.mutable.ListBuffer

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
 * RDD that is backed by the RecordService. This returns an RDD of arrays of
 * Writable objects.
 * Each array is a row.
 * TODO: redo this using the recordservice client/recordservice MR lib
 */
class RecordServiceRDD(sc: SparkContext, stmt: String, plannerHost: String = "localhost")
  extends RDD[Array[Writable]](sc, Nil) with Logging {

  val PLANNER_PORT: Int = 40000
  val WORKER_PORT: Int = 40100

  private def createConnection(host: String, port: Int) : TProtocol = {
    val transport = new TSocket(host, port)
    transport.open()
    new TBinaryProtocol(transport)
  }

  /**
   * Executes the task against the RecordServiceWorker and returns an iterator to fetch
   * result for the entire task.
   */
  override def compute(split: Partition, context: TaskContext):
      InterruptibleIterator[Array[Writable]] = {
    val iter = new NextIterator[Array[Writable]] {
      val partition = split.asInstanceOf[RecordServicePartition]
      var worker: RecordServiceWorker.Client = null

      // Reusable writable objects.
      var writables = new Array[Writable](partition.schema.cols.size())

      // Array for return values. value[i] = writables[i] if the value is non-null
      var value = new Array[Writable](partition.schema.cols.size())

      // Current batch, idx and batch size we are returning.
      var rowBatch: TColumnarRowBatch = null
      var rowIdx: Int = 0

      // For each column, the index into the column data.
      var colIdx = new Array[Int](partition.schema.cols.size())
      var batchSize: Int = 0

      // Register an on-task-completion callback to close the input stream.
      context.addTaskCompletionListener{ context => closeIfNeeded() }

      val execResult = try {
        // Always connect to localhost. This assumes that on each node, we have
        // a RecordServiceWorker running and that Spark has scheduled for locality
        // using getPreferredLocations.
        // TODO: we need to support the case where there is not a worker running on
        // each host, in which case this needs to talk to get the list of all workers
        // and pick one randomly.
        worker = new RecordServiceWorker.Client(
            createConnection("localhost", WORKER_PORT))

        val request = new TExecTaskParams()
        request.setTask(partition.task.task)
        request.setRow_batch_format(TRowBatchFormat.ColumnarThrift)
        worker.ExecTask(request)
      } catch {
        case e:TRecordServiceException => logError("Could not exec request: " + e.message)
          throw new SparkException("RecordServiceRDD failed", e)
        case e:TException => logError("Could not exec request: " + e.getMessage())
          throw new SparkException("RecordServiceRDD failed", e)
      }

      // Iterate over the schema to create the correct writable types
      for (i <- 0 until writables.length) {
        partition.schema.cols.get(i).getType().type_id match {
          case TTypeId.BOOLEAN => writables(i) = new BooleanWritable()
          case TTypeId.TINYINT => writables(i) = new ByteWritable()
          case TTypeId.SMALLINT => writables(i) = new ShortWritable()
          case TTypeId.INT => writables(i) = new IntWritable()
          case TTypeId.BIGINT => writables(i) = new LongWritable()
          case TTypeId.FLOAT => writables(i) = new FloatWritable()
          case TTypeId.DOUBLE => writables(i) = new DoubleWritable()
          case TTypeId.STRING => writables(i) = new Text()
          case _ => throw new SparkException(
            "Unsupported type: " + partition.schema.cols.get(i).getType().type_id)
        }
      }

      def readNextBatch() = {
        var gotAllBatches = false
        while (!gotAllBatches && batchSize == rowIdx) {
          // Fetch the next batch.
          // TODO: require record service Fetch to only return no rows at eos.
          val params = new TFetchParams()
          params.handle = execResult.handle
          val result = worker.Fetch(params)
          batchSize = result.num_rows
          gotAllBatches = result.done
          rowBatch = result.row_batch
          rowIdx = 0

          for (i <- 0 until writables.length) {
            colIdx(i) = 0
          }
        }
      }

      override def getNext() : Array[Writable] = {
        if (rowIdx == batchSize) {
          // Done with current batch, get next one
          readNextBatch()
          if (rowIdx == batchSize) {
            // All done.
            finished = true
            return value
          }
        }

        // Reconstruct the row
        for (i <- 0 until writables.length) {
          if (rowBatch.cols.get(i).is_null.get(rowIdx) == 1) {
            value(i) = null
          } else {
            value(i) = writables(i)
            partition.schema.cols.get(i).getType().type_id match {
              case TTypeId.BOOLEAN => value(i).asInstanceOf[BooleanWritable].set(
                rowBatch.cols.get(i).bool_vals.get(colIdx(i)))
              case TTypeId.TINYINT => value(i).asInstanceOf[ByteWritable].set(
                rowBatch.cols.get(i).byte_vals.get(colIdx(i)))
              case TTypeId.SMALLINT => value(i).asInstanceOf[ShortWritable].set(
                rowBatch.cols.get(i).short_vals.get(colIdx(i)))
              case TTypeId.INT => value(i).asInstanceOf[IntWritable].set(
                rowBatch.cols.get(i).int_vals.get(colIdx(i)))
              case TTypeId.BIGINT => value(i).asInstanceOf[LongWritable].set(
                rowBatch.cols.get(i).long_vals.get(colIdx(i)))
              case TTypeId.FLOAT => value(i).asInstanceOf[FloatWritable].set(
                rowBatch.cols.get(i).double_vals.get(colIdx(i)).toFloat)
              case TTypeId.DOUBLE => value(i).asInstanceOf[DoubleWritable].set(
                rowBatch.cols.get(i).double_vals.get(colIdx(i)))
              case TTypeId.STRING => value(i).asInstanceOf[Text].set(
                rowBatch.cols.get(i).string_vals.get(colIdx(i)))
              case _ => assert(false)
            }
            colIdx(i) = colIdx(i) + 1
          }
        }
        rowIdx += 1
        value
      }

      override def close() = {
        if (worker != null) {
          worker.CloseTask(execResult.handle)
        }
      }
    }

    new InterruptibleIterator[Array[Writable]](context, iter)
  }

  /**
   * Returns the list of preferred hosts to run this partition.
   */
  override def getPreferredLocations(split: Partition): Seq[String] = {
    val partition = split.asInstanceOf[RecordServicePartition]
    partition.hosts
  }

    /**
   * Sends the request to the RecordServicePlanner to generate the list of partitions
   * (tasks in RecordService terminology)
   */
  override protected def getPartitions: Array[Partition] = {
    logInfo("Running request: " + stmt)
    val planResult = try {
      val planner = new RecordServicePlanner.Client(
          createConnection(plannerHost, PLANNER_PORT))
      val request = new TPlanRequestParams()
      request.setSql_stmt(stmt)
      planner.PlanRequest(request)
    } catch {
      case e:TRecordServiceException => logError("Could not plan request: " + e.message)
        throw new SparkException("RecordServiceRDD failed", e)
      case e:TException => logError("Could not plan request: " + e.getMessage())
        throw new SparkException("RecordServiceRDD failed", e)
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
    partitions
  }
}
