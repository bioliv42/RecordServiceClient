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

/**
 * RDD that is backed by the RecordService. This returns an RDD of arrays of
 * Writable objects.
 * Each array is a row.
 * TODO: remove default localhost param. This should pull it from the configs.
 */
class RecordServiceRDD(sc: SparkContext, plannerHost: String = "localhost")
  extends RecordServiceRDDBase[Array[Writable]](sc, plannerHost) with Logging {

  override def setTable(table:String) = {
    super.setTable(table)
    this
  }

  override def setStatement(stmt:String) = {
    super.setStatement(stmt)
    this
  }

  /**
   * Executes the task against the RecordServiceWorker and returns an iterator to fetch
   * result for the entire task.
   */
  override def compute(split: Partition, context: TaskContext):
      InterruptibleIterator[Array[Writable]] = {
    val iter = new NextIterator[Array[Writable]] {
      val partition = split.asInstanceOf[RecordServicePartition]

      // Reusable writable objects.
      var writables = new Array[Writable](partition.schema.cols.size())

      // Array for return values. value[i] = writables[i] if the value is non-null
      var value = new Array[Writable](partition.schema.cols.size())

      // Register an on-task-completion callback to close the input stream.
      context.addTaskCompletionListener{ context => closeIfNeeded() }

      var (worker, rows) = execTask(partition)

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

      override def getNext() : Array[Writable] = {
        if (!rows.hasNext()) {
          finished = true
          return value
        }

        // Reconstruct the row
        val row = rows.next()
        for (i <- 0 until writables.length) {
          if (row.isNull(i)) {
            value(i) = null
          } else {
            value(i) = writables(i)
            partition.schema.cols.get(i).getType().type_id match {
              case TTypeId.BOOLEAN =>
                value(i).asInstanceOf[BooleanWritable].set(row.getBoolean(i))
              case TTypeId.TINYINT =>
                value(i).asInstanceOf[ByteWritable].set(row.getByte(i))
              case TTypeId.SMALLINT =>
                value(i).asInstanceOf[ShortWritable].set(row.getShort(i))
              case TTypeId.INT =>
                value(i).asInstanceOf[IntWritable].set(row.getInt(i))
              case TTypeId.BIGINT =>
                value(i).asInstanceOf[LongWritable].set(row.getLong(i))
              case TTypeId.FLOAT =>
                value(i).asInstanceOf[FloatWritable].set(row.getFloat(i))
              case TTypeId.DOUBLE =>
                value(i).asInstanceOf[DoubleWritable].set(row.getDouble(i))
              case TTypeId.STRING =>
                // TODO: ensure this doesn't copy.
                val v = row.getByteArray(i)
                value(i).asInstanceOf[Text].set(
                    v.byteBuffer().array(), v.offset(), v.len())
              case _ => assert(false)
            }
          }
        }
        value
      }

      override def close() = {
        if (rows != null) {
          rows.close()
          rows = null
        }
        if (worker != null) {
          worker.close()
          worker = null
        }
      }
    }

    new InterruptibleIterator[Array[Writable]](context, iter)
  }

  /**
   * Sends the request to the RecordServicePlanner to generate the list of partitions
   * (tasks in RecordService terminology)
   */
  override protected def getPartitions: Array[Partition] = {
    super.planRequest._2
  }
}
