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

import java.lang.reflect.Method

import com.cloudera.recordservice.thrift._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.thrift.TException
import org.apache.thrift.protocol.{TProtocol, TBinaryProtocol}
import org.apache.thrift.transport.{TSocket}

import scala.reflect.ClassTag

/**
 * RDD that is backed by the RecordService. This returns an RDD of case class objects.
 * The caller passes the case class that they'd like to use. This class uses reflection
 * to populate the case class objects.
 *
 * Example:
 * case class Nation(var key:Long, var name:String)
 * val data:RDD[Nation] = sc.recordServiceRecords[Nation]("tpch.nation")
 *
 * The schema and specified case class can be resolved either by ordinal or by name.
 *
 * If by ordinal, the ith field of the case class must match the type of the ith field of
 * the RecordService row. i.e. the case class types has to be a prefix of the query's
 * result types.
 * The names of the field in the case class are ignored.
 *
 * If by name, every field in the case class must exist in the query's result and the
 * types of those fields must match. Matching is case insensitive.
 *
 * TODO: redo this using the recordservice client/recordservice MR lib
 */
class SchemaRecordServiceRDD[T:ClassTag](sc: SparkContext,
                                         recordClass:Class[T],
                                         byOrdinal:Boolean = false)
    extends RDD[T](sc, Nil) with Logging {
  // Why doesn't classOf[T] work (and then you don't need to pass the recordClass arg)

  def setStatement(stmt:String) = {
    this.stmt = stmt
    this
  }

  def setTable(table:String) = {
    if (byOrdinal) {
      // TODO: add API to RecordService to get the table schema so we can do projection
      this.stmt = "SELECT * from " + table
    } else {
      val sb = new StringBuilder("SELECT ")
      for (i <- 0 until fields.length) {
        if (i != 0) sb.append(",")
        sb.append(" " + fields(i))
      }
      sb.append(" FROM " + table)
      this.stmt = sb.toString()
    }
    this
  }

  val PLANNER_PORT: Int = 40000
  val WORKER_PORT: Int = 40100
  var stmt:String = null
  var fields:Array[String] = extractFields()
  var types:Array[TTypeId] = extractTypes()

  private def extractFields() = {
    val f = recordClass.getDeclaredFields()
    val result = new Array[String](f.size)
    val allMethods = recordClass.getMethods()

    for (i <- 0 until f.length) {
      result(i) = f(i).getName()

      // Verify that the field is declared as 'var'. This means there is a
      // generated field_$eq method.
      val setter = f(i).getName + "_$eq"
      if (allMethods.find(_.getName() == setter) == None) {
        throw new SparkException("Incompatible Schema. Fields in case class " +
          "must be 'var'. Incorrect field: " + result(i))
      }
    }
    result
  }

  private def extractTypes() = {
    val f = recordClass.getDeclaredFields()
    val result = new Array[TTypeId](f.size)
    for (i <- 0 until f.length) {
      if (f(i).getType.getName == "boolean") {
        result(i) = TTypeId.BOOLEAN
      } else if (f(i).getType.getName == "byte") {
        result(i) = TTypeId.TINYINT
      } else if (f(i).getType.getName == "char") {
        result(i) = TTypeId.TINYINT
      } else if (f(i).getType.getName == "short") {
        result(i) = TTypeId.SMALLINT
      } else if (f(i).getType.getName == "int") {
        result(i) = TTypeId.INT
      } else if (f(i).getType.getName == "long") {
        result(i) = TTypeId.BIGINT
      } else if (f(i).getType.getName == "float") {
        result(i) = TTypeId.FLOAT
      } else if (f(i).getType.getName == "double") {
        result(i) = TTypeId.DOUBLE
      } else if (f(i).getType.getName == "java.lang.String") {
        result(i) = TTypeId.STRING
      } else {
        throw new SparkException("Case class uses types that are unsupported. " +
          "Only basic types and String are supported. Type=" + f(i).getType().getName())
      }
    }
    result
  }

  private def printSchema(schema:TSchema) = {
    val builder:StringBuilder = new StringBuilder("schema: {\n")
    for (i <- 0 until schema.cols.size()) {
      builder.append("  ")
             .append(schema.cols.get(i).name)
             .append(":")
             .append(schema.cols.get(i).getType.type_id)
             .append("\n")
    }
    builder.append("}")
    builder.toString()
  }

  private def verifySchema(schema: TSchema) = {
    if (schema.cols.size() < fields.length) {
      // TODO: default values?
      throw new SparkException("Schema mismatch. Cannot match if the case class " +
        " contains more fields than the table")
    }

    if (byOrdinal) {
      for (i <- 0 until fields.length) {
        if (types(i) != schema.cols.get(i).getType.type_id) {
          throw new SparkException("Schema mismatch. The type of field '" + fields(i) +
            "' does not match the result type. " +
             "Expected type: " + types(i) + " Actual type: " +
            schema.cols.get(i).getType.type_id)
        }
      }
    } else {
      for (i <- 0 until fields.length) {
        for (j <- 0 until i) {
          if (fields(i).equalsIgnoreCase(fields(j))) {
            throw new SparkException("Invalid case class. When matching by name, " +
              "fields cannot have the same case-insensitive name")
          }
        }

        var found = false
        for (j <- 0 until schema.cols.size()) {
          if (fields(i).equalsIgnoreCase(schema.cols.get(j).name)) {
            found = true
            if (types(i) != schema.cols.get(j).getType.type_id) {
              throw new SparkException("Schema mismatch. The type of field '" +
                fields(i) + "' does not match the result type. " +
                "Expected type: " + types(i) + " Actual type: " +
                schema.cols.get(j).getType.type_id)
            }
          }
        }
        if (!found) {
          // TODO: print schema
          throw new SparkException("Schema mismatch. Field in case class '" + fields(i) +
            "' did not match any field in the result schema:\n" + printSchema(schema))
        }
      }
    }
  }

  private def createConnection(host: String, port: Int) : TProtocol = {
    val transport = new TSocket(host, port)
    transport.open()
    new TBinaryProtocol(transport)
  }

  // Creates an object of type T, using reflection to call the constructor.
  private def createObject() : T = {
    val ctor = recordClass.getConstructors()(0)
    val numArgs = ctor.getParameterTypes().size
    val args = new Array[AnyRef](numArgs)
    for (i <- 0 until numArgs) {
      if (ctor.getParameterTypes()(i).getName == "boolean") {
        args(i) = new java.lang.Boolean(false)
      } else if (ctor.getParameterTypes()(i).getName == "byte") {
        args(i) = new java.lang.Byte(0.toByte)
      } else if (ctor.getParameterTypes()(i).getName == "char") {
        args(i) = new Character('0')
      } else if (ctor.getParameterTypes()(i).getName == "short") {
        args(i) = new java.lang.Short(0.toShort)
      } else if (ctor.getParameterTypes()(i).getName == "int") {
        args(i) = new java.lang.Integer(0)
      } else if (ctor.getParameterTypes()(i).getName == "long") {
        args(i) = new java.lang.Long(0)
      } else if (ctor.getParameterTypes()(i).getName == "float") {
        args(i) = new java.lang.Float(0)
      } else if (ctor.getParameterTypes()(i).getName == "double") {
        args(i) = new java.lang.Double(0)
      } else if (ctor.getParameterTypes()(i).getName == "java.lang.String") {
        args(i) = new String("")
      } else {
        throw new RuntimeException("Unsupported type: " +
            ctor.getParameterTypes()(i).getName)
      }
    }
    ctor.newInstance(args:_*).asInstanceOf[T]
  }

  private class SchemaRecordServicePartition(rddId: Int, idx: Int,
                                             h:String, t: TTask, s: TSchema)
    extends Partition {
    override def hashCode(): Int = 41 * (41 + rddId) + idx
    override val index: Int = idx
    val task: TTask = t
    val schema: TSchema = s
    val host: String = h
  }

  private class RecordServiceIterator(partition: SchemaRecordServicePartition)
      extends NextIterator[T] {
    var worker: RecordServiceWorker.Client = null

    // Current batch, idx and batch size we are returning.
    var rowBatch: TColumnarRowBatch = null
    var rowIdx: Int = 0

    // For each column, the index into the column data.
    var colIdx = new Array[Int](partition.schema.cols.size())
    var batchSize: Int = 0

    // The object to return in getNext(). We always return the same object
    // and just update the value for each row.
    var value:T = createObject()

    // The array of setters to populate 'value'. This is always indexed by the ordinal
    // returned by the record service.
    var setters:Array[Method] = new Array[Method](partition.schema.cols.size())

    val allMethods = value.getClass.getMethods()
    if (byOrdinal) {
      val declaredFields = value.getClass.getDeclaredFields()
      for (i <- 0 until declaredFields.length) {
        val setter = declaredFields(i).getName + "_$eq"
        val setterMethod = allMethods.find(_.getName() == setter)
        assert (setterMethod != None)
        setters(i) = setterMethod.get
      }
    } else {
      // Resolve the order of cols. e.g. the result from the record service could be
      // { name, key } but the case class is
      // { key, name }.
      // We know from earlier validation that the case class has to be a subset of
      // the result from the record service.
      // TODO: it should be equal to the record service, we should do additional
      // projection for the client.
      for (i <- 0 until partition.schema.cols.size()) {
        val resultColName = partition.schema.cols.get(i).name
        for (j <- 0 until fields.length) {
          if (resultColName.equalsIgnoreCase(fields(j))) {
            val setter = fields(j) + "_$eq"
            val setterMethod = allMethods.find(_.getName() == setter)
            assert (setterMethod != None)
            setters(i) = setterMethod.get
          }
        }
      }
    }

    val execResult = try {
      worker = new RecordServiceWorker.Client(
        createConnection(partition.host, WORKER_PORT))
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
        for (i <- 0 until fields.length) {
          colIdx(i) = 0
        }
      }
    }

    override def getNext() : T = {
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
      // TODO: handle NULLs
      for (i <- 0 until setters.length) {
        if (setters(i) != null) {
          partition.schema.cols.get(i).getType().type_id match {
            case TTypeId.BOOLEAN => setters(i).invoke(value,
              rowBatch.cols.get(i).bool_vals.get(colIdx(i)))
            case TTypeId.TINYINT => setters(i).invoke(value,
              rowBatch.cols.get(i).byte_vals.get(colIdx(i)))
            case TTypeId.SMALLINT => setters(i).invoke(value,
              rowBatch.cols.get(i).short_vals.get(colIdx(i)))
            case TTypeId.INT => setters(i).invoke(value,
              rowBatch.cols.get(i).int_vals.get(colIdx(i)))
            case TTypeId.BIGINT => setters(i).invoke(value,
              rowBatch.cols.get(i).long_vals.get(colIdx(i)))
            /*
             * TODO: how do get scala to box these?
          case TTypeId.FLOAT => setters(i).invoke(value,
            rowBatch.cols.get(i).double_vals.get(colIdx(i)).toFloat)
          case TTypeId.DOUBLE => setters(i).invoke(value,
            rowBatch.cols.get(i).double_vals.get(colIdx(i)))
            */
            case TTypeId.STRING => setters(i).invoke(value,
              rowBatch.cols.get(i).string_vals.get(colIdx(i)))
            case _ => assert(false)
          }
        }
        colIdx(i) = colIdx(i) + 1
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

  /**
   * Executes the task against the RecordServiceWorker and returns an iterator to fetch
   * result for the entire task.
   */
  override def compute(split: Partition, context: TaskContext):
      InterruptibleIterator[T] = {
    new InterruptibleIterator[T](context,
        new RecordServiceIterator(split.asInstanceOf[SchemaRecordServicePartition]))
  }

  /**
   * Sends the request to the RecordServicePlanner to generate the list of partitions
   * (tasks in RecordService terminology)
   * TODO: How does this handle locality.
   */
  override protected def getPartitions: Array[Partition] = {
    if (stmt == null) throw new SparkException("Statement not set.")
    logInfo("Request: " + stmt)
    val planResult = try {
      val planner = new RecordServicePlanner.Client(
        createConnection("localhost", PLANNER_PORT))
      val request = new TPlanRequestParams()
      request.setSql_stmt(stmt)
      planner.PlanRequest(request)
    } catch {
      case e:TRecordServiceException => logError("Could not plan request: " + e.message)
        throw new SparkException("RecordServiceRDD failed", e)
      case e:TException => logError("Could not plan request: " + e.getMessage())
        throw new SparkException("RecordServiceRDD failed", e)
    }

    // TODO: verify that T is not an inner class, Spark shell generates it that way.
    verifySchema(planResult.schema)
    logInfo("Schema matched")

    val partitions = new Array[Partition](planResult.tasks.size())
    for (i <- 0 until planResult.tasks.size()) {
      partitions(i) = new SchemaRecordServicePartition(
        id, i, planResult.tasks.get(i).hosts.get(0),
        planResult.tasks.get(i), planResult.schema)
    }
    partitions
  }
}
