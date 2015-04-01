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

import org.apache.commons.lang.StringEscapeUtils.escapeSql

import com.cloudera.recordservice.client.{Request, RecordServicePlannerClient}
import com.cloudera.recordservice.thrift.{TSchema, TTypeId, TType}
import org.apache.hadoop.io._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.SpecificMutableRow
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{Logging, SparkException}

/**
 * SparkSQL integration with the RecordService.
 *
 * Example usage is:
 * sc.sql(s"""
        |CREATE TEMPORARY TABLE nationTbl
        |USING com.cloudera.recordservice.spark.DefaultSource
        |OPTIONS (
        |  record_service_table 'tpch.nation'
        |)
      """.stripMargin)
 * sc.sql("select * from nationTbl")
 *
 * TODO: support other types
 * TODO: table "stats" are passed in the ctor. Add RecordService API to get stats.
 * SparkSQL currently only supports table size (in bytes).
 */
case class RecordServiceRelation(table:String, size:Option[Long])(
        @transient val sqlContext:SQLContext)
    extends BaseRelation with PrunedFilteredScan with Logging {

  val (plannerHost, plannerPort) = RecordServiceConf.getPlannerHostPort(sqlContext)

  override def schema: StructType = {
    val rsSchema = RecordServicePlannerClient.getSchema(
      plannerHost, plannerPort, Request.createTableScanRequest(table)).schema
    convertSchema(rsSchema)
  }

  override val sizeInBytes =
    if (size.isDefined) {
      size.get
    } else {
      super.sizeInBytes
    }

  def remapType(rsType:TType) : DataType = {
    val result = rsType.type_id match {
      case TTypeId.BOOLEAN => BooleanType
      case TTypeId.TINYINT => IntegerType
      case TTypeId.SMALLINT => IntegerType
      case TTypeId.INT => IntegerType
      case TTypeId.BIGINT => LongType
      case TTypeId.FLOAT => FloatType
      case TTypeId.DOUBLE => DoubleType
      case TTypeId.STRING => StringType
      case TTypeId.DECIMAL => DataTypes.createDecimalType(rsType.precision, rsType.scale)
      case TTypeId.TIMESTAMP_NANOS => DataTypes.TimestampType
      case _ => null
    }
    if (result == null) throw new SparkException("Unsupported type " + rsType)
    result
  }

  def convertSchema(rsSchema: TSchema): StructType = {
    val fields = new Array[StructField](rsSchema.getCols.size())
    for (i <- 0 until rsSchema.getCols.size()) {
      val colName = rsSchema.cols.get(i).name
      val metadata = new MetadataBuilder().putString("name", colName)
      val colType = remapType(rsSchema.cols.get(i).getType)
      fields(i) = StructField(colName, colType, true, metadata.build())
    }
    new StructType(fields)
  }

  /**
   * Converts value to SQL expression.
   * Taken from JdbcRDD
   */
  private def compileValue(value: Any): Any = value match {
    case stringValue: String => s"'${escapeSql(stringValue)}'"
    case _ => value
  }

  /**
   * Turns a single Filter into a String representing a SQL expression.
   * Returns null for an unhandled filter.
   * Taken from JdbcRDD
   * TODO: can we support even more filters?
   */
  private def compileFilter(f: Filter): String =
    f match {
      case EqualTo(attr, value) => s"$attr = ${compileValue(value)}"
      case LessThan(attr, value) => s"$attr < ${compileValue(value)}"
      case GreaterThan(attr, value) => s"$attr > ${compileValue(value)}"
      case LessThanOrEqual(attr, value) => s"$attr <= ${compileValue(value)}"
      case GreaterThanOrEqual(attr, value) => s"$attr >= ${compileValue(value)}"
      case Or(left, right) =>
        val leftString = compileFilter(left)
        val rightString = compileFilter(right)
        if (leftString == null || rightString == null) {
          null
        } else {
          "(" + leftString + " OR " + rightString + ")"
        }
      case And(left, right) =>
        val leftString = compileFilter(left)
        val rightString = compileFilter(right)
        if (leftString == null || rightString == null) {
          null
        } else {
          "(" + leftString + " AND " + rightString + ")"
        }
      case _ =>
        logWarning("Skipping filter: " + f)
        null
    }

  /**
   * `filters`, but as a WHERE clause suitable for injection into a SQL query.
   * Taken from JdbcRDD
   */
  private def filterWhereClause(filters: Array[Filter]) : String = {
    val filterStrings = filters map compileFilter filter (_ != null)
    if (filterStrings.size > 0) {
      val sb = new StringBuilder("WHERE ")
      filterStrings.foreach(x => sb.append(x).append(" AND "))
      sb.substring(0, sb.length - 5)
    } else ""
  }

  override def buildScan(requiredColumns: Array[String],
      filters: Array[Filter]): RDD[Row] = {
    val baseRDD = new RecordServiceRecordRDD(sqlContext.sparkContext)

    var emptyProjection = false
    if (requiredColumns.isEmpty && filters.isEmpty) {
      // Empty projection
      emptyProjection = true
      baseRDD.setRequest(Request.createProjectionRequest(table, null))
    } else {
      val sb: StringBuilder = new StringBuilder
      sb.append("SELECT ")
      for (i <- 0 until requiredColumns.length) {
        if (i != 0) sb.append(", ")
        sb.append(requiredColumns(i))
      }
      sb.append(" FROM " + table)
      sb.append(" " + filterWhereClause(filters))
      baseRDD.setStatement(sb.toString())
    }
    baseRDD.setPlannerHostPort(plannerHost, plannerPort)

    if (emptyProjection) {
      // We have an empty projection so we've mapped this to a count(*) in the
      // RecordService. (For NULLs, we need to do this for correctness). Here we
      // are going to expand it to return a NULL for each row.
      baseRDD.mapPartitions(input => {
        val record = input.next()
        assert(record.getSchema().cols.size() == 1)
        assert(record.getSchema().cols.get(0).getType.type_id == TTypeId.BIGINT)
        var numRows = record.getLong(0)
        new Iterator[Row] {
          override def next(): Row = {
            numRows -= 1
            null
          }
          override def hasNext: Boolean = {
            numRows > 0
          }
        }
      })
    } else {
      val fieldMap = Map(schema.fields map { x => x.metadata.getString("name") -> x }: _*)
      val projectedSchema = new StructType(requiredColumns map { name => fieldMap(name) })

      val mutableRow = new SpecificMutableRow(projectedSchema.fields.map(x => x.dataType))
      val numCols = requiredColumns.size

      // Map the result from the record service RDD to a MutableRow
      baseRDD.map(x => {
        val rsSchema = baseRDD.getSchema()
        for (i <- 0 until numCols) {
          if (x.isNull(i)) {
            mutableRow.setNullAt(i)
          } else {
            rsSchema.cols.get(i).getType.type_id match {
              case TTypeId.BOOLEAN => mutableRow.setBoolean(i, x.getBoolean(i))
              case TTypeId.TINYINT => mutableRow.setInt(i, x.getByte(i))
              case TTypeId.SMALLINT => mutableRow.setInt(i, x.getShort(i).toInt)
              case TTypeId.INT => mutableRow.setInt(i, x.getInt(i))
              case TTypeId.BIGINT => mutableRow.setLong(i, x.getLong(i))
              case TTypeId.FLOAT => mutableRow.setFloat(i, x.getFloat(i))
              case TTypeId.DOUBLE => mutableRow.setDouble(i, x.getDouble(i))
              case TTypeId.STRING => mutableRow.setString(i, x.getByteArray(i).toString)
              case TTypeId.DECIMAL =>
                val d = x.getDecimal(i)
                mutableRow.update(i,
                  Decimal(d.toBigDecimal, d.getPrecision, d.getScale))
              case TTypeId.TIMESTAMP_NANOS =>
                  mutableRow.update(i, x.getTimestampNanos(i).toTimeStamp)
              case _ => assert(false)
            }
          }
        }
        mutableRow
      })
    }
  }
}

class DefaultSource extends RelationProvider {
  val TABLE_KEY:String = "record_service_table"
  val TABLE_SIZE_KEY:String = "record_service_table_size"

  override def createRelation(
      sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    // TODO: we currently just map record_service_table and path to the same thing.
    // A lot of the nice machinery in sparkSQL uses 'path'.
    // TODO: support path correctly (the actual path).
    var table = parameters.get(TABLE_KEY)
    val path = parameters.get("path")
    if (table.isEmpty && path.isEmpty) {
      throw new SparkException("Must specify 'record_service_table' or 'path'")
    }
    if (table.isDefined && path.isDefined) {
      throw new SparkException("Cannot specify both 'record_service_table' and 'path'")
    }

    val sizeVal = parameters.get(TABLE_SIZE_KEY)
    val size = if (sizeVal.isDefined) Some(sizeVal.get.toLong) else None

    if (path.isDefined) table = path
    RecordServiceRelation(table.get, size)(sqlContext)
  }
}

