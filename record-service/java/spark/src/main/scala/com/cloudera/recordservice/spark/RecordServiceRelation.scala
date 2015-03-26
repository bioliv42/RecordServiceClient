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
 * TODO: this should not go through the RecordServiceRDD but instead go to the client
 * API directly. We save an extra copy/row format conversion.
 * TODO: support other types
 */
case class RecordServiceRelation(table:String)(@transient val sqlContext:SQLContext)
    extends BaseRelation with PrunedFilteredScan with Logging {
  // TODO: pull from configs
  val PLANNER_HOST = "localhost"
  val PLANNER_PORT = 40000

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

  override def schema: StructType = {
    val rsSchema = RecordServicePlannerClient.getSchema(
      PLANNER_HOST, PLANNER_PORT, Request.createTableRequest(table)).schema
    convertSchema(rsSchema)
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
  private def compileFilter(f: Filter): String = f match {
    case EqualTo(attr, value) => s"$attr = ${compileValue(value)}"
    case LessThan(attr, value) => s"$attr < ${compileValue(value)}"
    case GreaterThan(attr, value) => s"$attr > ${compileValue(value)}"
    case LessThanOrEqual(attr, value) => s"$attr <= ${compileValue(value)}"
    case GreaterThanOrEqual(attr, value) => s"$attr >= ${compileValue(value)}"
    case _ => null
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
    if (requiredColumns.length == 0) {
      // TODO: handle no projected columns.
      throw new SparkException("Not implemented: empty projection.")
    }

    val sb:StringBuilder = new StringBuilder
    sb.append("SELECT ")
    for (i <- 0 until requiredColumns.length) {
      if (i != 0) sb.append(", ")
      sb.append(requiredColumns(i))
    }
    sb.append(" FROM " + table)
    sb.append(" " + filterWhereClause(filters))
    System.out.println(sb.toString())

    val baseRDD =
      new RecordServiceRDD(sqlContext.sparkContext).setStatement(sb.toString())

    val fieldMap = Map(schema.fields map { x => x.metadata.getString("name") -> x }: _*)
    val projectedSchema = new StructType(requiredColumns map { name => fieldMap(name) })

    val mutableRow = new SpecificMutableRow(projectedSchema.fields.map(x => x.dataType))
    val numCols = requiredColumns.size

    // Map the result from the record service RDD to a MutableRow
    baseRDD.map(x => {
      val rsSchema = baseRDD.getSchema()
      for (i <- 0 until numCols) {
        val t:TType = rsSchema.cols.get(i).getType
        val v = x(i)
        if (v == null) {
          mutableRow.setNullAt(i)
        } else {
          t.type_id match {
            case TTypeId.BOOLEAN =>
              mutableRow.setBoolean(i, v.asInstanceOf[BooleanWritable].get())
            case TTypeId.TINYINT =>
              mutableRow.setInt(i, v.asInstanceOf[ByteWritable].get().toInt)
            case TTypeId.SMALLINT =>
              mutableRow.setInt(i, v.asInstanceOf[ShortWritable].get().toInt)
            case TTypeId.INT =>
              mutableRow.setInt(i, v.asInstanceOf[IntWritable].get())
            case TTypeId.BIGINT =>
              mutableRow.setLong(i, v.asInstanceOf[LongWritable].get())
            case TTypeId.FLOAT =>
              mutableRow.setFloat(i, v.asInstanceOf[FloatWritable].get())
            case TTypeId.DOUBLE =>
              mutableRow.setDouble(i, v.asInstanceOf[DoubleWritable].get())
            case TTypeId.STRING =>
              mutableRow.setString(i, v.asInstanceOf[Text].toString)
            case _ => assert(false)
          }
        }
      }
      mutableRow
    })
  }
}

class DefaultSource extends RelationProvider {
  val TABLE_KEY:String = "record_service_table"

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

    if (path.isDefined) table = path
    RecordServiceRelation(table.get)(sqlContext)
  }
}

