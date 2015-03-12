// Copyright (c) 2012 Cloudera, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.recordservice.spark

import org.apache.hadoop.io._
import org.apache.spark.SparkException
import org.scalatest.FunSuite

// It's important that these classes and helpers are defined outside of the test class
// to ensure that spark can serialize the task.
case class AllTypes(
 val boolCol: Option[Boolean],
 val tinyIntCol: Option[Byte],
 val smallIntCol: Option[Short],
 val intCol: Option[Int],
 val bigintCol: Option[Long],
 val floatCol: Option[Float],
 val doubleCol: Option[Double],
 val stringCol: Option[String],
 val vcharCol: Option[String],
 val charCol: Option[String]
)

object Helpers {
  def allTypesFromWritables(m: Array[Writable]) : AllTypes = {
    new AllTypes(
      if (m(0) == null) None else Some(m(0).asInstanceOf[BooleanWritable].get()),
      if (m(1) == null) None else Some(m(1).asInstanceOf[ByteWritable].get()),
      if (m(2) == null) None else Some(m(2).asInstanceOf[ShortWritable].get()),
      if (m(3) == null) None else Some(m(3).asInstanceOf[IntWritable].get()),
      if (m(4) == null) None else Some(m(4).asInstanceOf[LongWritable].get()),
      if (m(5) == null) None else Some(m(5).asInstanceOf[FloatWritable].get()),
      if (m(6) == null) None else Some(m(6).asInstanceOf[DoubleWritable].get()),
      if (m(7) == null) None else Some(m(7).asInstanceOf[Text].toString),
      if (m(8) == null) None else Some(m(8).asInstanceOf[Text].toString),
      if (m(9) == null) None else Some(m(9).asInstanceOf[Text].toString))
  }
}

class BasicClient extends FunSuite with SharedSparkContext {
  test("NationTest") {
    val rdd = new RecordServiceRDD(sc).setStatement("select * from tpch.nation")
    assert(rdd.count() == 25)

    val col1Vals = rdd.map(d => (d(0).asInstanceOf[ShortWritable].get().toInt)).collect()
    val col2Vals = rdd.map(d => (d(1).asInstanceOf[Text].toString)).collect()
    val col3Vals = rdd.map(d => (d(2).asInstanceOf[ShortWritable].get().toInt)).collect()
    val col4Vals = rdd.map(d => (d(3).asInstanceOf[Text].toString)).collect()

    assert(col1Vals.length == 25)
    assert(col2Vals.length == 25)
    assert(col3Vals.length == 25)
    assert(col4Vals.length == 25)

    assert(col1Vals(1) == 1)
    assert(col2Vals(2) == "BRAZIL")
    assert(col3Vals(3) == 1)
    assert(col4Vals(4) == "y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d")

    assert(col1Vals.reduce(_ + _) == 300)
    assert(col3Vals.reduce(_ + _) == 50)
    assert(col2Vals.reduce( (x,y) => if (x < y) x else y) == "ALGERIA")
    assert(col4Vals.reduce( (x,y) => if (x > y) x else y) == "y final packages. slow foxes cajole quickly. quickly silent platelets breach ironic accounts. unusual pinto be")
  }

  test("AllTypesTest") {
    val rdd = new RecordServiceRDD(sc).setTable("rs.alltypes")
    assert(rdd.count() == 2)

    val results = rdd.map(m => Helpers.allTypesFromWritables(m)).
        sortBy(k => k.tinyIntCol).collect()

    assert(results.length == 2)
    assert(results(0).equals(new AllTypes(Some(true), Some(0), Some(1), Some(2), Some(3),
        Some(4.0f), Some(5.0), Some("hello"), Some("vchar1"), Some("char1"))))
    assert(results(1).equals(new AllTypes(Some(false), Some(6), Some(7), Some(8), Some(9),
      Some(10.0f), Some(11.0), Some("world"), Some("vchar2"), Some("char2"))))
  }

  test("AllTypesNullTest") {
    val rdd = new RecordServiceRDD(sc).setTable("rs.alltypes_null")
    assert(rdd.count() == 1)

    val results = rdd.map(m => Helpers.allTypesFromWritables(m)).collect()

    assert(results.length == 1)
    assert(results(0).equals(
      new AllTypes(None, None, None, None, None, None, None, None, None, None)))
  }

  test("Nation By Path") {
    val rdd = new RecordServiceRDD(sc).setPath("/test-warehouse/tpch.nation/")
    assert(rdd.count() == 25)
    val results = rdd.map(v => v(0).asInstanceOf[Text].toString).collect()
    assert(results.length == 25)
    assert(results(10) == "10|IRAN|4|efully alongside of the slyly final dependencies. ")
  }

  test("Invalid Request") {
    val rdd = new RecordServiceRDD(sc)
    var threwException = false
    try {
      rdd.count()
    } catch {
      case e:SparkException =>
        threwException = true
        assert(e.getMessage.contains("Request not set"))
    }
    assert(threwException)

    rdd.setStatement("select 1")
    threwException = false
    try {
      rdd.setTable("foo")
    } catch {
      case e:SparkException =>
        threwException = true
        assert(e.getMessage.contains("Statement already set"))
    }
    assert(threwException)

    threwException = false
    try {
      rdd.setPath("/a/b/c")
    } catch {
      case e:SparkException =>
        threwException = true
        assert(e.getMessage.contains("Statement already set"))
    }
    assert(threwException)
  }
}
