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

import org.apache.hadoop.io.LongWritable
import org.scalatest.FunSuite

// It's important that these classes and helpers are defined outside of the test class
// to ensure that spark can serialize the task.
case class Nation(var key:Short, var name:String,
                  var regionKey:Short, var comment:String)

case class AllTypesProjection(var int_col:Int, var string_col:String)

case class Query1(var v:Long)

// TODO: add error tests.
class SchemaClientSuite extends FunSuite with SharedSparkContext {
  test("NationTest") {
    val rdd = new SchemaRecordServiceRDD[Nation](sc, classOf[Nation], true).
      setTable("tpch.nation")
    assert(rdd.count() == 25)

    assert(rdd.map(m => m.key.toInt).reduce(_ + _) == 300)
    assert(rdd.map(m => m.regionKey.toInt).reduce(_ + _) == 50)
    assert(rdd.map(m => m.name).reduce( (x,y) => if (x < y) x else y) == "ALGERIA")
    assert(rdd.map(m => m.comment).reduce( (x,y) => if (x > y) x else y) == "y final packages. slow foxes cajole quickly. quickly silent platelets breach ironic accounts. unusual pinto be")
  }

  test("DefaultValues") {
    val data = new SchemaRecordServiceRDD[AllTypesProjection](
      sc, classOf[AllTypesProjection], false)
      .setTable("rs.alltypes_null")
      // Here we set the default. Any time we see NULL in the first column, we populate
      // it with -1. Any time in the second column, we populate with "Empty"
      .setDefaultValue(new AllTypesProjection(-1, "Empty"))
    assert(data.first().int_col == -1)
    assert(data.first().string_col == "Empty")
  }

  test("IgnoreNulls") {
    val data = new SchemaRecordServiceRDD[AllTypesProjection](
      sc, classOf[AllTypesProjection], false)
      .setTable("rs.alltypes_null")
      .setIgnoreUnhandledNull(true)
    assert(data.count() == 0)
  }

  /*
  // TODO: move this somewhere else. We don't want to run this as a test.
  test("PerfTest") {
    val rdd = new RecordServiceRDD(sc, "select l_partkey from tpch10gb_parquet.lineitem")
    for (i <- 0 until 5) {
      val start = System.currentTimeMillis()
      System.out.println(rdd.map(v => v(0).asInstanceOf[LongWritable].get).reduce(_ + _))
      System.out.println("Duration " + (System.currentTimeMillis() - start))
    }
  }
  */
}
