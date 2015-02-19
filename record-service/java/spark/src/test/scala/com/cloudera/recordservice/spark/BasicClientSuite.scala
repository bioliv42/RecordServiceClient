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

import org.apache.hadoop.io.ShortWritable
import org.apache.hadoop.io.Text
import org.scalatest.FunSuite

class BasicClient extends FunSuite with SharedSparkContext {
  test("NationTest") {
    val rdd = new RecordServiceRDD(sc, "select * from tpch.nation")
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
}
