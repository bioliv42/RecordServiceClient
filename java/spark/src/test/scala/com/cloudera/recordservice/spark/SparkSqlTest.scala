// Confidential Cloudera Information: Covered by NDA.
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

import org.apache.spark.sql.Row
import org.scalatest.{FunSuite}

// TODO: more tests
//  Add NULLS, error tests, filter tests
class SparkSqlTest extends FunSuite with SharedSparkSQLContext {
  test("Nation Test") {
    sc.sql(s"""
        |CREATE TEMPORARY TABLE nationTbl
        |USING com.cloudera.recordservice.spark.DefaultSource
        |OPTIONS (
        |  record_service_table 'tpch.nation'
        |)
      """.stripMargin)

    // Try a count(*)
    assert (sc.sql("SELECT count(*) from nationTbl").collect()(0).getLong(0) == 25)
    assert (sc.sql("SELECT count(*) from nationTbl where n_nationkey = 1").
        collect()(0).getLong(0) == 1)

    // Scan the whole table
    var row = sc.sql("SELECT * from nationTbl").first()
    assert(row.get(0) == 0)
    assert(row.get(1) == "ALGERIA")
    assert(row.get(2) == 0)
    assert(row.get(3) == " haggle. carefully final deposits detect slyly agai")

    // Project two columns
    row = sc.sql("SELECT n_comment, n_name from nationTbl").collect()(5)
    assert(row.get(0) == "ven packages wake quickly. regu")
    assert(row.get(1) == "ETHIOPIA")
  }

  test("Predicate pushdown") {
    sc.sql( s"""
      |CREATE TEMPORARY TABLE nationTbl
      |USING com.cloudera.recordservice.spark.DefaultSource
      |OPTIONS (
      |  record_service_table 'tpch.nation'
      |)
    """.stripMargin)

    var row:Row = null

    row = sc.sql("SELECT count(*) from nationTbl where n_nationkey > 10").collect()(0)
    assert(row.get(0) == 14)

    row = sc.sql(
      "SELECT count(*) from nationTbl where n_nationkey = 10 OR n_nationkey = 1")
      .collect()(0)
    assert(row.get(0) == 2)

    row = sc.sql(
      "SELECT count(*) from nationTbl where n_nationkey = 10 AND n_nationkey = 1")
      .collect()(0)
    assert(row.get(0) == 0)
  }

  test("DataFrame Test") {
    val df = sc.load("tpch.nation", "com.cloudera.recordservice.spark.DefaultSource")
    // SELECT n_regionkey, count(*) FROM tpch.nation GROUP BY 1 ORDER BY 1
    val result = df.groupBy("n_regionkey").count().orderBy("n_regionkey").collect()
    assert(result.length == 5)
    assert(result(0).toString() == "[0,5]")
    assert(result(1).toString() == "[1,5]")
    assert(result(2).toString() == "[2,5]")
    assert(result(3).toString() == "[3,5]")
    assert(result(4).toString() == "[4,5]")
  }
}
