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

import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

// TODO: add error tests.
class KerberosTestSuite extends FunSuite with BeforeAndAfterAll {
  @transient private var _sc: SparkContext = _

  def sc: SparkContext = _sc
  def ENABLE_KERBEROS_TESTS:Boolean =
      System.getenv("RECORD_SERVICE_RUN_KERBEROS_TESTS") == "true"

  val conf = new SparkConf(false)
    .set(RecordServiceConf.RECORD_SERVICE_PLANNER_HOST_KEY, "vd0224.halxg.cloudera.com")
    .set(RecordServiceConf.RECORD_SERVICE_KERBEROS_PRINCIPAL_KEY,
        "impala/vd0224.halxg.cloudera.com@HALXG.CLOUDERA.COM")

  override def beforeAll() {
    super.beforeAll()
    _sc = new SparkContext("local", "test", conf)
  }

  override def afterAll() {
    LocalSparkContext.stop(_sc)
    _sc = null
    super.afterAll()
  }

  test("NationTest") {
    if (ENABLE_KERBEROS_TESTS) {
      val rdd = new RecordServiceRecordRDD(sc).setTable("sample_07")
      assert(rdd.count() == 823)
    }
  }
}
