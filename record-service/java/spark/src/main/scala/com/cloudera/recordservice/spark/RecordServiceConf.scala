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

import org.apache.spark.{SparkException, SparkContext}
import org.apache.spark.sql.SQLContext

object RecordServiceConf {
  val RECORD_SERVICE_PLANNER_HOST_KEY: String = "record.service.planner.host"
  val RECORD_SERVICE_PLANNER_PORT_KEY: String = "record.service.planner.port"
  val DEFAULT_PLANNER_PORT: Int = 40000
  val WORKER_PORT: Int = 40100

  /**
   * Returns the record service planner host/port.
   */
  def getPlannerHostPort(sc:SparkContext) : (String, Int) = {
    val host =
        sc.getConf.getOption(RECORD_SERVICE_PLANNER_HOST_KEY).getOrElse("localhost")
    val port =
        sc.getConf.getInt(RECORD_SERVICE_PLANNER_PORT_KEY, DEFAULT_PLANNER_PORT)
    (host, port)
  }

  /**
   * Returns the record service planner host/port, first looking in the SQLContext.
   */
  def getPlannerHostPort(ctx:SQLContext) : (String, Int) = {
    var (host, port) = getPlannerHostPort(ctx.sparkContext)
    host = ctx.getConf(RECORD_SERVICE_PLANNER_HOST_KEY, host)
    try {
      port = ctx.getConf(RECORD_SERVICE_PLANNER_PORT_KEY, port.toString()).toInt
    } catch {
      case e: Exception =>
        throw new SparkException("Invalid port config", e)
    }
    (host, port)
  }
}
