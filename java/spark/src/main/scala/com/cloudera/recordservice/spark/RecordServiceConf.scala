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

import com.cloudera.recordservice.core.NetworkAddress
import com.cloudera.recordservice.mr.RecordServiceConfig

object RecordServiceConf {
  // Spark requires that configs start with "spark." to be read.
  val PLANNER_HOSTPORTS_CONF:String =
      "spark." + RecordServiceConfig.PLANNER_HOSTPORTS_CONF

  val KERBEROS_PRINCIPAL_CONF:String =
      "spark." + RecordServiceConfig.KERBEROS_PRINCIPAL_CONF

  val FETCH_SIZE_CONF:String =
      "spark." + RecordServiceConfig.FETCH_SIZE_CONF

  val MEM_LIMIT_CONF:String =
      "spark." + RecordServiceConfig.MEM_LIMIT_CONF

  val RECORDS_LIMIT_CONF:String =
      "spark." + RecordServiceConfig.RECORDS_LIMIT_CONF

  val PLANNER_REQUEST_MAX_TASKS:String =
      "spark." + RecordServiceConfig.PLANNER_REQUEST_MAX_TASKS

  val PLANNER_RETRY_ATTEMPTS_CONF:String =
      "spark." + RecordServiceConfig.PLANNER_RETRY_ATTEMPTS_CONF

  val PLANNER_RETRY_SLEEP_MS_CONF:String =
      "spark." + RecordServiceConfig.PLANNER_RETRY_SLEEP_MS_CONF

  val PLANNER_CONNECTION_TIMEOUT_MS_CONF:String =
      "spark." + RecordServiceConfig.PLANNER_CONNECTION_TIMEOUT_MS_CONF

  val PLANNER_RPC_TIMEOUT_MS_CONF:String =
      "spark." + RecordServiceConfig.PLANNER_RPC_TIMEOUT_MS_CONF

  val WORKER_RETRY_ATTEMPTS_CONF:String =
      "spark." + RecordServiceConfig.WORKER_RETRY_ATTEMPTS_CONF

  val WORKER_RETRY_SLEEP_MS_CONF:String =
      "spark." + RecordServiceConfig.WORKER_RETRY_SLEEP_MS_CONF

  val WORKER_CONNECTION_TIMEOUT_MS_CONF:String =
      "spark." + RecordServiceConfig.WORKER_CONNECTION_TIMEOUT_MS_CONF

  val WORKER_RPC_TIMEOUT_MS_CONF:String =
      "spark." + RecordServiceConfig.WORKER_RPC_TIMEOUT_MS_CONF

  /**
   * Returns the list of record service planner host/port.
   */
  def getPlannerHostPort(sc:SparkContext) : java.util.List[NetworkAddress] = {
    val hostports = sc.getConf.getOption(PLANNER_HOSTPORTS_CONF)
        .getOrElse(RecordServiceConfig.DEFAULT_PLANNER_HOSTPORTS)
    RecordServiceConfig.getPlannerHostPort(hostports)
  }

  /**
   * Returns the kerberos principal to connect with.
   */
  def getKerberosPrincipal(sc:SparkContext) : String = {
    sc.getConf.getOption(KERBEROS_PRINCIPAL_CONF).getOrElse(null)
  }
}
