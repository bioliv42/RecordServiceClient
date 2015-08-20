// Confidential Cloudera Information: Covered by NDA.
// Copyright 2014 Cloudera Inc.
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

package com.cloudera.recordservice.sample;

import java.io.IOException;

import com.cloudera.recordservice.core.PlanRequestResult;
import com.cloudera.recordservice.core.RecordServiceException;
import com.cloudera.recordservice.core.RecordServicePlannerClient;
import com.cloudera.recordservice.core.Records;
import com.cloudera.recordservice.core.Records.Record;
import com.cloudera.recordservice.core.Request;
import com.cloudera.recordservice.core.WorkerClientUtil;

/**
 * This is similar to SampleClient except built using the client APIs.
 */
public class SampleClientLib {
  static final int PLANNER_PORT = 40000;

  static final String DEFAULT_QUERY = "select n_nationkey from tpch.nation";

  private static void runQuery(String query) throws RecordServiceException, IOException {
    /**
     * First talk to the plan service to get the list of tasks.
     */
    System.out.println("Running request: " + query);

    PlanRequestResult planResult = new RecordServicePlannerClient.Builder()
        .planRequest("localhost", PLANNER_PORT, Request.createSqlRequest(query));

    long totalTimeMs = 0;

    int totalRows = 0;
    long sum = 0;

    // Run each task and fetch results until we're done
    for (int i = 0; i < planResult.tasks.size(); ++i) {
      long start = System.currentTimeMillis();
      Records records = null;
      try {
        records = WorkerClientUtil.execTask(planResult, i);
        while (records.hasNext()) {
          Record record = records.next();
          sum += record.nextLong(0);
          ++totalRows;
        }
      } finally {
        if (records != null) records.close();
      }
      totalTimeMs += System.currentTimeMillis() - start;
    }

    System.out.println("Task complete. Returned: " + totalRows + " rows.");
    System.out.println("Sum: " + sum);
    System.out.println("Took " + totalTimeMs + "ms");
  }

  public static void main(String[] args) throws RecordServiceException, IOException {
    String query = DEFAULT_QUERY;
    if (args.length > 0) query = args[0];
    runQuery(query);
  }

}
