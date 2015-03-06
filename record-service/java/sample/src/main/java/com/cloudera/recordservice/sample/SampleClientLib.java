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

import org.apache.thrift.TException;

import com.cloudera.recordservice.client.RecordServicePlannerClient;
import com.cloudera.recordservice.client.RecordServiceWorkerClient;
import com.cloudera.recordservice.client.Records;
import com.cloudera.recordservice.client.Records.Record;
import com.cloudera.recordservice.thrift.TPlanRequestResult;
import com.cloudera.recordservice.thrift.TTask;

/**
 * This is similar to SampleClient except built using the client APIs.
 */
public class SampleClientLib {
  static final int PLANNER_PORT = 40000;
  static final int WORKER_PORT = 40100;

  static final String DEFAULT_QUERY = "select n_nationkey from tpch.nation";

  private static void runQuery(String query) throws TException, IOException {
    /**
     * First talk to the plan service to get the list of tasks.
     */
    System.out.println("Running request: " + query);

    RecordServiceWorkerClient worker = new RecordServiceWorkerClient();
    worker.connect("localhost", WORKER_PORT);

    TPlanRequestResult planResult = RecordServicePlannerClient.planRequest(
        "localhost", PLANNER_PORT, query);
    long totalTimeMs = 0;
    /**
     * Run each task on one of the workers.
     */
    int totalRows = 0;
    long sum = 0;
    for (TTask task: planResult.tasks) {
      Records records = null;
      long start = System.currentTimeMillis();
      /* Fetch results until we're done */
      try {
        records = worker.execAndFetch(task.task);
        while (records.hasNext()) {
          Record record = records.next();
          sum += record.getLong(0);
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

  public static void main(String[] args) throws TException, IOException {
    String query = DEFAULT_QUERY;
    if (args.length > 0) query = args[0];
    runQuery(query);
  }

}
