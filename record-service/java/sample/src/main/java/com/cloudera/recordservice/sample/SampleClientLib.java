package com.cloudera.recordservice.sample;

import org.apache.thrift.TException;

import com.cloudera.recordservice.client.RecordServicePlannerClient;
import com.cloudera.recordservice.client.RecordServiceWorkerClient;
import com.cloudera.recordservice.client.Rows;
import com.cloudera.recordservice.client.Rows.Row;
import com.cloudera.recordservice.thrift.TPlanRequestResult;
import com.cloudera.recordservice.thrift.TTask;

/**
 * This is similar to SampleClient except built using the client APIs.
 */
public class SampleClientLib {
  static final int PLANNER_PORT = 40000;
  static final int WORKER_PORT = 40100;

  static final String DEFAULT_QUERY = "select n_nationkey from tpch.nation";

  private static void runQuery(String query) throws TException {
    /**
     * First talk to the plan service to get the list of tasks.
     */
    System.out.println("Running request: " + query);

    RecordServicePlannerClient planner = new RecordServicePlannerClient();
    planner.connect("localhost", PLANNER_PORT);
    RecordServiceWorkerClient worker = new RecordServiceWorkerClient();
    worker.connect("localhost", WORKER_PORT);

    TPlanRequestResult planResult = planner.planRequest(query);
    planner.close();

    long totalTimeMs = 0;
    /**
     * Run each task on one of the workers.
     */
    int totalRows = 0;
    long sum = 0;
    for (TTask task: planResult.tasks) {
      Rows rows = null;
      long start = System.currentTimeMillis();
      /* Fetch results until we're done */
      try {
        rows = worker.execAndFetch(task.task);
        while (rows.hasNext()) {
          Row row = rows.next();
          sum += row.getLong(0);
          ++totalRows;
        }
      } finally {
        if (rows != null) rows.close();
      }
      totalTimeMs += System.currentTimeMillis() - start;
    }

    System.out.println("Task complete. Returned: " + totalRows + " rows.");
    System.out.println("Sum: " + sum);
    System.out.println("Took " + totalTimeMs + "ms");
  }

  public static void main(String[] args) throws TException {
    String query = DEFAULT_QUERY;
    if (args.length > 0) query = args[0];
    runQuery(query);
  }

}
