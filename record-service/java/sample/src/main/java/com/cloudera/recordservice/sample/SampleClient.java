package com.cloudera.recordservice.sample;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.cloudera.recordservice.thrift.RecordServicePlanner;
import com.cloudera.recordservice.thrift.RecordServiceWorker;
import com.cloudera.recordservice.thrift.TExecTaskParams;
import com.cloudera.recordservice.thrift.TExecTaskResult;
import com.cloudera.recordservice.thrift.TFetchParams;
import com.cloudera.recordservice.thrift.TFetchResult;
import com.cloudera.recordservice.thrift.TPlanRequestParams;
import com.cloudera.recordservice.thrift.TPlanRequestResult;
import com.cloudera.recordservice.thrift.TProtocolVersion;
import com.cloudera.recordservice.thrift.TRecordServiceException;
import com.cloudera.recordservice.thrift.TTask;

/**
 * This is a simple example RecordService client that demonstrates how
 * to communicate with the two services to read records.
 */
public class SampleClient {
  static final String DEFAULT_QUERY = "select n_nationkey from tpch.nation";
  static final int PLANNER_PORT = 40000;
  static final int WORKER_PORT = 40100;

  static TProtocol createConnection(int port, String serviceName)
    throws TTransportException {
      TTransport transport = new TSocket("localhost", port);
      try {
        transport.open();
      } catch (TTransportException e) {
        System.err.println("Could not connect to service: " + serviceName);
        throw e;
      }
      return new TBinaryProtocol(transport);
    }

  private static void runQuery(String query) throws TException {
    /**
     * First talk to the plan service to get the list of tasks.
     */
    System.out.println("Running request: " + query);

    RecordServicePlanner.Client planner = new RecordServicePlanner.Client(
        createConnection(PLANNER_PORT, "Planner"));
    TPlanRequestResult planResult;
    try {
      TPlanRequestParams planParams = new TPlanRequestParams(TProtocolVersion.V1, query);
      planResult = planner.PlanRequest(planParams);
    } catch (TRecordServiceException e) {
      System.err.println("Could not plan request: " + e.message);
      throw e;
    } catch (TException e) {
      System.err.println("Could not plan request: " + e.getMessage());
      throw e;
    }
    System.out.println("Generated " + planResult.tasks.size() + " tasks.");

    long totalTimeMs = 0;

    /**
     * Run each task on one of the workers.
     */
    int totalRows = 0;
    long sum = 0;
    for (TTask task: planResult.tasks) {
      /* Start executing the task */
      RecordServiceWorker.Client worker = new RecordServiceWorker.Client(
          createConnection(WORKER_PORT, "Worker"));
      TExecTaskResult taskResult = null;
      try {
        TExecTaskParams taskParams = new TExecTaskParams(task.task);
        taskResult = worker.ExecTask(taskParams);
      } catch (TRecordServiceException e) {
        System.err.println("Could not exec task: " + e.message);
        throw e;
      } catch (TException e) {
        System.err.println("Could not exec task: " + e.getMessage());
        throw e;
      }

      long start = System.currentTimeMillis();
      /* Fetch results until we're done */
      try {
        TFetchResult fetchResult = null;
        do {
          TFetchParams fetchParams = new TFetchParams(taskResult.handle);
          fetchResult = worker.Fetch(fetchParams);
          totalRows += fetchResult.num_rows;
          ByteBuffer data = fetchResult.columnar_row_batch.cols.get(0).
              data.order(ByteOrder.LITTLE_ENDIAN);
          for (int i = 0; i < fetchResult.num_rows; ++i) {
            sum += data.getLong(i * 8);
          }
        } while (!fetchResult.done);
      } catch (TRecordServiceException e) {
        System.err.println("Could not fetch from task: " + e.message);
        throw e;
      } catch (TException e) {
        System.err.println("Could not fetch from task: " + e.getMessage());
        throw e;
      } finally {
        worker.CloseTask(taskResult.handle);
      }
      totalTimeMs += System.currentTimeMillis() - start;
    }

    System.out.println("Task complete. Returned: " + totalRows + " rows.");
    System.out.println("Sum: " + sum);
    System.out.println("Took " + totalTimeMs + "ms");
  }

  public static void main(String[] args) throws TException {
    String query = DEFAULT_QUERY;
    if (args.length > 0) query = args[1];
    for (int i = 0; i < 10; ++i)
    runQuery(query);
  }
}
