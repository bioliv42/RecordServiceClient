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

package com.cloudera.recordservice.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;

import org.junit.Test;

import com.cloudera.recordservice.thrift.TPlanRequestResult;
import com.cloudera.recordservice.thrift.TRecordServiceException;

// Tests fault tolerance and retry logic in the client library.
public class TestFaultTolerance {
  static final int PLANNER_PORT = 40000;
  static final int WORKER_PORT = 40100;

  public TestFaultTolerance() {
    // Setup log4j for testing.
    org.apache.log4j.BasicConfigurator.configure();
  }

  @Test
  public void testWorkerRetry() throws RuntimeException, IOException,
        TRecordServiceException, InterruptedException {
    TPlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .planRequest("localhost", PLANNER_PORT,
            Request.createTableScanRequest("tpch.nation"));
    RecordServiceWorkerClient worker = new RecordServiceWorkerClient.Builder()
        .setMaxAttempts(3).setSleepDurationMs(10).setFetchSize(1)
        .connect("localhost", WORKER_PORT);
    Records records = worker.execAndFetch(plan.tasks.get(0));
    int numRecords = 0;
    while (records.hasNext()) {
      if (numRecords % 2 == 0) {
        // Close the underlying connection. This simulates a failure where the
        // worker does not die but the connection is dropped.
        worker.closeConnectionForTesting();
      }
      Records.Record record = records.next();
      assertEquals(record.getShort(0), numRecords);
      ++numRecords;
    }

    assertEquals(numRecords, 25);

    worker.close();
  }

  @Test
  public void testPlannerRetry() throws RuntimeException, IOException,
          TRecordServiceException, InterruptedException {
    RecordServicePlannerClient planner = new RecordServicePlannerClient.Builder()
        .setMaxAttempts(3).setSleepDurationMs(10)
        .connect("localhost", PLANNER_PORT);

    planner.closeConnectionForTesting();
    boolean exceptionThrown = false;
    try {
      planner.planRequest(Request.createTableScanRequest("tpch.nation"));
    } catch (Exception e) {
      exceptionThrown = true;
    }
    assertFalse(exceptionThrown);

    planner.closeConnectionForTesting();
    exceptionThrown = false;
    try {
      planner.getSchema(Request.createTableScanRequest("tpch.nation"));
    } catch (Exception e) {
      exceptionThrown = true;
    }
    assertFalse(exceptionThrown);

    planner.close();
  }
}
