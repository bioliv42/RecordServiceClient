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
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;

import com.cloudera.recordservice.thrift.TPlanRequestResult;
import com.cloudera.recordservice.thrift.TRecordServiceException;

// Tests connect to kerberized cluster. This is not normally run as it involves
// a non-trivial local set up to get tickets and what not.
public class TestKerberosConnection {
  static final int PLANNER_PORT = 40000;
  static final int WORKER_PORT = 40100;
  // Kerberized cluster.
  static final String HOST = "vd0224.halxg.cloudera.com";
  static final String PRINCIPAL = "impala/vd0224.halxg.cloudera.com@HALXG.CLOUDERA.COM";

  static final boolean RUN_KERBEROS_TESTS =
      System.getenv("RECORD_SERVICE_RUN_KERBEROS_TESTS") == "true";

  public TestKerberosConnection() {
    // Setup log4j for testing.
    org.apache.log4j.BasicConfigurator.configure();
    if (RUN_KERBEROS_TESTS) {
      System.out.println("Running kerberos tests.");
    } else {
      System.out.println("Skipping kerberos tests.");
    }
  }

  @Test
  public void testConnection() throws RuntimeException, IOException,
        TRecordServiceException, InterruptedException {
    if (!RUN_KERBEROS_TESTS) return;
    TPlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .setKerberosPrincipal(PRINCIPAL)
        .planRequest(HOST, PLANNER_PORT, Request.createTableScanRequest("sample_07"));
    assertEquals(plan.schema.cols.size(), 4);

    RecordServiceWorkerClient worker = new RecordServiceWorkerClient.Builder().
        setKerberosPrincipal(PRINCIPAL).connect(HOST, WORKER_PORT);
    Records records = worker.execAndFetch(plan.tasks.get(0));
    int numRecords = 0;
    while (records.hasNext()) {
      records.next();
      ++numRecords;
    }
    assertEquals(numRecords, 823);
  }

  @Test
  // Test without providing a principal or a bad principal.
  public void testBadConnection() throws RuntimeException, IOException,
        TRecordServiceException, InterruptedException {
    if (!RUN_KERBEROS_TESTS) return;

    // Try planner connection with no principal and bad principal
    boolean exceptionThrown = false;
    try {
      new RecordServicePlannerClient.Builder()
          .planRequest(HOST, PLANNER_PORT, Request.createTableScanRequest("sample_07"));
    } catch (TRecordServiceException e) {
      exceptionThrown = true;
    }
    assertTrue("Should not be able to connect without kerberos principal",
        exceptionThrown);

    exceptionThrown = false;
    try {
      new RecordServicePlannerClient.Builder()
          .setKerberosPrincipal("BAD/bad.com@bad.com")
          .planRequest(HOST, PLANNER_PORT, Request.createTableScanRequest("sample_07"));
    } catch (IOException e) {
      exceptionThrown = true;
    }
    assertTrue("Should not be able to connect with bad kerberos principal",
        exceptionThrown);

    // Try worker connection with no principal and bad principal
    exceptionThrown = false;
    try {
      new RecordServiceWorkerClient.Builder().connect(HOST, WORKER_PORT);
    } catch (TRecordServiceException e) {
      exceptionThrown = true;
    }
    assertTrue("Should not be able to connect without kerberos principal",
        exceptionThrown);

    exceptionThrown = false;
    try {
      new RecordServiceWorkerClient.Builder().setKerberosPrincipal("BAD/bad.com@bad.com")
          .connect(HOST, WORKER_PORT);
    } catch (IOException e) {
      exceptionThrown = true;
    }
    assertTrue("Should not be able to connect with bad kerberos principal",
        exceptionThrown);
  }
}
