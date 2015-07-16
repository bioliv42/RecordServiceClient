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

import org.junit.BeforeClass;
import org.junit.Test;

import com.cloudera.recordservice.thrift.TDelegationToken;
import com.cloudera.recordservice.thrift.TPlanRequestResult;
import com.cloudera.recordservice.thrift.TRecordServiceException;

// Tests connect to kerberized cluster. This is not normally run as it involves
// a non-trivial local set up to get tickets and what not.
public class TestKerberosConnection extends TestBase {
  static final int PLANNER_PORT = 40000;
  static final int WORKER_PORT = 40100;
  // Kerberized cluster.
  static final String HOST = "vd0224.halxg.cloudera.com";
  static final String PRINCIPAL = "impala/vd0224.halxg.cloudera.com@HALXG.CLOUDERA.COM";

  static final boolean RUN_KERBEROS_TESTS =
      System.getenv("RECORD_SERVICE_RUN_KERBEROS_TESTS") != null &&
      System.getenv("RECORD_SERVICE_RUN_KERBEROS_TESTS").equalsIgnoreCase("true");

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TestBase.setUpBeforeClass();
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

    // TODO: use the principal when the worker connection is kerberized.
    RecordServiceWorkerClient worker = new RecordServiceWorkerClient.Builder().
        setKerberosPrincipal(null).connect(HOST, WORKER_PORT);
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
    // TODO: enable no principal once worker service is kerberized.
    /*
    exceptionThrown = false;
    try {
      new RecordServiceWorkerClient.Builder().connect(HOST, WORKER_PORT);
    } catch (TRecordServiceException e) {
      exceptionThrown = true;
    }
    assertTrue("Should not be able to connect without kerberos principal",
        exceptionThrown);
    */

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


  @Test
  // Test authentication with delegation token.
  public void testDelegationToken() throws RuntimeException, IOException,
        TRecordServiceException, InterruptedException {
    if (!RUN_KERBEROS_TESTS) return;
    boolean exceptionThrown = false;

    // Connect to the planner via kerberos.
    RecordServicePlannerClient kerberizedPlanner = new RecordServicePlannerClient.Builder()
        .setKerberosPrincipal(PRINCIPAL)
        .connect(HOST, PLANNER_PORT);

    // Get a token from planner.
    TDelegationToken token = kerberizedPlanner.getDelegationToken("impala");
    assertTrue(token.identifier.length() > 0);
    assertTrue(token.password.length() > 0);
    assertTrue(token.token.remaining() > token.identifier.length());
    assertTrue(token.token.remaining() > token.password.length());

    // Renew the token.
    kerberizedPlanner.renewDelegationToken(token);
    kerberizedPlanner.close();

    // Connect to the planner using the token.
    RecordServicePlannerClient tokenPlanner = new RecordServicePlannerClient.Builder().
        setDelegationToken(token).
        connect(HOST, PLANNER_PORT);

    // Should only be able to get tokens if the connection is kerberized.
    exceptionThrown = false;
    try {
      tokenPlanner.getDelegationToken(null);
    } catch (TRecordServiceException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage(), e.getMessage().contains(
          "can only be called with a Kerberos connection."));
    }
    assertTrue(exceptionThrown);

    exceptionThrown = false;
    try {
      tokenPlanner.renewDelegationToken(token);
    } catch (TRecordServiceException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage(), e.getMessage().contains(
          "can only be called with a Kerberos connection."));
    }
    assertTrue(exceptionThrown);

    // But other APIs should work.
    TPlanRequestResult plan = tokenPlanner.planRequest(
        Request.createTableScanRequest("sample_07"));
    assertTrue(plan.tasks.size() == 1);

    /* TODO: update the worker connection to be secure as well.
    // Create a worker connection with the token.
    RecordServiceWorkerClient worker = new RecordServiceWorkerClient.Builder().
        setDelegationToken(token).connect(HOST, WORKER_PORT);

    // Fetch the results.
    Records records = worker.execAndFetch(plan.tasks.get(0));
    int numRecords = 0;
    while (records.hasNext()) {
      records.next();
      ++numRecords;
    }
    assertEquals(numRecords, 823);
    worker.close();

    // Cancel the token. Note that this can be done without a kerberized connection.
    tokenPlanner.cancelDelegationToken(token);

    // Shouldn't be able to connect with it anymore.
    try {
      new RecordServiceWorkerClient.Builder().setDelegationToken(token)
          .connect(HOST, WORKER_PORT);
    } catch (IOException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("Invalid token."));
    }
    assertTrue(exceptionThrown);
    */
    // Cancel the token. Note that this can be done without a kerberized connection.
    tokenPlanner.cancelDelegationToken(token);
    tokenPlanner.close();

    // Try to connect with the canceled token. Should fail.
    exceptionThrown = false;
    try {
      new RecordServicePlannerClient.Builder().setDelegationToken(token)
          .connect(HOST, PLANNER_PORT);
    } catch (IOException e) {
      exceptionThrown = true;
      // TODO: the error is generated deep in the sasl negotiation and we
      // don't get a generic error. Fix this.
    }
    assertTrue(exceptionThrown);
  }
}
