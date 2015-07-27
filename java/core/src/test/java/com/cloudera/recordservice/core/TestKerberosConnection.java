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

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import com.cloudera.recordservice.thrift.TDelegationToken;
import com.cloudera.recordservice.thrift.TErrorCode;
import com.cloudera.recordservice.thrift.TPlanRequestResult;
import com.cloudera.recordservice.thrift.TRecordServiceException;

// Tests connect to kerberized cluster. This is not normally run as it involves
// a non-trivial local set up to get tickets and what not.
// TODO: add renew/expire tests.
public class TestKerberosConnection extends TestBase {
  static final int PLANNER_PORT = 40000;
  static final int WORKER_PORT = 40100;
  // Kerberized cluster.
  static final String HOST = "vd0224.halxg.cloudera.com";
  static final String[] SECURE_CLUSTER =
      { HOST, "vd0226.halxg.cloudera.com", "vd0228.halxg.cloudera.com" };

  static final String PRINCIPAL = "impala/vd0224.halxg.cloudera.com@HALXG.CLOUDERA.COM";

  // Number of rows in the sample_07 table.
  static final int SAMPLE_07_ROW_COUNT = 823;

  static final boolean HAS_KERBEROS_CREDENTIALS =
      System.getenv("HAS_KERBEROS_CREDENTIALS") != null &&
      System.getenv("HAS_KERBEROS_CREDENTIALS").equalsIgnoreCase("true");

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TestBase.setUpBeforeClass();
    // Setup log4j for testing.
    org.apache.log4j.BasicConfigurator.configure();
    if (HAS_KERBEROS_CREDENTIALS) {
      System.out.println("Running tests with kerberos credentials.");
    } else {
      System.out.println("Skipping tests which require kerberos credentials.");
    }
  }

  @Test
  public void testConnection() throws IOException,
        TRecordServiceException, InterruptedException {
    Assume.assumeTrue(HAS_KERBEROS_CREDENTIALS);

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
    assertEquals(numRecords, SAMPLE_07_ROW_COUNT);
    worker.close();
  }

  @Test
  // Test without providing a principal or a bad principal.
  public void testBadConnection() throws IOException,
        TRecordServiceException, InterruptedException {
    Assume.assumeTrue(HAS_KERBEROS_CREDENTIALS);

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

  @Test
  // Test authentication with delegation token.
  public void testDelegationToken() throws IOException,
        TRecordServiceException, InterruptedException {
    Assume.assumeTrue(HAS_KERBEROS_CREDENTIALS);
    boolean exceptionThrown = false;

    // Connect to the planner via kerberos.
    RecordServicePlannerClient kerberizedPlanner = new RecordServicePlannerClient.Builder()
        .setKerberosPrincipal(PRINCIPAL)
        .connect(HOST, PLANNER_PORT);

    // Get a token from planner.
    TDelegationToken token1 = kerberizedPlanner.getDelegationToken("impala");
    assertTrue(token1.identifier.length() > 0);
    assertTrue(token1.password.length() > 0);
    assertTrue(token1.token.remaining() > token1.identifier.length());
    assertTrue(token1.token.remaining() > token1.password.length());

    // Get a second token
    TDelegationToken token2 = kerberizedPlanner.getDelegationToken("impala");

    // Renew the token.
    kerberizedPlanner.renewDelegationToken(token1);
    kerberizedPlanner.close();

    // Connect to the planner using the token.
    RecordServicePlannerClient tokenPlanner = new RecordServicePlannerClient.Builder()
        .setDelegationToken(token1).connect(HOST, PLANNER_PORT);

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
      tokenPlanner.renewDelegationToken(token1);
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

    // Try a new request (this creates a new connection).
    new RecordServicePlannerClient.Builder().setDelegationToken(token1)
        .getSchema(HOST, PLANNER_PORT, Request.createTableScanRequest("sample_07"));
    // Try with other token.
    new RecordServicePlannerClient.Builder().setDelegationToken(token2)
        .getSchema(HOST, PLANNER_PORT, Request.createTableScanRequest("sample_07"));

    // Create a worker connection with the token.
    RecordServiceWorkerClient worker = new RecordServiceWorkerClient.Builder().
        setDelegationToken(token1).connect(HOST, WORKER_PORT);

    // Fetch the results.
    Records records = worker.execAndFetch(plan.tasks.get(0));
    int numRecords = 0;
    while (records.hasNext()) {
      records.next();
      ++numRecords;
    }
    assertEquals(numRecords, SAMPLE_07_ROW_COUNT);
    worker.close();

    // Cancel the token. Note that this can be done without a kerberized connection.
    tokenPlanner.cancelDelegationToken(token1);
    tokenPlanner.close();

    // Shouldn't be able to connect with it anymore.
    try {
      new RecordServiceWorkerClient.Builder().setDelegationToken(token1)
          .connect(HOST, WORKER_PORT);
    } catch (IOException e) {
      exceptionThrown = true;
      // TODO: the error is generated deep in the sasl negotiation and we
      // don't get a generic error. Fix this.
    }
    assertTrue(exceptionThrown);

    // Try to connect with the canceled token. Should fail.
    exceptionThrown = false;
    try {
      new RecordServicePlannerClient.Builder().setDelegationToken(token1)
          .connect(HOST, PLANNER_PORT).close();
    } catch (IOException e) {
      exceptionThrown = true;
      // TODO: the error is generated deep in the sasl negotiation and we
      // don't get a generic error. Fix this.
    }
    assertTrue(exceptionThrown);

    // Token2 should still work (not cancelled).
    new RecordServicePlannerClient.Builder().setDelegationToken(token2)
        .getSchema(HOST, PLANNER_PORT, Request.createTableScanRequest("sample_07"));
    new RecordServiceWorkerClient.Builder().setDelegationToken(token2)
        .connect(HOST, WORKER_PORT).close();
  }

  @Test
  public void testInvalidToken() throws IOException,
      TRecordServiceException, InterruptedException {
    Assume.assumeTrue(HAS_KERBEROS_CREDENTIALS);
    boolean exceptionThrown = false;

    // Connect to the planner via kerberos.
    RecordServicePlannerClient kerberizedPlanner = new RecordServicePlannerClient.Builder()
        .setKerberosPrincipal(PRINCIPAL)
        .connect(HOST, PLANNER_PORT);

    // Get two tokens from planner.
    TDelegationToken token1 = kerberizedPlanner.getDelegationToken("impala");
    TDelegationToken token2 = kerberizedPlanner.getDelegationToken("impala");
    kerberizedPlanner.close();

    // Verify they work.
    new RecordServicePlannerClient.Builder()
        .setDelegationToken(token1).connect(HOST, PLANNER_PORT).close();
    new RecordServicePlannerClient.Builder()
        .setDelegationToken(token2).connect(HOST, PLANNER_PORT).close();

    TDelegationToken testToken = new TDelegationToken();
    try {
      new RecordServicePlannerClient.Builder().setDelegationToken(testToken)
          .connect(HOST, PLANNER_PORT);
    } catch (IOException e) {
      exceptionThrown = true;
    }
    assert(exceptionThrown);

    // Set the identifier but no password
    testToken.identifier = token1.identifier;
    try {
      new RecordServicePlannerClient.Builder().setDelegationToken(testToken)
          .connect(HOST, PLANNER_PORT).close();
    } catch (IOException e) {
      exceptionThrown = true;
    }
    assert(exceptionThrown);

    // Set it to the other (wrong password);
    testToken.password = token2.password;
    try {
      new RecordServicePlannerClient.Builder().setDelegationToken(testToken)
          .connect(HOST, PLANNER_PORT).close();
    } catch (IOException e) {
      exceptionThrown = true;
    }
    assert(exceptionThrown);

    // Set it to the right password. Everything should still work.
    testToken.password = token1.password;
    new RecordServicePlannerClient.Builder().setDelegationToken(testToken)
        .connect(HOST, PLANNER_PORT).close();
    new RecordServicePlannerClient.Builder().setDelegationToken(token1)
        .connect(HOST, PLANNER_PORT).close();
    new RecordServicePlannerClient.Builder().setDelegationToken(token2)
        .connect(HOST, PLANNER_PORT).close();
  }

  // Tests that the delegation token APIs fail gracefully if called to a
  // non-secure server.
  @Test
  public void testUnsecureConnectionTokens() throws IOException,
        TRecordServiceException, InterruptedException {
    RecordServicePlannerClient planner = new RecordServicePlannerClient.Builder()
        .connect("localhost", PLANNER_PORT);
    boolean exceptionThrown = false;
    try {
      planner.getDelegationToken(null);
    } catch (TRecordServiceException e) {
      assertTrue(e.getCode() == TErrorCode.AUTHENTICATION_ERROR);
      assertTrue(e.getMessage().contains(
          "can only be called with a Kerberos connection."));
      exceptionThrown = true;
    }
    assertTrue(exceptionThrown);

    exceptionThrown = false;
    try {
      planner.cancelDelegationToken(null);
    } catch (TRecordServiceException e) {
      assertTrue(e.getCode() == TErrorCode.AUTHENTICATION_ERROR);
      assertTrue(e.getMessage().contains(
          "can only be called from a secure connection."));
      exceptionThrown = true;
    }
    assertTrue(exceptionThrown);

    exceptionThrown = false;
    try {
      planner.renewDelegationToken(null);
    } catch (TRecordServiceException e) {
      assertTrue(e.getCode() == TErrorCode.AUTHENTICATION_ERROR);
      assertTrue(e.getMessage().contains(
          "can only be called with a Kerberos connection."));
      exceptionThrown = true;
    }
    assertTrue(exceptionThrown);
    planner.close();
  }

  // Tests that a secure client connecting to an unsecure server behaves
  // reasonably.
  @Test
  public void testUnsecureConnection() throws IOException,
      TRecordServiceException, InterruptedException {
    Assume.assumeTrue(HAS_KERBEROS_CREDENTIALS);
    boolean exceptionThrown = false;

    // Try to connect to a unsecure server with a principal. This should fail.
    exceptionThrown = false;
    try {
      new RecordServicePlannerClient.Builder()
          .setKerberosPrincipal(PRINCIPAL)
          .setTimeoutMs(1000)
          .connect("localhost", PLANNER_PORT);
    } catch (IOException e) {
      assertTrue(e.getMessage(), e.getMessage().contains(
          "Ensure the server has security enabled."));
      exceptionThrown = true;
    }
    assertTrue(exceptionThrown);

    // Try to connect to a unsecure server with a principal. This should fail.
    exceptionThrown = false;
    try {
      new RecordServiceWorkerClient.Builder()
          .setKerberosPrincipal(PRINCIPAL)
          .setTimeoutMs(1000)
          .connect("localhost", WORKER_PORT);
    } catch (IOException e) {
      assertTrue(e.getMessage().contains(
          "Ensure the server has security enabled."));
      exceptionThrown = true;
    }
    assertTrue(exceptionThrown);
  }

  // Tests that tokens are distributed across the cluster.
  @Test
  public void testPersistedTokens() throws IOException,
      TRecordServiceException, InterruptedException {
    Assume.assumeTrue(HAS_KERBEROS_CREDENTIALS);

    RecordServicePlannerClient planner = new RecordServicePlannerClient.Builder()
        .setKerberosPrincipal(PRINCIPAL)
        .connect(HOST, PLANNER_PORT);
    for (String hostToCancel: SECURE_CLUSTER) {
      TDelegationToken token = planner.getDelegationToken("impala");

      // Try all the connections, they should all work.
      for (String host: SECURE_CLUSTER) {
        new RecordServicePlannerClient.Builder().setDelegationToken(token)
            .connect(host, PLANNER_PORT).close();
        new RecordServiceWorkerClient.Builder().setDelegationToken(token)
            .connect(host, WORKER_PORT).close();
      }

      // Cancel the token.
      RecordServicePlannerClient client = new RecordServicePlannerClient.Builder()
          .setDelegationToken(token)
          .connect(hostToCancel, PLANNER_PORT);
      client.cancelDelegationToken(token);
      client.close();

      // Try all the connections, they should all fail now.
      for (String host: SECURE_CLUSTER) {
        boolean exceptionThrown = false;
        try {
          new RecordServicePlannerClient.Builder().setDelegationToken(token)
              .connect(host, PLANNER_PORT).close();
        } catch (IOException e) {
          exceptionThrown = true;
          assertTrue(e.getMessage().contains(
              "Could not connect to RecordServicePlanner"));
        }
        assertTrue(exceptionThrown);

        exceptionThrown = false;
        try {
          new RecordServiceWorkerClient.Builder().setDelegationToken(token)
              .connect(host, WORKER_PORT).close();
        } catch (IOException e) {
          exceptionThrown = true;
          assertTrue(e.getMessage().contains(
              "Could not connect to RecordServiceWorker"));
        }
        assertTrue(exceptionThrown);
      }
    }
    planner.close();
  }
}
