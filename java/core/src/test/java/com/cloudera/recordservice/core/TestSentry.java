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

/**
 * This class tests the Sentry service with RecordService, and is
 * used in $RECORD_SERVICE_HOME/tests/run_sentry_tests.py.
 * Notice that this assumes that the test user, role, view, etc.,
 * have been set up by that script, before this test is executed.
 */
public class TestSentry extends TestBase {
  private static final boolean IGNORE_SENTRY_TESTS =
      System.getenv("IGNORE_SENTRY_TESTS") == null ||
      System.getenv("IGNORE_SENTRY_TESTS").equalsIgnoreCase("true");

  @Test
  /**
   * Tests Sentry service with RecordService SQL request.
   */
  public void testSQLRequest() throws IOException, RecordServiceException {
    if (IGNORE_SENTRY_TESTS) return;

    // First, try to access the tpch.nation table, which the test role doesn't
    // have access to. It should fail with exception.
    RecordServicePlannerClient planner = new RecordServicePlannerClient.Builder()
        .connect(PLANNER_HOST, PLANNER_PORT);

    try {
      planner.planRequest(Request.createTableScanRequest("tpch.nation"));
      assertTrue("plan request should have thrown an exception", false);
    } catch (RecordServiceException ex) {
      assertEquals(RecordServiceException.ErrorCode.INVALID_REQUEST, ex.code);
      assertTrue("Actual message is: " + ex.detail,
          ex.detail.contains("does not have privileges to execute"));
    }

    // Should fail when accessing the columns which the test role doesn't have access to.
    try {
      planner.planRequest(
          Request.createSqlRequest("select n_regionkey, n_comment from tpch.nation"));
      assertTrue("plan request should have thrown an exception", false);
    } catch (RecordServiceException ex) {
      assertEquals(RecordServiceException.ErrorCode.INVALID_REQUEST, ex.code);
      assertTrue("Actual message is: " + ex.detail,
          ex.detail.contains("does not have privileges to execute"));
    }

    // Accessing columns which the test role has access to should work.
    planner.planRequest(
        Request.createSqlRequest("select n_name, n_nationkey from tpch.nation"));

    // Accessing tpch.nation_view should work
    planner.planRequest(Request.createTableScanRequest("tpch.nation_view"));
    planner.close();
  }

  @Test
  /**
   * Tests Sentry service with RecordService path request.
   */
  public void testPathRequest() throws IOException, RecordServiceException {
    if (IGNORE_SENTRY_TESTS) return;

    // First, try to access the /test-warehouse/tpch.orders, which the test role doesn't
    // have access to. It should fail with exception.
    RecordServicePlannerClient planner = new RecordServicePlannerClient.Builder()
        .connect(PLANNER_HOST, PLANNER_PORT);

    try {
      planner.planRequest(Request.createPathRequest("/test-warehouse/tpch.orders"));
      assertTrue("plan request should have thrown an exception", false);
    } catch (RecordServiceException ex) {
      assertEquals(RecordServiceException.ErrorCode.AUTHENTICATION_ERROR, ex.code);
      assertTrue("Actual message is: " + ex.message,
          ex.message.contains("does not have full access to the path"));
    }

    // Accessing /test-warehouse/tpch.nation should work
    planner.planRequest(Request.createPathRequest("/test-warehouse/tpch.nation"));
    planner.close();
  }
}
