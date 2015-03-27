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

package com.cloudera.recordservice.client;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;

import com.cloudera.recordservice.thrift.TRecordServiceException;

/**
 * Tests that the RecordService does not support these kind of queries.
 */
public class TestUnsupportedFunctionality {
  static final String PLANNER_HOST = "localhost";
  static final int PLANNER_PORT = 40000;

  private void testUnsupported(String sql) {
    boolean exceptionThrown = false;
    try {
      RecordServicePlannerClient.planRequest(
          PLANNER_HOST, PLANNER_PORT, Request.createSqlRequest(sql));
    } catch (IOException e) {
      assertFalse(e.getMessage(), true);
    } catch (TRecordServiceException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage(), e.getMessage().contains("Could not plan request."));
      System.out.println(e);
      assertTrue(e.detail,
          e.detail.contains("RecordService only supports scan queries."));
    }
    assertTrue("Request should not be supported: " + sql, exceptionThrown);
  }

  @Test
  public void testUnsupported() {
    testUnsupported("SELECT 1");
    testUnsupported("SELECT count(*) FROM tpch.nation");
    testUnsupported("SELECT t1.* FROM tpch.nation as t1 JOIN tpch.nation as t2 "+
        "ON t1.n_nationkey = t2.n_nationkey");
    testUnsupported("use default");
    testUnsupported("set num_nodes=1");
    testUnsupported("insert into tpch.nation select * from tpch.nation");
    testUnsupported("explain select * from tpch.nation");
    testUnsupported("create table rs.not_exists(i int)");
  }
}
