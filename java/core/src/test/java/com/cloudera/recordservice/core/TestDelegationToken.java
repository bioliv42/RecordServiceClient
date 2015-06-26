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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Test;

import com.cloudera.recordservice.thrift.TRecordServiceException;


public class TestDelegationToken {
  static final int PLANNER_PORT = 40000;
  static final String HOST = "localhost";


  public TestDelegationToken() {
    // Setup log4j for testing.
    org.apache.log4j.BasicConfigurator.configure();
  }

  // Just some dummy tests to verify plumbing is working.
  @Test
  public void testAPI() throws RuntimeException, IOException,
        TRecordServiceException, InterruptedException {
    RecordServicePlannerClient planner =
      new RecordServicePlannerClient(HOST, PLANNER_PORT);

    boolean exceptionThrown = false;
    try {
      planner.getDelegationToken(null);
    } catch (TRecordServiceException e) {
      exceptionThrown = true;
      assertTrue(e.getDetail().contains("Not yet implemented"));
    }
    assertTrue(exceptionThrown);

    exceptionThrown = false;
    try {
      planner.cancelDelegationToken(ByteBuffer.allocate(10));
    } catch (TRecordServiceException e) {
      exceptionThrown = true;
      assertTrue(e.getDetail().contains("Not yet implemented"));
    }
    assertTrue(exceptionThrown);

    exceptionThrown = false;
    try {
      planner.renewDelegationToken(ByteBuffer.allocate(10));
    } catch (TRecordServiceException e) {
      exceptionThrown = true;
      assertTrue(e.getDetail().contains("Not yet implemented"));
    }
    assertTrue(exceptionThrown);

    planner.close();
  }
}
