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

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;

import com.cloudera.recordservice.thrift.TErrorCode;
import com.cloudera.recordservice.thrift.TRecordServiceException;

public class TestDelegationToken {
  static final int PLANNER_PORT = 40000;
  static final String HOST = "localhost";

  public TestDelegationToken() {
    // Setup log4j for testing.
    org.apache.log4j.BasicConfigurator.configure();
  }


  // Tests that the APIs fail gracefully if called to a non-secure server.
  @Test
  public void testUnsecureConnection() throws RuntimeException, IOException,
        TRecordServiceException, InterruptedException {
    RecordServicePlannerClient planner = new RecordServicePlannerClient.Builder()
        .connect(HOST, PLANNER_PORT);
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
  }
}
