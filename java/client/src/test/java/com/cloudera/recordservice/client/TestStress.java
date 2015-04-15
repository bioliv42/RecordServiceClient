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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.junit.Test;

import com.cloudera.recordservice.thrift.TErrorCode;
import com.cloudera.recordservice.thrift.TRecordServiceException;
import com.google.common.collect.Lists;

// Tests the clients in stressful environments.
public class TestStress {
  static final int PLANNER_PORT = 40000;
  static final int WORKER_PORT = 40100;

  public TestStress() {
    // Setup log4j for testing.
    org.apache.log4j.BasicConfigurator.configure();
  }

  @Test
  public void testPlannerConnections() throws RuntimeException, IOException,
        TRecordServiceException, InterruptedException {
    // This is more than the maximum number of client threads.
    // TODO: fix the thrift connections to not service the request on the
    // connection thread? This might be hard to do.
    int numConnections = 75;
    List<RecordServicePlannerClient> clients = Lists.newArrayList();

    boolean gotServerBusy = false;
    for (int i = 0; i < numConnections;) {
      ++i;
      try {
        RecordServicePlannerClient planner =
          new RecordServicePlannerClient("localhost", PLANNER_PORT);
        clients.add(planner);
      } catch(TRecordServiceException ex) {
        assertEquals(ex.code, TErrorCode.SERVICE_BUSY);
        gotServerBusy = true;

        // Closing an existing connection should work.
        assertTrue(clients.size() > 0);
        RecordServicePlannerClient c = clients.remove(0);
        c.close();
        // TODO: we need a sleep to because close is processed asynchronously
        // somewhere in thrift. This is kind of a hack but only marginally as
        // a reasonable client should sleep before retrying.
        Thread.sleep(200); // ms

        c = new RecordServicePlannerClient("localhost", PLANNER_PORT);
        clients.add(c);
      }
    }

    for (RecordServicePlannerClient c: clients) {
      c.close();
    }

    // If this fails, increase numConnections. It must be larger than what the
    // service is configured to (default = 64).
    assertTrue(gotServerBusy);
  }

  @Test
  public void testWorkerConnections() throws RuntimeException, IOException,
      TRecordServiceException, InterruptedException {
    // This is more than the maximum number of client threads.
    // TODO: fix the thrift connections to not service the request on the
    // connection thread? This might be hard to do.
    int numConnections = 75;
    List<RecordServiceWorkerClient> clients = Lists.newArrayList();

    boolean gotServerBusy = false;
    for (int i = 0; i < numConnections;) {
      ++i;
      try {
        RecordServiceWorkerClient worker = new RecordServiceWorkerClient();
        worker.connect("localhost", WORKER_PORT);
        clients.add(worker);
      } catch(TRecordServiceException ex) {
        assertEquals(ex.code, TErrorCode.SERVICE_BUSY);
        gotServerBusy = true;

        // Closing an existing connection should work.
        assertTrue(clients.size() > 0);
        RecordServiceWorkerClient c = clients.remove(0);
        c.close();
        Thread.sleep(200);

        c = new RecordServiceWorkerClient();
        c.connect("localhost", WORKER_PORT);
        clients.add(c);
      }
    }

    for (RecordServiceWorkerClient c: clients) {
      c.close();
    }

    // If this fails, increase numConnections. It must be larger than what the
    // service is configured to (default = 64).
    assertTrue(gotServerBusy);
  }
}
