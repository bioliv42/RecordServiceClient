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

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.cloudera.recordservice.thrift.RecordServicePlanner;
import com.cloudera.recordservice.thrift.TPlanRequestParams;
import com.cloudera.recordservice.thrift.TPlanRequestResult;
import com.cloudera.recordservice.thrift.TProtocolVersion;

/**
 * Java client for the RecordServicePlanner. This class is not thread safe.
 * TODO: This class should not expose the raw Thrift objects, should use proper logger.
 */
public class RecordServicePlannerClient {
  private RecordServicePlanner.Client plannerClient_;
  private TProtocol protocol_;
  private boolean isClosed_ = false;
  private TProtocolVersion protocolVersion_ = null;

  /**
   * Opens a connection to the RecordServicePlanner.
   */
  public void connect(String hostname, int port) throws TException {
    if (plannerClient_ != null) throw new RuntimeException("Already connected.");

    TTransport transport = new TSocket(hostname, port);
    try {
      transport.open();
    } catch (TTransportException e) {
      System.err.println(String.format(
          "Could not connect to RecordServicePlanner: %s:%d", hostname, port));
      throw e;
    }
    protocol_ = new TBinaryProtocol(transport);
    plannerClient_ = new RecordServicePlanner.Client(protocol_);
    protocolVersion_ = plannerClient_.GetProtocolVersion();
  }

  /**
   * Closes a connection to the RecordServicePlanner.
   */
  public void close() {
    if (protocol_ != null && !isClosed_) {
      protocol_.getTransport().close();
      isClosed_ = true;
    }
  }

  /**
   * Returns the protocol version of the connected service.
   */
  public TProtocolVersion getProtocolVersion() throws RuntimeException, TException {
    validateIsConnected();
    return protocolVersion_;
  }

  /**
   * Calls the RecordServicePlanner to generate a new plan - set of tasks that can be
   * executed using a RecordServiceWorker.
   */
  public TPlanRequestResult planRequest(String query) throws TException {
    validateIsConnected();

    TPlanRequestResult planResult;
    try {
      TPlanRequestParams planParams = new TPlanRequestParams(TProtocolVersion.V1, query);
      planResult = plannerClient_.PlanRequest(planParams);
    } catch (TException e) {
      // TODO: this should mark the connection as bad on some error codes.
      System.err.println("Could not plan request: " + e.getMessage());
      throw e;
    }
    System.out.println("Generated " + planResult.tasks.size() + " tasks.");
    return planResult;
  }

  private void validateIsConnected() throws RuntimeException {
    if (plannerClient_ == null || isClosed_) {
      throw new RuntimeException("Client not connected.");
    }
  }
}
