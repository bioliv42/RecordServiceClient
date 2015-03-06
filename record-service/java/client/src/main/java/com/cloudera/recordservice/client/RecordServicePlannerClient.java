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

import java.io.IOException;

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
import com.cloudera.recordservice.thrift.TRecordServiceException;

/**
 * Java client for the RecordServicePlanner. This class is not thread safe.
 * TODO: This class should not expose the raw Thrift objects, should use proper logger.
 */
public class RecordServicePlannerClient {
  private RecordServicePlanner.Client plannerClient_;
  private TProtocol protocol_;
  private boolean isClosed_ = false;
  private ProtocolVersion protocolVersion_ = null;

  /**
   * Generates a plan for 'stmt', connecting to the planner service at
   * hostname/port.
   */
  public static TPlanRequestResult planRequest(String hostname, int port, String stmt)
      throws IOException, TRecordServiceException {
    RecordServicePlannerClient client = null;
    try {
      client = new RecordServicePlannerClient(hostname, port);
      return client.planRequest(stmt);
    } finally {
      if (client != null) client.close();
    }
  }

  /**
   * Opens a connection to the RecordServicePlanner.
   */
  public RecordServicePlannerClient(String hostname, int port) throws IOException {
    TTransport transport = new TSocket(hostname, port);
    try {
      transport.open();
    } catch (TTransportException e) {
      throw new IOException(String.format(
          "Could not connect to RecordServicePlanner: %s:%d", hostname, port), e);
    }
    protocol_ = new TBinaryProtocol(transport);
    plannerClient_ = new RecordServicePlanner.Client(protocol_);
    try {
      protocolVersion_ = ThriftUtils.fromThrift(plannerClient_.GetProtocolVersion());
    } catch (TException e) {
      // TODO: this probably means they connected to a thrift service that is not the
      // planner service (i.e. wrong port). Improve this message.
      throw new IOException("Could not get service protocol version.", e);
    }
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
  public ProtocolVersion getProtocolVersion() throws RuntimeException {
    validateIsConnected();
    return protocolVersion_;
  }

  /**
   * Calls the RecordServicePlanner to generate a new plan - set of tasks that can be
   * executed using a RecordServiceWorker.
   */
  public TPlanRequestResult planRequest(String query)
      throws IOException, TRecordServiceException {
    validateIsConnected();

    TPlanRequestResult planResult;
    try {
      TPlanRequestParams planParams = new TPlanRequestParams(TProtocolVersion.V1, query);
      planResult = plannerClient_.PlanRequest(planParams);
    } catch (TRecordServiceException e) {
      throw e;
    } catch (TException e) {
      // TODO: this should mark the connection as bad on some error codes.
      throw new IOException("Could not plan request.", e);
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
