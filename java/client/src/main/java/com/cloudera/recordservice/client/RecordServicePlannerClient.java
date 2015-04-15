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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.recordservice.thrift.RecordServicePlanner;
import com.cloudera.recordservice.thrift.TErrorCode;
import com.cloudera.recordservice.thrift.TGetSchemaResult;
import com.cloudera.recordservice.thrift.TPlanRequestParams;
import com.cloudera.recordservice.thrift.TPlanRequestResult;
import com.cloudera.recordservice.thrift.TProtocolVersion;
import com.cloudera.recordservice.thrift.TRecordServiceException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * Java client for the RecordServicePlanner. This class is not thread safe.
 * TODO: This class should not expose the raw Thrift objects, should use proper logger.
 * TODO: Add retry in the clients.
 */
public class RecordServicePlannerClient {
  private final static Logger LOG =
      LoggerFactory.getLogger(RecordServicePlannerClient.class);

  // Planner client connection. null if closed.
  private RecordServicePlanner.Client plannerClient_;
  private TProtocol protocol_;
  private ProtocolVersion protocolVersion_ = null;

  /**
   * Generates a plan for 'request', connecting to the planner service at
   * hostname/port.
   */
  public static TPlanRequestResult planRequest(
      String hostname, int port, Request request)
      throws IOException, TRecordServiceException {
    RecordServicePlannerClient client = null;
    try {
      client = new RecordServicePlannerClient(hostname, port);
      return client.planRequest(request);
    } finally {
      if (client != null) client.close();
    }
  }

  /**
   * Gets the schema for 'request', connecting to the planner service at
   * hostname/port.
   */
  public static TGetSchemaResult getSchema(
      String hostname, int port, Request request)
      throws IOException, TRecordServiceException {
    RecordServicePlannerClient client = null;
    try {
      client = new RecordServicePlannerClient(hostname, port);
      return client.getSchema(request);
    } finally {
      if (client != null) client.close();
    }
  }

  /**
   * Opens a connection to the RecordServicePlanner.
   */
  public RecordServicePlannerClient(String hostname, int port)
      throws IOException, TRecordServiceException {
    LOG.info("Connecting to RecordServicePlanner at " + hostname + ":" + port);
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
      LOG.debug("Connected to planner service with version: " + protocolVersion_);
    } catch (TTransportException e) {
      LOG.warn("Could not connect to planner service. " + e);
      if (e.getType() == TTransportException.END_OF_FILE) {
        // TODO: this is basically a total hack. It looks like there is an issue in
        // thrift where the exception thrown by the server is swallowed.
        TRecordServiceException ex = new TRecordServiceException();
        ex.code = TErrorCode.SERVICE_BUSY;
        ex.message = "Server is likely busy. Try the request again.";
        throw ex;
      }
      throw new IOException("Could not get serivce protocol version.", e);
    } catch (TException e) {
      LOG.warn("Could not connection to planner service. " + e);
      throw new IOException("Could not get service protocol version. It's likely " +
          "the service at " + hostname + ":" + port + " is not running the " +
          "RecordServicePlanner.", e);
    }
  }

  /**
   * Closes a connection to the RecordServicePlanner.
   */
  public void close() {
    if (plannerClient_ != null) {
      LOG.debug("Closing RecordServicePlanner connection.");
      protocol_.getTransport().close();
      plannerClient_ = null;
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
  public TPlanRequestResult planRequest(Request request)
      throws IOException, TRecordServiceException {
    validateIsConnected();

    TPlanRequestResult planResult;
    try {
      LOG.info("Planning request: " + request);
      TPlanRequestParams planParams = request.request_;
      planParams.client_version = TProtocolVersion.V1;
      planResult = plannerClient_.PlanRequest(planParams);
    } catch (TException e) {
      handleThriftException(e, "Could not plan request.");
      throw new RuntimeException(e);
    }
    LOG.debug("PlanRequest generated " + planResult.tasks.size() + " tasks.");
    return planResult;
  }

  /**
   * Calls the RecordServicePlanner to return the schema for a request.
   */
  public TGetSchemaResult getSchema(Request request)
    throws IOException, TRecordServiceException {
    validateIsConnected();
    TGetSchemaResult result;
    try {
      LOG.info("Getting schema for request: " + request);
      TPlanRequestParams planParams = request.request_;
      planParams.client_version = TProtocolVersion.V1;
      result = plannerClient_.GetSchema(planParams);
    } catch (TException e) {
      handleThriftException(e, "Could not get schema.");
      throw new RuntimeException(e);
    }
    return result;
  }

  /**
   * Closes the underlying transport, used to simulate an error with the service
   * connection.
   */
  @VisibleForTesting
  void closeConnection() {
    protocol_.getTransport().close();
    Preconditions.checkState(!protocol_.getTransport().isOpen());
  }

  /**
   * Handles TException, throwing a more canonical exception.
   * generalMsg is thrown if we can't infer more information from e.
   */
  private void handleThriftException(TException e, String generalMsg)
      throws TRecordServiceException, IOException {
    // TODO: this should mark the connection as bad on some error codes.
    if (e instanceof TRecordServiceException) {
      throw (TRecordServiceException)e;
    } else if (e instanceof TTransportException) {
      LOG.warn("Could not reach planner serivce.");
      throw new IOException("Could not reach service.", e);
    } else {
      throw new IOException(generalMsg, e);
    }
  }

  private void validateIsConnected() throws RuntimeException {
    if (plannerClient_ == null) {
      throw new RuntimeException("Client not connected.");
    }
  }
}
