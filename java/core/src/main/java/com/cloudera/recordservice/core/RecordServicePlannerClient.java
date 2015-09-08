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

import java.io.IOException;
import java.net.Socket;

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
/**
 * Java client for the RecordServicePlanner. This class is not thread safe.
 */
public class RecordServicePlannerClient {
  private final static Logger LOG =
      LoggerFactory.getLogger(RecordServicePlannerClient.class);

  // Planner client connection. null if closed.
  private RecordServicePlanner.Client plannerClient_;
  private TProtocol protocol_;
  private ProtocolVersion protocolVersion_ = null;
  private String kerberosPrincipal_ = null;
  private DelegationToken delegationToken_ = null;

  // Number of consecutive attempts before failing any request.
  private int maxAttempts_ = 3;

  // Duration to sleep between retry attempts.
  private int retrySleepMs_ = 1000;

  // Millisecond timeout for TSocket, 0 means infinite timeout.
  private int timeoutMs_ = 20000;

  private final String USER = System.getProperty("user.name");

  /**
   * Builder to create worker client with various configs.
   */
  public final static class Builder {
    RecordServicePlannerClient client_ = new RecordServicePlannerClient();

    public Builder setMaxAttempts(int maxAttempts) {
      if (maxAttempts <= 0) {
        throw new IllegalArgumentException("Attempts must be greater than zero.");
      }
      LOG.debug("Setting maxAttempts to " + maxAttempts);
      client_.maxAttempts_ = maxAttempts;
      return this;
    }

    public Builder setSleepDurationMs(int retrySleepMs) {
      if (retrySleepMs < 0) {
        throw new IllegalArgumentException("Sleep duration must be non-negative.");
      }
      LOG.debug("Setting sleep duration to " + retrySleepMs);
      client_.retrySleepMs_ = retrySleepMs;
      return this;
    }

    public Builder setKerberosPrincipal(String principal) {
      if (client_.delegationToken_ != null) {
        // TODO: is this the behavior we want? Maybe try one then the other?
        throw new IllegalStateException(
            "Cannot set both kerberos principal and delegation token.");
      }
      client_.kerberosPrincipal_ = principal;
      return this;
    }

    public Builder setDelegationToken(DelegationToken token) {
      if (client_.kerberosPrincipal_ != null) {
        // TODO: is this the behavior we want? Maybe try one then the other?
        throw new IllegalStateException(
            "Cannot set both kerberos principal and delegation token.");
      }
      client_.delegationToken_ = token;
      return this;
    }

    public Builder setTimeoutMs(int timeoutMs) {
      if (timeoutMs < 0) {
        throw new IllegalArgumentException(
            "Timeout must not be less than 0. Timeout=" + timeoutMs);
      }
      LOG.debug("Setting timeoutMs to " + timeoutMs);
      client_.timeoutMs_ = timeoutMs;
      return this;
    }

    /**
     * Creates a planner client connecting to 'hostname'/'port' with previously
     * set options, and the caller must call close().
     */
    public RecordServicePlannerClient connect(String hostname, int port)
        throws RecordServiceException, IOException {
      client_.connect(hostname, port);
      return client_;
    }

    /**
     * Get the plan request result.
     */
    public PlanRequestResult planRequest(String hostname, int port, Request request)
        throws IOException, RecordServiceException {
      try {
        client_.connect(hostname, port);
        return client_.planRequest(request);
      } finally {
        client_.close();
      }
    }

    /**
     * Get the schema for 'request'.
     */
    public GetSchemaResult getSchema(String hostname, int port, Request request)
        throws IOException, RecordServiceException {
      try {
        client_.connect(hostname, port);
        return client_.getSchema(request);
      } finally {
        client_.close();
      }
    }
  }

  /**
   * Opens a connection to the RecordServicePlanner.
   */
  private void connect(String hostname, int port)
      throws IOException, RecordServiceException {
    TTransport transport = ThriftUtils.createTransport("RecordServicePlanner", hostname,
        port, kerberosPrincipal_, delegationToken_, timeoutMs_);
    protocol_ = new TBinaryProtocol(transport);
    plannerClient_ = new RecordServicePlanner.Client(protocol_);
    try {
      protocolVersion_ = ThriftUtils.fromThrift(plannerClient_.GetProtocolVersion());
      LOG.debug("Connected to RecordServicePlanner with version: " + protocolVersion_);
    } catch (TRecordServiceException e) {
      // For 'GetProtocolVersion' call, the server side will first establish
      // the connection, and then throws the exception when it's processing the call.
      // The client is responsible to close the connection after seeing the exception.
      close();
      LOG.warn("Connection is rejected because RecordServicePlanner has reached the " +
          "maximum number of connections it is able to handle.");
      throw new RecordServiceException("Connection to RecordServicePlanner at "
          + hostname + ":" + port + " is rejected. ", e);
    } catch (TTransportException e) {
      LOG.warn("Could not connect to RecordServicePlanner. " + e);
      if (e.getType() == TTransportException.END_OF_FILE) {
        TRecordServiceException ex = new TRecordServiceException();
        ex.code = TErrorCode.SERVICE_BUSY;
        ex.message = "Connection to RecordServicePlanner at " + hostname + ":" + port +
            " has failed. Please check if the client has the same security setting as " +
            "the server.";
        throw new RecordServiceException(ex);
      }
      throw new IOException("Could not get service protocol version from " +
          "RecordServicePlanner at " + hostname + ":" + port, e);
    } catch (TException e) {
      LOG.warn("Could not connection to RecordServicePlanner. " + e);
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
      LOG.info("Closing RecordServicePlanner connection.");
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
   * Returns true if this client is authenticated with kerberos.
   */
  public boolean isKerberosAuthenticated() { return kerberosPrincipal_ != null; }

  /**
   * Calls the RecordServicePlanner to generate a new plan - set of tasks that can be
   * executed using a RecordServiceWorker.
   */
  public PlanRequestResult planRequest(Request request)
      throws IOException, RecordServiceException {
    validateIsConnected();

    TPlanRequestResult planResult;
    TException firstException = null;
    boolean connected = true;
    for (int i = 0; i < maxAttempts_; ++i) {
      try {
        if (!connected) {
          connected = waitAndReconnect();
          if (!connected) continue;
        }
        LOG.info("Planning request: {} with attempt {}/{}", request, i + 1, maxAttempts_);
        TPlanRequestParams planParams = request.request_;
        planParams.client_version = TProtocolVersion.V1;
        planParams.setUser(USER);
        planResult = plannerClient_.PlanRequest(planParams);
        LOG.debug("PlanRequest generated {} tasks.", planResult.tasks.size());
        return new PlanRequestResult(planResult);
      } catch (TRecordServiceException e) {
        switch (e.code) {
          case SERVICE_BUSY:
            if (firstException == null) firstException = e;
            LOG.warn("Failed to planRequest: ", e);
            sleepForRetry();
            break;
          default:
            throw new RecordServiceException(e);
        }
      } catch (TException e) {
        connected = false;
        if (firstException == null) firstException = e;
        LOG.warn("Failed to planRequest: ", e);
      }
    }
    handleThriftException(firstException, "Could not plan request.");
    throw new RuntimeException(firstException);
  }

  /**
   * Calls the RecordServicePlanner to return the schema for a request.
   */
  public GetSchemaResult getSchema(Request request)
      throws IOException, RecordServiceException {
    validateIsConnected();
    TGetSchemaResult result;
    TException firstException = null;
    boolean connected = true;
    for (int i = 0; i < maxAttempts_; ++i) {
      try {
        if (!connected) {
          connected = waitAndReconnect();
          if (!connected) continue;
        }
        LOG.info("Getting schema for request: {} with attempt {}/{}",
            request, i + 1, maxAttempts_);
        TPlanRequestParams planParams = request.request_;
        planParams.setUser(USER);
        planParams.client_version = TProtocolVersion.V1;
        result = plannerClient_.GetSchema(planParams);
        return new GetSchemaResult(result);
      } catch (TRecordServiceException e) {
        switch (e.code) {
          case SERVICE_BUSY:
            if (firstException == null) firstException = e;
            LOG.warn("Failed to getSchema: ", e);
            sleepForRetry();
            break;
          default:
            throw new RecordServiceException(e);
        }
      } catch (TException e) {
        connected = false;
        if (firstException == null) firstException = e;
        LOG.warn("Failed to getSchema: ", e);
      }
    }
    handleThriftException(firstException, "Could not get schema.");
    throw new RuntimeException(firstException);
  }

  /**
   * Returns a delegation token for the current user. If renewer is set, this renewer
   * can renew the token.
   */
  public DelegationToken getDelegationToken(String renewer)
      throws RecordServiceException, IOException {
    try {
      return new DelegationToken(plannerClient_.GetDelegationToken(USER, renewer));
    } catch (TRecordServiceException e) {
      throw new RecordServiceException(e);
    } catch (TException e) {
      throw new IOException("Could not get delegation token.", e);
    }
  }

  /**
   * Cancels the token.
   */
  public void cancelDelegationToken(DelegationToken token)
      throws RecordServiceException, IOException {
    try {
      plannerClient_.CancelDelegationToken(token.toThrift());
    } catch (TRecordServiceException e) {
      throw new RecordServiceException(e);
    } catch (TException e) {
      throw new IOException("Could not cancel delegation token.", e);
    }
  }

  /**
   * Renews the token.
   */
  public void renewDelegationToken(DelegationToken token)
      throws RecordServiceException, IOException {
    try {
      plannerClient_.RenewDelegationToken(token.toThrift());
    } catch (TRecordServiceException e) {
      throw new RecordServiceException(e);
    } catch (TException e) {
      throw new IOException("Could not renew delegation token.", e);
    }
  }

  /**
   * Private constructor, use builder.
   */
  private RecordServicePlannerClient() { }

  /**
   * Closes the underlying transport, used to simulate an error with the service
   * connection.
   *
   * @VisibleForTesting
   */
  void closeConnectionForTesting() {
    protocol_.getTransport().close();
    assert (!protocol_.getTransport().isOpen());
  }

  /**
   * Handles TException, throwing a more canonical exception.
   * generalMsg is thrown if we can't infer more information from e.
   */
  private void handleThriftException(TException e, String generalMsg)
      throws RecordServiceException, IOException {
    // TODO: this should mark the connection as bad on some error codes.
    if (e instanceof TRecordServiceException) {
      throw new RecordServiceException((TRecordServiceException) e);
    } else if (e instanceof TTransportException) {
      StringBuilder msg = new StringBuilder("Could not reach service");
      if (e.getCause() != null) {
        msg.append(" because of ").append(e.getCause().toString());
      }
      msg.append(".");
      LOG.warn(msg.toString());
      throw new IOException(msg.toString(), e);
    } else {
      throw new IOException(generalMsg, e);
    }
  }

  private void validateIsConnected() throws RuntimeException {
    if (plannerClient_ == null) {
      throw new RuntimeException("Client not connected.");
    }
  }

  /**
   * Sleeps for retrySleepMs_ and reconnects to the planner. Returns
   * true if the connection was established.
   */
  private boolean waitAndReconnect() {
    sleepForRetry();
    try {
      Socket socket = ((TSocket) protocol_.getTransport()).getSocket();
      if (socket.isClosed()) {
        LOG.debug("Socket is closed, will reconnect to {}:{}",
            socket.getInetAddress().getHostAddress(), socket.getPort());
        connect(socket.getInetAddress().getHostAddress(), socket.getPort());
      }
      if (!protocol_.getTransport().isOpen()) {
        LOG.debug("TTransport is closed, will reopen");
        protocol_.getTransport().open();
        plannerClient_ = new RecordServicePlanner.Client(protocol_);
      }
      return true;
    } catch (Exception e) {
      // TRecordServiceException | TTransportException | IOException e
      LOG.warn("Failed to reconnect: ", e);
      return false;
    }
  }

  /**
   * Sleeps for retrySleepMs_.
   */
  private void sleepForRetry() {
    if (LOG.isInfoEnabled()) {
      LOG.info("Sleeping for " + retrySleepMs_ + "ms before retrying.");
    }
    try {
      Thread.sleep(retrySleepMs_);
    } catch (InterruptedException e) {
      LOG.error("Failed sleeping: " + e);
    }
  }
}
