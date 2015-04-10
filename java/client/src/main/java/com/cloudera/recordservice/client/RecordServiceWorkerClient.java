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
import java.nio.ByteBuffer;
import java.util.Set;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.recordservice.thrift.RecordServiceWorker;
import com.cloudera.recordservice.thrift.TExecTaskParams;
import com.cloudera.recordservice.thrift.TExecTaskResult;
import com.cloudera.recordservice.thrift.TFetchParams;
import com.cloudera.recordservice.thrift.TFetchResult;
import com.cloudera.recordservice.thrift.TNetworkAddress;
import com.cloudera.recordservice.thrift.TRecordServiceException;
import com.cloudera.recordservice.thrift.TTaskStatus;
import com.cloudera.recordservice.thrift.TUniqueId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

/**
 * Java client for the RecordServiceWorker. This class is not thread safe.
 * TODO: Don't expose raw Thrift objects, use proper logger, add Kerberos support.
 */
public class RecordServiceWorkerClient {
  private final static Logger LOG =
    LoggerFactory.getLogger(RecordServiceWorkerClient.class);

  // Worker client connection. null if not connected/closed
  private RecordServiceWorker.Client workerClient_;
  private TProtocol protocol_;
  private ProtocolVersion protocolVersion_ = null;

  // The set of all active tasks.
  private Set<TUniqueId> activeTasks_ = Sets.newHashSet();

  // Fetch size to pass to execTask(). If null, server will determine fetch size.
  private Integer fetchSize_;

  // Memory limit to pass to execTask(). If null, server will manage this.
  private Long memLimit_;

  /**
   * Connects to the RecordServiceWorker.
   * @throws TException
   */
  public void connect(TNetworkAddress address) throws IOException {
    connect(address.hostname, address.port);
  }

  public void connect(String hostname, int port) throws IOException {
    if (workerClient_ != null) {
      throw new RuntimeException("Already connected. Must call close() first.");
    }

    TTransport transport = new TSocket(hostname, port);
    try {
      LOG.info("Connecting to worker at " + hostname + ":" + port);
      transport.open();
    } catch (TTransportException e) {
      throw new IOException(String.format("Could not connect to RecordServiceWorker: %s:%d",
          hostname, port), e);
    }
    protocol_ = new TBinaryProtocol(transport);
    workerClient_ = new RecordServiceWorker.Client(protocol_);
    try {
      protocolVersion_ = ThriftUtils.fromThrift(workerClient_.GetProtocolVersion());
      LOG.debug("Connected to worker service with version: " + protocolVersion_);
    } catch (TException e) {
      throw new IOException("Could not get service protocol version. It's likely " +
          "the service at " + hostname + ":" + port + " is not running the " +
          "RecordServiceWorker.", e);
    }
  }

  /**
   * Close the connection to the RecordServiceWorker. All open tasks will also be
   * closed.
   */
  public void close() {
    if (workerClient_ != null) {
      LOG.debug("Closing RecordServiceWorker connection.");
      for (TUniqueId handle: activeTasks_) {
        try {
          workerClient_.CloseTask(handle);
        } catch (TException e) {
          LOG.warn(
              "Failed to close task handle=" + handle + " reason=" + e.getMessage());
        }
      }
      activeTasks_.clear();
      protocol_.getTransport().close();
      workerClient_ = null;
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
   * Closes the underlying transport, used to simulate an error with the service
   * connection.
   */
  public void closeTask(TUniqueId handle) {
    validateIsConnected();
    if (activeTasks_.contains(handle)) {
      LOG.debug("Closing RecordServiceWorker task: " + handle);
      try {
        workerClient_.CloseTask(handle);
      } catch (TException e) {
        LOG.warn(
            "Failed to close task handle=" + handle + " reason=" + e.getMessage());
      }
      activeTasks_.remove(handle);
    }
  }

  /**
   * Executes the task asynchronously, returning the handle the client.
   */
  public TUniqueId execTask(ByteBuffer task)
      throws TRecordServiceException, IOException {
    validateIsConnected();
    return execTaskInternal(task).getHandle();
  }

  /**
   * Executes the task asynchronously, returning a Rows object that can be
   * used to fetch results.
   */
  public Records execAndFetch(ByteBuffer task)
      throws TRecordServiceException, IOException {
    validateIsConnected();
    TExecTaskResult result = execTaskInternal(task);
    Records records = null;
    try {
      records = new Records(this, result.getHandle(), result.schema);
      return records;
    } finally {
      if (records == null) closeTask(result.getHandle());
    }
  }

  /**
   * Fetches a batch of records and returns the result.
   */
  public TFetchResult fetch(TUniqueId handle)
      throws TRecordServiceException, IOException {
    validateIsConnected();
    validateHandleIsActive(handle);
    TFetchParams fetchParams = new TFetchParams(handle);
    try {
      LOG.trace("Calling fetch(): " + handle);
      return workerClient_.Fetch(fetchParams);
    } catch (TException e) {
      handleThriftException(e, "Could not call fetch.");
      throw new RuntimeException(e);
    }
  }

  /**
   * Gets status on the current task executing.
   */
  public TTaskStatus getTaskStatus(TUniqueId handle)
      throws TRecordServiceException, IOException {
    validateIsConnected();
    validateHandleIsActive(handle);
    LOG.debug("Calling getTaskStatus(): " + handle);
    try {
      return workerClient_.GetTaskStatus(handle);
    } catch (TException e) {
      handleThriftException(e, "Could not call getTaskStatus.");
      throw new RuntimeException(e);
    }
  }

  /**
   * Sets the fetch size. Set to null to use server default.
   */
  public void setFetchSize(Integer fetchSize) { fetchSize_ = fetchSize; }

  /**
   * Sets the memory limit, in bytes.
   */
  public void setMemLimit(Long memLimit) { memLimit_ = memLimit; }

  /**
   * Returns the number of active tasks for this worker.
   */
  public int numActiveTasks() { return activeTasks_.size(); }

  /**
   * Closes the underlying transport, simulating if the service connection is
   * dropped.
   */
  @VisibleForTesting
  void closeConnection() {
    protocol_.getTransport().close();
    Preconditions.checkState(!protocol_.getTransport().isOpen());
  }

  /**
   * Executes the task asynchronously, returning the handle the client.
   */
  private TExecTaskResult execTaskInternal(ByteBuffer task)
          throws TRecordServiceException, IOException {
    Preconditions.checkNotNull(task);
    TExecTaskParams taskParams = new TExecTaskParams(task);
    try {
      LOG.debug("Executing task");
      if (fetchSize_ != null) taskParams.setFetch_size(fetchSize_);
      if (memLimit_ != null) taskParams.setMem_limit(memLimit_);
      TExecTaskResult result = workerClient_.ExecTask(taskParams);
      Preconditions.checkState(!activeTasks_.contains(result.handle));
      activeTasks_.add(result.handle);
      LOG.info("Got task handle: " + result.handle);
      return result;
    } catch (TException e) {
      handleThriftException(e, "Could not exec task.");
      throw new RuntimeException(e);
    }
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
      LOG.warn("Could not reach worker serivce.");
      throw new IOException("Could not reach service.", e);
    } else {
      throw new IOException(generalMsg, e);
    }
  }

  private void validateIsConnected() throws RuntimeException {
    if (workerClient_ == null) throw new RuntimeException("Client not connected.");
  }

  private void validateHandleIsActive(TUniqueId handle) {
    if (!activeTasks_.contains(handle)) {
      throw new IllegalArgumentException("Invalid task handle.");
    }
  }
}
