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
import com.cloudera.recordservice.thrift.TRecordServiceException;
import com.cloudera.recordservice.thrift.TTaskStatus;
import com.cloudera.recordservice.thrift.TUniqueId;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

/**
 * Java client for the RecordServiceWorker. This class is not thread safe.
 * TODO: Don't expose raw Thrift objects, use proper logger, add Kerberos support.
 */
public class RecordServiceWorkerClient {
  private final static Logger LOG =
    LoggerFactory.getLogger(RecordServiceWorkerClient.class);

  private RecordServiceWorker.Client workerClient_;
  private TProtocol protocol_;
  private boolean isClosed_ = false;
  private ProtocolVersion protocolVersion_ = null;

  // The set of all active tasks.
  private Set<TUniqueId> activeTasks_ = Sets.newHashSet();

  // Fetch size to pass to execTask(). If null, server will determine fetch size.
  private Integer fetchSize_;

  /**
   * Connects to the RecordServiceWorker.
   * @throws TException
   */
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
      // TODO: this probably means they connected to a thrift service that is not the
      // worker service (i.e. wrong port). Improve this message.
      throw new IOException("Could not get service protocol version.", e);
    }
    isClosed_ = false;
  }

  /**
   * Close the connection to the RecordServiceWorker. All open tasks will also be
   * closed.
   */
  public void close() {
    if (protocol_ != null && !isClosed_) {
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
      isClosed_ = true;
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
   * Closes the specified task. Handle will be invalidated after making this call.
   */
  public void closeTask(TUniqueId handle) {
    validateIsConnected();
    if (activeTasks_.contains(handle)) {
      LOG.debug("Closing RecordServiceWorker task: " + handle);
      try {
        workerClient_.CloseTask(handle);
      } catch (TException e) {
        // Ignore. TODO log.
      }
      activeTasks_.remove(handle);
    }
    // TODO: what if it doesn't exist?
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
    return new Records(this, result.getHandle(), result.schema);

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
    } catch (TRecordServiceException e) {
      throw e;
    } catch (TException e) {
      throw new IOException("Could not fetch from task: " + e.getMessage(), e);
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
    } catch (TRecordServiceException e) {
      throw e;
    } catch (TException e) {
      throw new IOException("Could not call getTaskStatus: ", e);
    }
  }

  /**
   * Sets the fetch size. Set to null to use server default.
   */
  public void setFetchSize(Integer fetchSize) { fetchSize_ = fetchSize; }
  public Integer getFetchSize() { return fetchSize_; }

  /**
   * Returns the number of active tasks for this worker.
   */
  public int numActiveTasks() { return activeTasks_.size(); }

  /**
   * Executes the task asynchronously, returning the handle the client.
   */
  private TExecTaskResult execTaskInternal(ByteBuffer task)
          throws TRecordServiceException, IOException {
    Preconditions.checkNotNull(task);
    TExecTaskParams taskParams = new TExecTaskParams(task);
    try {
      if (fetchSize_ != null) taskParams.setFetch_size(fetchSize_);
      LOG.debug("Executing task");
      TExecTaskResult result = workerClient_.ExecTask(taskParams);
      Preconditions.checkState(!activeTasks_.contains(result.handle));
      activeTasks_.add(result.handle);
      LOG.info("Got task handle: " + result.handle);
      return result;
    } catch (TRecordServiceException e) {
      throw e;
    } catch (TException e) {
      throw new IOException("Could not exec task.", e);
    }
  }

  private void validateIsConnected() throws RuntimeException {
    if (workerClient_ == null || isClosed_) {
      throw new RuntimeException("Client not connected.");
    }
  }

  private void validateHandleIsActive(TUniqueId handle) {
    if (!activeTasks_.contains(handle)) {
      throw new RuntimeException("Invalid task handle.");
    }
  }
}
