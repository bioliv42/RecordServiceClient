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

import com.cloudera.recordservice.thrift.RecordServiceWorker;
import com.cloudera.recordservice.thrift.TExecTaskParams;
import com.cloudera.recordservice.thrift.TExecTaskResult;
import com.cloudera.recordservice.thrift.TFetchParams;
import com.cloudera.recordservice.thrift.TFetchResult;
import com.cloudera.recordservice.thrift.TStats;
import com.cloudera.recordservice.thrift.TUniqueId;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

/**
 * Java client for the RecordServiceWorker. This class is not thread safe.
 * TODO: Don't expose raw Thrift objects, use proper logger, add Kerberos support.
 */
public class RecordServiceWorkerClient {
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
   */
  public void connect(String hostname, int port) throws TException {
    if (workerClient_ != null) {
      throw new RuntimeException("Already connected. Must call close() first.");
    }

    TTransport transport = new TSocket(hostname, port);
    try {
      transport.open();
    } catch (TTransportException e) {
      System.err.println(String.format("Could not connect to RecordServiceWorker: %s:%d",
          hostname, port));
      throw e;
    }
    protocol_ = new TBinaryProtocol(transport);
    workerClient_ = new RecordServiceWorker.Client(protocol_);
    protocolVersion_ = ThriftUtils.fromThrift(workerClient_.GetProtocolVersion());
    isClosed_ = false;
  }

  /**
   * Close the connection to the RecordServiceWorker. All open tasks will also be
   * closed.
   */
  public void close() {
    if (protocol_ != null && !isClosed_) {
      for (TUniqueId handle: activeTasks_) {
        try {
          workerClient_.CloseTask(handle);
        } catch (TException e) {
          // Ignore. TODO: log
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
  public TUniqueId execTask(ByteBuffer task) throws TException {
    validateIsConnected();
    return execTaskInternal(task).getHandle();
  }

  /**
   * Executes the task asynchronously, returning a Rows object that can be
   * used to fetch results.
   */
  public Records execAndFetch(ByteBuffer task) throws IOException {
    validateIsConnected();
    try {
      TExecTaskResult result = execTaskInternal(task);
      return new Records(this, result.getHandle(), result.schema);
    } catch (TException e) {
      throw new IOException(e);
    }
  }


  /**
   * Fetches a batch of records and returns the result.
   */
  public TFetchResult fetch(TUniqueId handle) throws TException {
    validateIsConnected();
    validateHandleIsActive(handle);
    TFetchParams fetchParams = new TFetchParams(handle);
    try {
      return workerClient_.Fetch(fetchParams);
    } catch (TException e) {
      System.err.println("Could not fetch from task: " + e.getMessage());
      throw e;
    }
  }

  /**
   * Gets stats on the current task executing.
   */
  public TStats getTaskStats(TUniqueId handle) throws TException {
    validateIsConnected();
    validateHandleIsActive(handle);
    return workerClient_.GetTaskStats(handle);
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
          throws TException {
    Preconditions.checkNotNull(task);
    TExecTaskParams taskParams = new TExecTaskParams(task);
    try {
      if (fetchSize_ != null) taskParams.setFetch_size(fetchSize_);
      TExecTaskResult result = workerClient_.ExecTask(taskParams);
      Preconditions.checkState(!activeTasks_.contains(result.handle));
      activeTasks_.add(result.handle);
      return result;
    } catch (TException e) {
      System.err.println("Could not exec task: " + e.getMessage());
      throw e;
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
