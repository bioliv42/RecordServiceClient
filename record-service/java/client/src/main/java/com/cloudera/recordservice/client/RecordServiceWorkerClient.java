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

import java.nio.ByteBuffer;

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
import com.cloudera.recordservice.thrift.TRowBatchFormat;
import com.cloudera.recordservice.thrift.TStats;
import com.cloudera.recordservice.thrift.TUniqueId;
import com.google.common.base.Preconditions;

/**
 * Java client for the RecordServiceWorker. This class is not thread safe.
 * TODO: Don't expose raw Thrift objects, use proper logger, add Kerberos support.
 */
public class RecordServiceWorkerClient {
  RecordServiceWorker.Client workerClient_;
  TRowBatchFormat format_;
  TProtocol protocol_;
  boolean isClosed_ = false;

  public RecordServiceWorkerClient() {
    this(TRowBatchFormat.ColumnarThrift);
  }

  public RecordServiceWorkerClient(TRowBatchFormat format) {
    format_ = format;
  }

  /**
   * Connects to the RecordServiceWorker.
   */
  public void connect(String hostname, int port)
      throws TTransportException {
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
  }

  /**
   * Close the connection to the RecordServiceWorker. All open tasks will also be
   * closed.
   */
  public void close() {
    if (protocol_ != null && !isClosed_) {
      protocol_.getTransport().close();
      isClosed_ = true;
    }
  }

  /**
   * Closes the specified task. Handle will be invalidated after making this call.
   */
  public void closeTask(TUniqueId handle) throws TException {
    workerClient_.CloseTask(handle);
  }

  /**
   * Executes the task asynchronously, returning the handle the client.
   */
  public TUniqueId execTask(ByteBuffer task) throws TException {
    Preconditions.checkNotNull(task);
    try {
      TExecTaskParams taskParams = new TExecTaskParams(task);
      taskParams.row_batch_format = format_;
      TExecTaskResult result = workerClient_.ExecTask(taskParams);
      return result.getHandle();
    } catch (TException e) {
      System.err.println("Could not exec task: " + e.getMessage());
      throw e;
    }
  }

  public Rows execAndFetch(ByteBuffer task) throws TException {
    Preconditions.checkNotNull(task);
    try {
      TExecTaskParams taskParams = new TExecTaskParams(task);
      taskParams.row_batch_format = format_;
      TExecTaskResult result = workerClient_.ExecTask(taskParams);
      return new Rows(this, result.getHandle(), result.schema);
    } catch (TException e) {
      System.err.println("Could not exec task: " + e.getMessage());
      throw e;
    }
  }

  /**
   * Fetches a batch of rows and returns the result.
   */
  public TFetchResult fetch(TUniqueId handle) throws TException {
    Preconditions.checkNotNull(handle);

    TFetchParams fetchParams = new TFetchParams(handle);
    try {
      return workerClient_.Fetch(fetchParams);
    } catch (TException e) {
      System.err.println("Could not fetch from task: " + e.getMessage());
      throw e;
    }
  }

  /**
   * Fetches all rows from the server and returns the total number of rows
   * retrieved. Closes the task after executing.
   */
  public long fetchAllAndCountRows(TUniqueId handle) throws TException  {
    long totalRows = 0;
    try {
      /* Fetch results until we're done */
      TFetchResult fetchResult = null;
      do {
        fetchResult = fetch(handle);
        totalRows += fetchResult.num_rows;
      } while (!fetchResult.done);
    } finally {
      workerClient_.CloseTask(handle);
    }
    return totalRows;
  }

  /**
   * Gets stats on the current task executing.
   */
  public TStats getTaskStats(TUniqueId handle) throws TException {
    return workerClient_.GetTaskStats(handle);
  }
}
