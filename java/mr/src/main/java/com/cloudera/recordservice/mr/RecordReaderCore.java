// Confidential Cloudera Information: Covered by NDA.
//Copyright 2012 Cloudera Inc.
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

package com.cloudera.recordservice.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.recordservice.core.NetworkAddress;
import com.cloudera.recordservice.core.RecordServiceException;
import com.cloudera.recordservice.core.RecordServiceWorkerClient;
import com.cloudera.recordservice.core.Records;
import com.cloudera.recordservice.mr.security.DelegationTokenIdentifier;
import com.cloudera.recordservice.mr.security.TokenUtils;

/**
 * Core RecordReader functionality. Classes that implement the MR RecordReader
 * interface should contain this object.
 *
 * This class can authenticate to the worker using delegation tokens. We never
 * try to authenticate using kerberos (the planner should have created the delegation
 * token) to avoid causing issues with the KDC.
 */
public class RecordReaderCore {
  private final static Logger LOG = LoggerFactory.getLogger(RecordReaderCore.class);
  // Underlying worker connection.
  private RecordServiceWorkerClient worker_;

  // Current row batch that is being processed.
  private Records records_;

  // Schema for records_
  private Schema schema_;

  // Default fetch size to use for MR. This is currently much larger than the
  // server default but perfs better this way (but uses more memory).
  // TODO: investigate this more and do this in the server. Remove this.
  private static final int DEFAULT_FETCH_SIZE = 50000;

  public RecordReaderCore(Configuration config, Credentials credentials,
      TaskInfo taskInfo) throws RecordServiceException, IOException {
    try {
      int fetchSize = config.getInt(RecordServiceConfig.FETCH_SIZE_CONF,
          DEFAULT_FETCH_SIZE);
      long memLimit = config.getLong(RecordServiceConfig.MEM_LIMIT_CONF, -1);
      long limit = config.getLong(RecordServiceConfig.RECORDS_LIMIT_CONF, -1);
      int maxAttempts =
          config.getInt(RecordServiceConfig.TASK_RETRY_ATTEMPTS_CONF, -1);
      int taskSleepMs = config.getInt(RecordServiceConfig.TASK_RETRY_SLEEP_MS_CONF, -1);
      int socketTimeoutMs =
          config.getInt(RecordServiceConfig.TASK_SOCKET_TIMEOUT_MS_CONF, -1);
      boolean enableLogging =
          config.getBoolean(RecordServiceConfig.TASK_ENABLE_SERVER_LOGGING_CONF, false);

      // Try to get the delegation token from the credentials. If it is there, use it.
      @SuppressWarnings("unchecked")
      Token<DelegationTokenIdentifier> token = (Token<DelegationTokenIdentifier>)
          credentials.getToken(DelegationTokenIdentifier.DELEGATION_KIND);

      RecordServiceWorkerClient.Builder builder =
          new RecordServiceWorkerClient.Builder();
      if (fetchSize != -1) builder.setFetchSize(fetchSize);
      if (memLimit != -1) builder.setMemLimit(memLimit);
      if (limit != -1) builder.setLimit(limit);
      if (maxAttempts != -1) builder.setMaxAttempts(maxAttempts);
      if (taskSleepMs != -1) builder.setSleepDurationMs(taskSleepMs);
      if (socketTimeoutMs != -1) builder.setTimeoutMs(socketTimeoutMs);
      if (enableLogging) builder.setLoggingLevel(LOG);
      if (token != null) builder.setDelegationToken(TokenUtils.toDelegationToken(token));

      // TODO: handle multiple locations.
      NetworkAddress task = taskInfo.getLocations()[0];
      worker_ = builder.connect(task.hostname, task.port);
      records_ = worker_.execAndFetch(taskInfo.getTask());
    } finally {
      if (records_ == null) close();
    }
    schema_ = new Schema(records_.getSchema());
  }

  /**
   * Closes the task and worker connection.
   */
  public void close() {
    if (records_ != null) records_.close();
    if (worker_ != null) worker_.close();
  }

  public Records records() { return records_; }
  public Schema schema() { return schema_; }
}

