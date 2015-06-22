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

import com.cloudera.recordservice.core.RecordServiceWorkerClient;
import com.cloudera.recordservice.core.Records;
import com.cloudera.recordservice.mapreduce.RecordServiceInputFormatBase;
import com.cloudera.recordservice.thrift.TNetworkAddress;
import com.cloudera.recordservice.thrift.TRecordServiceException;

/**
 * Core RecordReader functionality. Classes that implement the MR RecordReader
 * interface should contain this object.
 */
public class RecordReaderCore {
  // Optional configuration option for performance tuning that configures
  // the number of max number of records returned when fetching results from
  // the RecordService. If not set, server default will be used.
  // TODO: It would be nice for the server to adjust this automatically based
  // on how fast the client is able to process records.
  public final static String FETCH_SIZE_CONF = "recordservice.fetch.size";

  private RecordServiceWorkerClient worker_;

  // Current row batch that is being processed.
  private Records records_;

  // Schema for records_
  private Schema schema_;

  public RecordReaderCore(Configuration config, TaskInfo taskInfo)
      throws TRecordServiceException, IOException {
    try {
      // TODO: handle multiple locations.
      TNetworkAddress task = taskInfo.getLocations()[0];
      int fetchSize = config.getInt(FETCH_SIZE_CONF, -1);
      worker_ = new RecordServiceWorkerClient.Builder()
          .setFetchSize(fetchSize != -1 ? fetchSize : null)
          .setKerberosPrincipal(
              config.get(RecordServiceInputFormatBase.KERBEROS_PRINCIPAL))
          .connect(task.hostname, task.port);
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

