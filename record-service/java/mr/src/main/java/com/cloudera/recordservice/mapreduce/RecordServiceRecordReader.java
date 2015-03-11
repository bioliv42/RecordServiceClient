// Copyright 2012 Cloudera Inc.
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

package com.cloudera.recordservice.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.cloudera.recordservice.client.RecordServiceWorkerClient;
import com.cloudera.recordservice.client.Records;
import com.cloudera.recordservice.client.Records.Record;
import com.cloudera.recordservice.thrift.TNetworkAddress;
import com.google.common.annotations.VisibleForTesting;

/**
 * RecordReader implementation that uses the RecordService for data access. Values
 * are returned as RecordServiceRecords, which contain schema and data for a single
 * record.
 * Keys are the record number returned by this RecordReader.
 * To reduce the creation of new objects, existing storage is reused for both
 * keys and values (objects are updated in-place).
 * TODO: Should keys just be NullWritables?
 */
public class RecordServiceRecordReader extends
    RecordReader<LongWritable, RecordServiceRecord> {
  private RecordServiceWorkerClient worker_;

  // Current row batch that is being processed.
  private Records records_;

  // Schema for records_
  private Schema schema_;

  // Current record being processed
  private Records.Record currentRSRecord_;

  // The record that is returned. Updated in-place when nextKeyValue() is called.
  private RecordServiceRecord record_;

  // The current record number assigned this record. Incremented each time
  // nextKeyValue() is called and assigned to currentKey_.
  private static long recordNum_ = 0;

  // The key corresponding to the record.
  private final LongWritable currentKey_ = new LongWritable();

  // True after initialize() has fully completed.
  private volatile boolean isInitialized_ = false;

  /**
   * Initializes the RecordReader and starts execution of the task.
   */
  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    RecordServiceInputSplit rsSplit = (RecordServiceInputSplit)split;
    initialize(rsSplit, context.getConfiguration());
  }

  /**
   * The general contract of the RecordReader is that the client (Mapper) calls
   * this method to load the next Key and Value.. before calling getCurrentKey()
   * and getCurrentValue().
   *
   * Returns true if there are more values to retrieve, false otherwise.
   */
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (!isInitialized_) {
      throw new RuntimeException("Record Reader not initialized !!");
    }

    if (!nextRecord()) return false;
    record_.reset(currentRSRecord_);
    currentKey_.set(recordNum_++);
    return true;
  }

  @Override
  public LongWritable getCurrentKey() throws IOException,
      InterruptedException {
    return currentKey_;
  }

  @Override
  public RecordServiceRecord getCurrentValue() throws IOException,
      InterruptedException {
    return record_;
  }

  @Override
  public float getProgress() {
    return records_.progress();
  }

  @Override
  public void close() throws IOException {
    if (records_ != null) records_.close();
    if (worker_ != null) worker_.close();
  }

  /**
   * Advances to the next record. Return false if there are no more records.
   */
  public boolean nextRecord() throws IOException {
    if (!isInitialized_) {
      throw new RuntimeException("Record Reader not initialized !!");
    }
    if (!records_.hasNext()) return false;
    currentRSRecord_ = records_.next();
    return true;
  }

  public Schema schema() { return schema_; }
  public Record currentRecord() { return currentRSRecord_; }
  public boolean isInitialized() { return isInitialized_; }

  @VisibleForTesting
  void initialize(RecordServiceInputSplit split, Configuration config)
      throws IOException {
    close();
    worker_ = new RecordServiceWorkerClient();

    try {
      // TODO: handle multiple locations.
      TNetworkAddress task = split.getTaskInfo().getLocations()[0];
      worker_.connect(task.hostname, task.port);

      int fetchSize = config.getInt(
          RecordServiceInputFormat.FETCH_SIZE_CONF, -1);
      worker_.setFetchSize(fetchSize != -1 ? fetchSize : null);
      records_ = worker_.execAndFetch(split.getTaskInfo().getTaskAsByteBuffer());
    } catch (Exception e) {
      throw new IOException("Failed to execute task.", e);
    }
    schema_ = new Schema(records_.getSchema());
    record_ = new RecordServiceRecord(schema_);
    isInitialized_ = true;
  }
}
