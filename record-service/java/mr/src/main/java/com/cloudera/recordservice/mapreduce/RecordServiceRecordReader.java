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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.cloudera.recordservice.client.RecordServiceWorkerClient;
import com.cloudera.recordservice.client.Rows;
import com.cloudera.recordservice.client.Rows.Row;

/**
 * RecordReader implementation that uses the RecordService for data access. Values
 * are returned as RecordServiceRecords, which contain schema and data for a single row.
 * Keys are the row number returned by this RecordReader.
 * To reduce the creation of new objects, existing storage is reused for both
 * keys and values (objects are updated in-place).
 * TODO: Should keys just be NullWritables?
 */
public class RecordServiceRecordReader extends
    RecordReader<WritableComparable<?>, RecordServiceRecord> {
  private RecordServiceWorkerClient worker_;

  // Current row batch that is being processed.
  private Rows rows_;

  // Schema for rows_;
  private Schema schema_;

  // Current row being processed
  private Rows.Row currentRow_;

  // The current record being processed. Updated in-place when nextKeyValue() is called.
  private RecordServiceRecord currentRecord_;

  // The current row number assigned this record. Incremented each time nextKeyValue() is
  // called and assigned to currentKey_.
  private static long rowNum_ = 0;

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
    worker_ = new RecordServiceWorkerClient();

    try {
      // TODO: Make port configurable, handle multiple locations.
      worker_.connect(rsSplit.getLocations()[0], 40100);

      int fetchSize = context.getConfiguration().getInt(
          RecordServiceInputFormat.FETCH_SIZE_CONF, -1);
      worker_.setFetchSize(fetchSize != -1 ? fetchSize : null);
      rows_ = worker_.execAndFetch(rsSplit.getTaskInfo().getTaskAsByteBuffer());
    } catch (Exception e) {
      throw new IOException(e);
    }
    schema_ = new Schema(rows_.getSchema());
    currentRecord_ = new RecordServiceRecord(schema_);
    isInitialized_ = true;
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
    currentRecord_.reset(currentRow_);
    currentKey_.set(rowNum_++);
    return true;
  }

  @Override
  public WritableComparable<?> getCurrentKey() throws IOException,
      InterruptedException {
    return currentKey_;
  }

  @Override
  public RecordServiceRecord getCurrentValue() throws IOException,
      InterruptedException {
    return currentRecord_;
  }

  @Override
  public float getProgress() {
    return rows_.progress();
  }

  @Override
  public void close() throws IOException {
    if (rows_ != null) rows_.close();
    if (worker_ != null) worker_.close();
  }

  /**
   * Advances to the next record. Return false if there are no more records.
   */
  public boolean nextRecord() throws IOException {
    //if (!isInitialized_) {
    //  throw new RuntimeException("Record Reader not initialized !!");
    //}
    if (!rows_.hasNext()) return false;
    currentRow_ = rows_.next();
    return true;
  }

  public Schema schema() { return schema_; }
  public Row currentRow() { return currentRow_; }
  public boolean isInitialized() { return isInitialized_; }
}
