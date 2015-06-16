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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.recordservice.core.Records;
import com.cloudera.recordservice.core.Records.Record;
import com.cloudera.recordservice.mr.RecordReaderCore;
import com.cloudera.recordservice.mr.RecordServiceRecord;
import com.cloudera.recordservice.mr.Schema;
import com.cloudera.recordservice.thrift.TRecordServiceException;
import com.google.common.annotations.VisibleForTesting;

/**
 * Input format which returns <Long, RecordServiceReceord>
 * TODO: is this useful? This introduces the RecordServiceRecord "object"
 * model.
 */
public class RecordServiceInputFormat extends
    RecordServiceInputFormatBase<LongWritable, RecordServiceRecord> {

  private static final Logger LOG =
      LoggerFactory.getLogger(RecordServiceInputFormat.class);

  @Override
  public RecordReader<LongWritable, RecordServiceRecord>
      createRecordReader(InputSplit split, TaskAttemptContext context)
          throws IOException, InterruptedException {
    RecordServiceRecordReader rReader = new RecordServiceRecordReader();
    rReader.initialize(split, context);
    return rReader;
  }

  /**
   * RecordReader implementation that uses the RecordService for data access. Values
   * are returned as RecordServiceRecords, which contain schema and data for a single
   * record.
   * Keys are the record number returned by this RecordReader.
   * To reduce the creation of new objects, existing storage is reused for both
   * keys and values (objects are updated in-place).
   * TODO: Should keys just be NullWritables?
   */
  public static class RecordServiceRecordReader extends
      RecordReaderBase<LongWritable, RecordServiceRecord> {
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

    /**
     * Advances to the next record. Return false if there are no more records.
     */
    public boolean nextRecord() throws IOException {
      if (!isInitialized_) {
        throw new RuntimeException("Record Reader not initialized !!");
      }
      try {
        if (!reader_.records().hasNext()) return false;
      } catch (TRecordServiceException e) {
        // TODO: is this the most proper way to deal with this in MR?
        throw new IOException("Could not fetch record.", e);
      }
      currentRSRecord_ = reader_.records().next();
      return true;
    }

    public Schema schema() { return reader_.schema(); }
    public Record currentRecord() { return currentRSRecord_; }
    public boolean isInitialized() { return isInitialized_; }

    @VisibleForTesting
    void initialize(RecordServiceInputSplit split, Configuration config)
        throws IOException {
      try {
        reader_ = new RecordReaderCore(config, split.getTaskInfo());
      } catch (Exception e) {
        throw new IOException("Failed to execute task.", e);
      }
      record_ = new RecordServiceRecord(schema());
      isInitialized_ = true;
    }
  }
}
