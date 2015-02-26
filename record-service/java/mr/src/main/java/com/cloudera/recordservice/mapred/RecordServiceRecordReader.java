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

package com.cloudera.recordservice.mapred;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.RecordReader;

import com.cloudera.recordservice.mapreduce.RecordServiceRecord;
import com.google.common.base.Preconditions;

public class RecordServiceRecordReader implements
    RecordReader<WritableComparable<?>, RecordServiceRecord> {

  private final com.cloudera.recordservice.mapreduce.RecordServiceRecordReader reader_;

  public RecordServiceRecordReader(
      com.cloudera.recordservice.mapreduce.RecordServiceRecordReader reader) {
    Preconditions.checkNotNull(reader);
    Preconditions.checkState(reader.isInitialized());
    this.reader_ = reader;
  }

  /**
   * Advances to the next record, populating key,value with the results.
   * This is the hot path.
   */
  @Override
  public boolean next(WritableComparable<?> key, RecordServiceRecord value)
      throws IOException {
    if (!reader_.nextRecord()) return false;
    value.reset(reader_.currentRow());
    return true;
  }

  @Override
  public WritableComparable<?> createKey() {
    // TODO: is this legit?
    return NullWritable.get();
  }

  @Override
  public RecordServiceRecord createValue() {
    return new RecordServiceRecord(reader_.schema());
  }

  @Override
  public long getPos() throws IOException {
    // TODO: what does this mean for us?
    return 0;
  }

  @Override
  public void close() throws IOException {
    reader_.close();
  }

  @Override
  public float getProgress() {
    return reader_.getProgress();
  }
}
