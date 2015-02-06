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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.RecordReader;

import com.cloudera.recordservice.mapreduce.RecordServiceRecord;
import com.google.common.base.Preconditions;

public class RecordServiceRecordReader implements
    RecordReader<WritableComparable<?>, RecordServiceRecord> {

  private final com.cloudera.recordservice.mapreduce.RecordServiceRecordReader reader_;
  private RecordServiceRecord currentRecord_;
  private LongWritable currentKey_;

  public RecordServiceRecordReader(
      com.cloudera.recordservice.mapreduce.RecordServiceRecordReader reader) {
    Preconditions.checkNotNull(reader);
    // The reader has to be initialized at this point !!
    this.reader_ = reader;
  }

  @Override
  public boolean next(WritableComparable<?> key, RecordServiceRecord value)
      throws IOException {
    try {
      boolean hasNext = reader_.nextKeyValue();
      if (hasNext) {
        ((LongWritable)key).set(((LongWritable) reader_.getCurrentKey()).get());
        value.set(this.reader_.getCurrentValue());
      }
      return hasNext;
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public WritableComparable<?> createKey() {
    return currentKey_ == null ? new LongWritable() : currentKey_;
  }

  @Override
  public RecordServiceRecord createValue() {
    return currentRecord_ == null ? new RecordServiceRecord() : currentRecord_;
  }

  @Override
  public long getPos() throws IOException {
    try {
      return (long)this.reader_.getProgress();
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void close() throws IOException {
    this.reader_.close();
  }

  @Override
  public float getProgress() throws IOException {
    try {
      return this.reader_.getProgress();
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

}
