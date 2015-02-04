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

public class RecordServiceRecordReader implements
    RecordReader<WritableComparable<?>, RecordServiceRecord> {

  private com.cloudera.recordservice.mapreduce.RecordServiceRecordReader reader_;

  public RecordServiceRecordReader(
      com.cloudera.recordservice.mapreduce.RecordServiceRecordReader reader) {
    // The reader has to be initialized at this point !!
    this.reader_ = reader;
  }

  @Override
  public boolean next(WritableComparable<?> key, RecordServiceRecord value)
      throws IOException {
    try {
      boolean hasNext = this.reader_.nextKeyValue();
      if (hasNext) {
        ((LongWritable)key).set(
            ((LongWritable)this.reader_.getCurrentKey()).get());
        value.cloneFrom(this.reader_.getCurrentValue());
      }
      return hasNext;
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public WritableComparable<?> createKey() {
    return new LongWritable();
  }

  @Override
  public RecordServiceRecord createValue() {
    return new RecordServiceRecord();
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
