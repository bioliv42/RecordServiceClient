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
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptContextImpl;
import org.apache.hadoop.mapred.TaskAttemptID;

import com.cloudera.recordservice.mapreduce.RecordServiceInputFormatBase;
import com.cloudera.recordservice.mr.RecordServiceRecord;
import com.google.common.base.Preconditions;

/**
 * Implemention of RecordServiceInputFormat.
 */
public class RecordServiceInputFormat implements
    InputFormat<WritableComparable<?>, RecordServiceRecord>{

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    List<org.apache.hadoop.mapreduce.InputSplit> splits =
        RecordServiceInputFormatBase.getSplits(job);
    InputSplit[] retSplits = new InputSplit[splits.size()];
    int i = 0;
    for (org.apache.hadoop.mapreduce.InputSplit split: splits) {
      retSplits[i] = new RecordServiceInputSplit(
          (com.cloudera.recordservice.mapreduce.RecordServiceInputSplit) split);
      i++;
    }
    return retSplits;
  }

  @Override
  public RecordReader<WritableComparable<?>, RecordServiceRecord>
      getRecordReader(InputSplit split, JobConf job, Reporter reporter)
          throws IOException {
    com.cloudera.recordservice.mapreduce.RecordServiceInputFormat.RecordServiceRecordReader reader =
        new com.cloudera.recordservice.mapreduce.RecordServiceInputFormat.RecordServiceRecordReader();
    RecordReader<WritableComparable<?>, RecordServiceRecord> ret = null;
    try {
      reader.initialize(((RecordServiceInputSplit)split).getBackingSplit(),
          new TaskAttemptContextImpl(job, new TaskAttemptID()));
      ret = new RecordServiceRecordReader(reader);
      return ret;
    } catch (InterruptedException e) {
      throw new IOException(e);
    } finally {
      if (ret == null) reader.close();
    }
  }

  public static class RecordServiceRecordReader implements
      RecordReader<WritableComparable<?>, RecordServiceRecord> {

    private final com.cloudera.recordservice.mapreduce.RecordServiceInputFormat.RecordServiceRecordReader reader_;

    public RecordServiceRecordReader(
        com.cloudera.recordservice.mapreduce.RecordServiceInputFormat.RecordServiceRecordReader reader) {
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
      value.reset(reader_.currentRecord());
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
}
