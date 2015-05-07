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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.recordservice.mapreduce.RecordServiceInputFormatBase;
import com.cloudera.recordservice.mr.RecordReaderCore;
import com.cloudera.recordservice.mr.RecordServiceRecord;
import com.cloudera.recordservice.thrift.TRecordServiceException;

/**
 * Implementation of RecordServiceInputFormat.
 */
public class RecordServiceInputFormat implements
    InputFormat<WritableComparable<?>, RecordServiceRecord>{

  private static final Logger LOG =
      LoggerFactory.getLogger(RecordServiceInputFormat.class);

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
    return new RecordServiceRecordReader(split, job, reporter);
  }

  public static class RecordServiceRecordReader implements
      RecordReader<WritableComparable<?>, RecordServiceRecord> {
    private Reporter reporter_;
    private RecordReaderCore reader_;

    public RecordServiceRecordReader(InputSplit split,
        JobConf job, Reporter reporter) throws IOException {
      try {
        reader_ = new RecordReaderCore(job,
            ((RecordServiceInputSplit)split).getBackingSplit().getTaskInfo());
      } catch (Exception e) {
        throw new IOException("Failed to execute task.", e);
      }
      reporter_ = reporter;
    }

    /**
     * Advances to the next record, populating key,value with the results.
     * This is the hot path.
     */
    @Override
    public boolean next(WritableComparable<?> key, RecordServiceRecord value)
        throws IOException {
      try {
        if (!reader_.records().hasNext()) return false;
      } catch (TRecordServiceException e) {
        // TODO: is this the most proper way to deal with this in MR?
        throw new IOException("Could not fetch record.", e);
      }
      value.reset(reader_.records().next());
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
      if (reader_ != null) {
        try {
          RecordServiceInputFormatBase.setCounters(
              reporter_, reader_.records().getStatus().stats);
        } catch (TRecordServiceException e) {
          LOG.debug("Could not populate counters: " + e);
        }
        reader_.close();
        reader_ = null;
      }
    }

    @Override
    public float getProgress() {
      return reader_.records().progress();
    }
  }
}
