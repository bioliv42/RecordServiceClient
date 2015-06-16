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

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.recordservice.mr.RecordReaderCore;
import com.cloudera.recordservice.thrift.TRecordServiceException;

public abstract class RecordServiceInputFormatBase<K, V> implements InputFormat<K, V> {

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    com.cloudera.recordservice.mapreduce.RecordServiceInputFormatBase.SplitsInfo splits =
        com.cloudera.recordservice.mapreduce.RecordServiceInputFormatBase.getSplits(job);
    return convertSplits(splits.splits);
  }

  /**
   * Converts splits from mapreduce to mapred splits.
   */
  public static InputSplit[] convertSplits(
      List<org.apache.hadoop.mapreduce.InputSplit> splits) {
    InputSplit[] retSplits = new InputSplit[splits.size()];
    for (int i = 0; i < splits.size(); ++i) {
      retSplits[i] = new RecordServiceInputSplit(
          (com.cloudera.recordservice.mapreduce.RecordServiceInputSplit) splits.get(i));
    }
    return retSplits;
  }

  /**
   * Base class of RecordService based record readers.
   */
  protected abstract static class RecordReaderBase<K,V> implements RecordReader<K, V> {
    private static final Logger LOG =
        LoggerFactory.getLogger(RecordReaderBase.class);

    protected RecordReaderCore reader_;
    protected final Reporter reporter_;

    protected RecordReaderBase(RecordServiceInputSplit split, JobConf config,
        Reporter reporter) throws IOException {
      try {
        reader_ = new RecordReaderCore(config, split.getBackingSplit().getTaskInfo());
      } catch (Exception e) {
        throw new IOException("Failed to execute task.", e);
      }
      reporter_ = reporter;
    }

    @Override
    public long getPos() throws IOException {
      return 0;
    }

    @Override
    public void close() throws IOException {
      if (reader_ != null) {
        assert(reporter_ != null);
        try {
          com.cloudera.recordservice.mapreduce.RecordServiceInputFormatBase.setCounters(
              reporter_, reader_.records().getStatus().stats);
        } catch (TRecordServiceException e) {
          LOG.debug("Could not populate counters: " + e);
        }
        reader_.close();
        reader_ = null;
      }
    }

    @Override
    public float getProgress() throws IOException {
      if (reader_ == null) return 1;
      return reader_.records().progress();
    }
  }
}
