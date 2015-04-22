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

public abstract class RecordServiceInputFormatBase<K, V> implements InputFormat<K, V> {

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    List<org.apache.hadoop.mapreduce.InputSplit> splits =
      com.cloudera.recordservice.mapreduce.RecordServiceInputFormatBase.getSplits(job);
    InputSplit[] retSplits = new InputSplit[splits.size()];
    int i = 0;
    for (org.apache.hadoop.mapreduce.InputSplit split: splits) {
      retSplits[i] = new RecordServiceInputSplit(
          (com.cloudera.recordservice.mapreduce.RecordServiceInputSplit) split);
      i++;
    }
    return retSplits;
  }
}
