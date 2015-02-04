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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * The InputSplit implementation that is used in conjunction with the
 * Record Service. It contains the Schema of the record as well as all the
 * information required for the Record Service Worker to execute the task
 */
public class RecordServiceInputSplit extends InputSplit implements Writable {

  private TaskInfo taskInfo_;
  private Schema schema_;

  public RecordServiceInputSplit() {}

  public RecordServiceInputSplit(Schema schema, TaskInfo taskInfo) {
    this.schema_ = schema;
    this.taskInfo_ = taskInfo;
  }

  public Schema getSchema() {
    return schema_;
  }

  public TaskInfo getTaskInfo() {
    return taskInfo_;
  }

  @Override
  public long getLength() throws IOException, InterruptedException {
    return taskInfo_.getLength();
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    return taskInfo_.getLocations();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    schema_.write(out);
    taskInfo_.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.schema_ = new Schema();
    this.schema_.readFields(in);
    this.taskInfo_ = new TaskInfo();
    this.taskInfo_.readFields(in);
    
  }

}
