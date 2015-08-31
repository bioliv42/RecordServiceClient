// Confidential Cloudera Information: Covered by NDA.
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

package com.cloudera.recordservice.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import com.cloudera.recordservice.core.NetworkAddress;
import com.cloudera.recordservice.core.Task;

/**
 * Wrapper around core.Task, implementing the Wrtiable interface.
 */
public class TaskInfo implements Writable {

  private Task task_;

  public TaskInfo() {}
  public TaskInfo(Task task) {
    task_ = task;
  }

  // TODO : Some representation of the size of output
  public long getLength() {
    return 100;
  }

  public NetworkAddress[] getLocations() {
    NetworkAddress[] hosts = new NetworkAddress[task_.localHosts.size()];
    for (int i = 0; i < task_.localHosts.size(); ++i) {
      hosts[i] = task_.localHosts.get(i);
    }
    return hosts;
  }

  public Task getTask() { return task_; }

  @Override
  public void write(DataOutput out) throws IOException {
    task_.serialize(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    task_ = Task.deserialize(in);
  }
}
