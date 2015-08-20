// Confidential Cloudera Information: Covered by NDA.
// Copyright 2014 Cloudera Inc.
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

package com.cloudera.recordservice.core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.cloudera.recordservice.thrift.TTask;

/**
 * POJO wrapper for TTask
 */
public class Task implements Serializable {
  private static final long serialVersionUID = -9174012309832563502L;

  public final List<NetworkAddress> localHosts;
  public final byte[] task;
  public final UniqueId taskId;
  public final boolean resultsOrdered;

  Task(TTask t) {
    localHosts = NetworkAddress.fromThrift(t.local_hosts);
    task = t.task.array();
    taskId = new UniqueId(t.task_id);
    resultsOrdered = t.results_ordered;
  }

  /**
   * Serializes this task to 'out'
   */
  public void serialize(DataOutput out) throws IOException {
    out.writeInt(localHosts.size());
    for (NetworkAddress n: localHosts) {
      out.writeInt(n.hostname.length());
      out.writeBytes(n.hostname);
      out.writeInt(n.port);
    }
    out.writeInt(task.length);
    out.write(task);
    out.writeLong(taskId.hi);
    out.writeLong(taskId.lo);
    out.writeBoolean(resultsOrdered);
  }

  /**
   * Deserializes Task from 'in'
   */
  public static Task deserialize(DataInput in) throws IOException {
    int numLocalHosts = in.readInt();
    List<NetworkAddress> localHosts = new ArrayList<NetworkAddress>();
    for (int i = 0; i < numLocalHosts; ++i) {
      int hostnameLen = in.readInt();
      byte[] hostnameBuffer = new byte[hostnameLen];
      in.readFully(hostnameBuffer);
      int port = in.readInt();
      localHosts.add(new NetworkAddress(new String(hostnameBuffer), port));
    }
    int taskLen = in.readInt();
    byte[] taskBuffer = new byte[taskLen];
    in.readFully(taskBuffer);
    UniqueId id = new UniqueId(in.readLong(), in.readLong());
    boolean resultsOrdered = in.readBoolean();
    return new Task(localHosts, taskBuffer, id, resultsOrdered);
  }

  Task(List<NetworkAddress> localHosts, byte[] task,
      UniqueId id, boolean resultsOrdered) {
    this.localHosts = localHosts;
    this.task = task;
    this.taskId = id;
    this.resultsOrdered = resultsOrdered;
  }

  /**
   * Returns a list of Tasks from the thrift version.
   */
  static List<Task> fromThrift(List<TTask> list) {
    List<Task> result = new ArrayList<Task>();
    for (TTask l: list) {
      result.add(new Task(l));
    }
    return result;
  }

}
