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
import java.nio.ByteBuffer;

import org.apache.hadoop.io.Writable;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;

import com.cloudera.recordservice.thrift.TTask;

public class TaskInfo implements Writable {

  private TTask task_;

  public TaskInfo() {}

  public TaskInfo(TTask task) {
    this.task_ = task;
  }

  // TODO : Some representation of the size of output
  public long getLength() {
    return 100;
  }

  public String[] getLocations() {
    return task_.getHosts().toArray(new String[0]);
  }

  public byte[] getTask() {
    return task_.getTask();
  }

  public ByteBuffer getTaskAsByteBuffer() {
    return task_.bufferForTask();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    TSerializer ser = new TSerializer(new TCompactProtocol.Factory());
    try {
      byte[] taskBytes = ser.serialize(task_);
      out.writeInt(taskBytes.length);
      out.write(taskBytes);
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    TTask task = new TTask();
    int numBytes = in.readInt();
    byte[] taskBytes = new byte[numBytes];
    in.readFully(taskBytes);
    TDeserializer deSer = new TDeserializer(new TCompactProtocol.Factory());
    try {
      deSer.deserialize(task, taskBytes);
      this.task_ = task;
    } catch (TException e) {
      new IOException(e);
    }
  }

}
