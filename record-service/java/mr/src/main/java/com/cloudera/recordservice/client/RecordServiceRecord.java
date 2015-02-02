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

package com.cloudera.recordservice.client;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import com.cloudera.recordservice.client.Schema.ColumnInfo;
import com.cloudera.recordservice.client.Schema.ColumnType;
import com.google.common.base.Preconditions;

public class RecordServiceRecord implements Writable {

  private Writable[] columnValues_ = new Writable[0];
  private Schema schema_ = new Schema();
  
  public RecordServiceRecord() {}

  public RecordServiceRecord(Schema schema, Writable[] columnValues) {
    Preconditions.checkNotNull(schema);
    Preconditions.checkNotNull(columnValues);
    this.columnValues_ = columnValues;
    this.schema_ = schema;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    schema_.write(out);
    out.writeInt(columnValues_.length);
    for(int i = 0; i < columnValues_.length; i++) {
      ColumnInfo columnInfo = schema_.getColumnInfo(i);
      out.writeShort(columnInfo.getType().ordinal());
      columnValues_[i].write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    schema_ = new Schema();
    schema_.readFields(in);
    int numCols = in.readInt();
    columnValues_ = new Writable[numCols];
    for (int i = 0; i < numCols; i++) {
      short t = in.readShort();
      try {
        Writable writable = ColumnType.values()[t].getWritableInstance();
        writable.readFields(in);
        columnValues_[i] = writable;
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }

}
