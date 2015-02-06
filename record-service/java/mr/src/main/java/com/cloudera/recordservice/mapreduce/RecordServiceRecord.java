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
import java.util.Map;

import org.apache.hadoop.io.Writable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

public class RecordServiceRecord implements Writable {
  private Writable[] columnValues_;
  private Schema schema_ = new Schema();

  // Map of column name to column value index in columnValues_.
  // TODO: Handle column names should be case-insensitive, we need to handle
  // that efficiently.
  private Map<String, Integer> colNameToIdx_ = Maps.newHashMap();

  public RecordServiceRecord() {}

  public RecordServiceRecord(Schema schema, Writable[] columnValues) {
    Preconditions.checkNotNull(schema);
    Preconditions.checkNotNull(columnValues);
    Preconditions.checkState(schema.getNumColumns() == columnValues.length);
    columnValues_ = columnValues;
    schema_ = schema;
    colNameToIdx_ = Maps.newHashMapWithExpectedSize(schema.getNumColumns());
    for (int i = 0; i < schema_.getNumColumns(); ++i) {
      colNameToIdx_.put(schema_.getColumnInfo(i).getColumnName(), i);
    }
  }

  /**
   * Initializes the RecordServiceRecord with new column values. The values must
   * match the existing schema (no validation checks are done).
   */
  public void initialize(Writable[] columnValues) {
    columnValues_ = columnValues;
  }

  public Writable getColumnValue(int colIndex) {
    return columnValues_[colIndex];
  }

  /**
   * Returns the column value for the given column name, or null of the column name
   * does not exist in this record. This is on the hot path when running with Hive -
   * it is called for every record returned.
   */
  public Writable getColumnValue(String columnName) {
    Integer index = colNameToIdx_.get(columnName);
    if (index == null) return null;
    return getColumnValue(index);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    schema_.write(out);
    for(int i = 0; i < columnValues_.length; ++i) {
      columnValues_[i].write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    schema_ = new Schema();
    schema_.readFields(in);
    columnValues_ = new Writable[schema_.getNumColumns()];
    for (int i = 0; i < columnValues_.length; i++) {
      try {
        columnValues_[i] = schema_.getColumnInfo(i).getType().getWritableInstance();
        columnValues_[i].readFields(in);
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }

  /**
   * Sets this RecordServiceRecord's internal state to be equal to another
   * RecordServiceRecord (shallow copy). This is on the hot path - it is called for every
   * value in the mapreduce -> mapred conversion.
   */
  public void set(RecordServiceRecord other) {
    Preconditions.checkNotNull(other);
    schema_ = other.schema_;
    columnValues_ = other.columnValues_;
    colNameToIdx_ = other.colNameToIdx_;
  }
}
