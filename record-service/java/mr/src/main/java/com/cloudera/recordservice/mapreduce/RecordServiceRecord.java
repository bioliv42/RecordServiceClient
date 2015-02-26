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

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.cloudera.recordservice.client.ByteArray;
import com.cloudera.recordservice.client.Rows.Row;
import com.cloudera.recordservice.mapreduce.Schema.ColumnInfo;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

public class RecordServiceRecord implements Writable {
  // Array of Writable column values.
  private Writable[] columnVals_;

  // Schema associated with columnVals_.
  private Schema schema_;

  // Map of column name to column value index in columnValues_.
  // TODO: Handle column names should be case-insensitive, we need to handle
  // that efficiently.
  private Map<String, Integer> colNameToIdx_ = Maps.newHashMap();

  public RecordServiceRecord(Schema schema) {
    schema_ = schema;
    columnVals_ = new Writable[schema_.getNumColumns()];
    colNameToIdx_ = Maps.newHashMapWithExpectedSize(schema_.getNumColumns());
    for (int i = 0; i < schema_.getNumColumns(); ++i) {
      ColumnInfo columnInfo = schema_.getColumnInfo(i);
      colNameToIdx_.put(columnInfo.getColumnName(), i);
      columnVals_[i] = columnInfo.getType().getWritableInstance();
    }
  }

  /**
   * Resets the data in this RecordServiceRecord by translating the column data from the
   * given Row to the internal array of Writables (columnVals_).
   * Reads the column data from the given Row into this RecordServiceRecord. The
   * schema are expected to match, minimal error checks are performed.
   * This is a performance critical method.
   */
  public void reset(Row row) {
    if (row.getSchema().getColsSize() != schema_.getNumColumns()) {
      throw new IllegalArgumentException(String.format("Schema for new row does not " +
        "match existing schema: %d (new) != %d (existing)",
        row.getSchema().getColsSize(), schema_.getNumColumns()));
    }

    for (int i = 0; i < schema_.getNumColumns(); ++i) {
      // TODO: Handle nulls.
      ColumnInfo cInfo = schema_.getColumnInfo(i);
      Preconditions.checkNotNull(cInfo);
      switch (cInfo.getType()) {
        case BIGINT:
          ((LongWritable) columnVals_[i]).set(row.getLong(i));
          break;
        case BOOLEAN:
          ((BooleanWritable) columnVals_[i]).set(row.getBoolean(i));
          break;
        case DOUBLE:
          ((DoubleWritable) columnVals_[i]).set(row.getDouble(i));
          break;
        case INT:
          ((IntWritable) columnVals_[i]).set(row.getInt(i));
          break;
        case STRING:
          ByteArray s = row.getByteArray(i);
          ((Text) columnVals_[i]).set(s.byteBuffer().array(), s.offset(), s.len());
          break;
        case BINARY:
          ByteArray b = row.getByteArray(i);
          ((BytesWritable) columnVals_[i]).set(
              b.byteBuffer().array(), b.offset(), b.len());
          break;
        case FLOAT:
          ((FloatWritable) columnVals_[i]).set(row.getFloat(i));
          break;
        case SMALLINT:
          ((ShortWritable) columnVals_[i]).set(row.getShort(i));
          break;
        case TINYINT:
          ((ByteWritable) columnVals_[i]).set(row.getByte(i));
          break;
        default:
          Preconditions.checkState(false);
      }
    }
  }

  public Writable getColumnValue(int colIdx) {
    return columnVals_[colIdx];
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
    for(int i = 0; i < columnVals_.length; ++i) {
      columnVals_[i].write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    schema_ = new Schema();
    schema_.readFields(in);
    columnVals_ = new Writable[schema_.getNumColumns()];
    for (int i = 0; i < columnVals_.length; i++) {
      try {
        columnVals_[i] = schema_.getColumnInfo(i).getType().getWritableInstance();
        columnVals_[i].readFields(in);
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }

  public Schema getSchema() { return schema_; }
}
