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
import com.cloudera.recordservice.client.Records.Record;
import com.cloudera.recordservice.mapreduce.Schema.ColumnInfo;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

public class RecordServiceRecord implements Writable {
  // Array of Writable objects. This is created once and reused.
  private Writable[] columnValObjects_;

  // The values for the current record. If column[i] is NULL,
  // columnVals_[i] is null, otherwise it is columnValObjects_[i].
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
    columnValObjects_ = new Writable[schema_.getNumColumns()];
    colNameToIdx_ = Maps.newHashMapWithExpectedSize(schema_.getNumColumns());
    for (int i = 0; i < schema_.getNumColumns(); ++i) {
      ColumnInfo columnInfo = schema_.getColumnInfo(i);
      colNameToIdx_.put(columnInfo.getColumnName(), i);
      columnValObjects_[i] = columnInfo.getType().getWritableInstance();
    }
  }

  /**
   * Resets the data in this RecordServiceRecord by translating the column data from the
   * given Row to the internal array of Writables (columnVals_).
   * Reads the column data from the given Row into this RecordServiceRecord. The
   * schema are expected to match, minimal error checks are performed.
   * This is a performance critical method.
   */
  public void reset(Record record) {
    if (record.getSchema().getColsSize() != schema_.getNumColumns()) {
      throw new IllegalArgumentException(String.format("Schema for new record does " +
        "not match existing schema: %d (new) != %d (existing)",
        record.getSchema().getColsSize(), schema_.getNumColumns()));
    }

    for (int i = 0; i < schema_.getNumColumns(); ++i) {
      if (record.isNull(i)) {
        columnVals_[i] = null;
        continue;
      }
      columnVals_[i] = columnValObjects_[i];
      ColumnInfo cInfo = schema_.getColumnInfo(i);
      Preconditions.checkNotNull(cInfo);
      switch (cInfo.getType()) {
        case BIGINT:
          ((LongWritable) columnValObjects_[i]).set(record.getLong(i));
          break;
        case BOOLEAN:
          ((BooleanWritable) columnValObjects_[i]).set(record.getBoolean(i));
          break;
        case DOUBLE:
          ((DoubleWritable) columnValObjects_[i]).set(record.getDouble(i));
          break;
        case INT:
          ((IntWritable) columnValObjects_[i]).set(record.getInt(i));
          break;
        case STRING:
        case VARCHAR:
        case CHAR:
          ByteArray s = record.getByteArray(i);
          ((Text) columnValObjects_[i]).set(s.byteBuffer().array(), s.offset(), s.len());
          break;
        case BINARY:
          ByteArray b = record.getByteArray(i);
          ((BytesWritable) columnValObjects_[i]).set(
              b.byteBuffer().array(), b.offset(), b.len());
          break;
        case FLOAT:
          ((FloatWritable) columnValObjects_[i]).set(record.getFloat(i));
          break;
        case SMALLINT:
          ((ShortWritable) columnValObjects_[i]).set(record.getShort(i));
          break;
        case TINYINT:
          ((ByteWritable) columnValObjects_[i]).set(record.getByte(i));
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
  // TODO: what is the contract here? Handle NULLs
  public void write(DataOutput out) throws IOException {
    schema_.write(out);
    for(int i = 0; i < columnValObjects_.length; ++i) {
      columnValObjects_[i].write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    schema_ = new Schema();
    schema_.readFields(in);
    columnValObjects_ = new Writable[schema_.getNumColumns()];
    for (int i = 0; i < columnValObjects_.length; i++) {
      try {
        columnValObjects_[i] = schema_.getColumnInfo(i).getType().getWritableInstance();
        columnValObjects_[i].readFields(in);
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }

  public Schema getSchema() { return schema_; }
}
