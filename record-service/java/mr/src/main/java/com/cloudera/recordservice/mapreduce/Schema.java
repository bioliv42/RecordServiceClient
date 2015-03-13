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
import java.util.List;

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
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;

import com.cloudera.recordservice.thrift.TColumnDesc;
import com.cloudera.recordservice.thrift.TSchema;
import com.cloudera.recordservice.thrift.TTypeId;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * The Schema class provides metadata for the Record. It contains
 * the information of all columns for the record as well as the number
 * of columns.
 *
 * TODO: this should move into the client package.
 */
public class Schema implements Writable {
  // The ColumnType enum is basically used to wrap the
  // Thrift classes as well as maintain a mapping to the
  // associated Writable type
  public static enum ColumnType {
    BOOLEAN,
    TINYINT,
    SMALLINT,
    INT,
    BIGINT,
    FLOAT,
    DOUBLE,
    STRING,
    BINARY,
    VARCHAR,
    CHAR,
    TIMESTAMP,
    DECIMAL,
    ;

    /**
     * Returns the corresponding Writable object for this column type.
     */
    public Writable getWritableInstance() {
      switch (this) {
        case BOOLEAN: return new BooleanWritable();
        case TINYINT: return new ByteWritable();
        case SMALLINT: return new ShortWritable();
        case INT: return new IntWritable();
        case BIGINT: return new LongWritable();
        case FLOAT: return new FloatWritable();
        case DOUBLE: return new DoubleWritable();
        case VARCHAR:
        case CHAR:
        case STRING: return new Text();
        case BINARY: return new BytesWritable();
        case TIMESTAMP: return new TimestampNanosWritable();
        // TODO : need to handle this properly
        case DECIMAL: return new BytesWritable();
        default: throw new UnsupportedOperationException(
            "Unexpected type: " + toString());
      }
    }

    public static ColumnType fromThrift(TTypeId typeId) {
      switch (typeId) {
        case BIGINT: return ColumnType.BIGINT;
        case BOOLEAN: return ColumnType.BOOLEAN;
        case DECIMAL: return ColumnType.DECIMAL;
        case DOUBLE: return ColumnType.DOUBLE;
        case FLOAT: return ColumnType.FLOAT;
        case INT: return ColumnType.INT;
        case SMALLINT: return ColumnType.SMALLINT;
        case STRING: return ColumnType.STRING;
        case VARCHAR: return ColumnType.VARCHAR;
        case CHAR: return ColumnType.CHAR;
        case TIMESTAMP_NANOS: return ColumnType.TIMESTAMP;
        case TINYINT: return ColumnType.TINYINT;
        default: throw new UnsupportedOperationException("Unsupported type: " +
            typeId);
      }
    }
  }

  public static class ColumnInfo {
    private final TColumnDesc columnDesc_;
    private final ColumnType type_;

    ColumnInfo(TColumnDesc columnDesc) {
      Preconditions.checkNotNull(columnDesc);
      columnDesc_ = columnDesc;
      type_ = ColumnType.fromThrift(columnDesc_.getType().getType_id());
    }

    public ColumnType getType() { return type_; }

    public int getPrecision() {
      if (columnDesc_.getType().getType_id() != TTypeId.DECIMAL) {
        throw new UnsupportedOperationException(
            "Type does not have a precision !!");
      }
      if (!columnDesc_.getType().isSetPrecision()) {
        throw new UnsupportedOperationException(
            "Service did not set precision !!");
      }
      return columnDesc_.getType().getPrecision();
    }

    public int getScale() {
      if (columnDesc_.getType().getType_id() != TTypeId.DECIMAL) {
        throw new UnsupportedOperationException(
            "Type does not have a scale !!");
      }
      if (!columnDesc_.getType().isSetScale()) {
        throw new UnsupportedOperationException(
            "Service did not set scale !!");
      }
      return columnDesc_.getType().getScale();
    }

    public int getLength() {
      if (columnDesc_.getType().getType_id() != TTypeId.VARCHAR &&
          columnDesc_.getType().getType_id() != TTypeId.CHAR) {
        throw new UnsupportedOperationException(
            "Type does not have a length !!");
      }
      if (!columnDesc_.getType().isSetLen()) {
        throw new UnsupportedOperationException(
            "Service did not set length !!");
      }
      return columnDesc_.getType().getLen();
    }

    public String getColumnName() { return columnDesc_.getName(); }
  }

  private TSchema tSchema_;

  // List of columns, in order of their index in the schema.
  private List<ColumnInfo> columnInfos_;

  public Schema() {
    columnInfos_ = Lists.newArrayList();
  }

  public Schema(TSchema tSchema) {
    initialize(tSchema);
  }

  private void initialize(TSchema schema) {
    tSchema_ = schema;
    columnInfos_ = Lists.newArrayListWithExpectedSize(schema.getCols().size());
    for (TColumnDesc colDesc: schema.getCols()) {
      columnInfos_.add(new ColumnInfo(colDesc));
    }
  }

  /**
   * Given a column name, returns the column index in the schema using a case-sensitive
   * check on the column name.
   * Returns -1 if the given column name is not found.
   */
  public int getColIdxFromColName(String colName) {
    for (int colIdx = 0; colIdx < columnInfos_.size(); ++colIdx) {
      if (columnInfos_.get(colIdx).getColumnName().equals(colName)) {
        return colIdx;
      }
    }
    return -1;
  }

  public int getNumColumns() { return tSchema_.getColsSize(); }
  public ColumnInfo getColumnInfo(int columnIdx) { return columnInfos_.get(columnIdx); }

  @Override
  public void readFields(DataInput in) throws IOException {
    TSchema tSchema = new TSchema();
    int numBytes = in.readInt();
    byte[] schemaBytes = new byte[numBytes];
    in.readFully(schemaBytes);

    try {
      // TODO: Do we need to create a new instance each time?
      TDeserializer deSer = new TDeserializer(new TCompactProtocol.Factory());
      deSer.deserialize(tSchema, schemaBytes);
      Preconditions.checkNotNull(tSchema);
    } catch (TException e) {
      new IOException(e);
    }
    initialize(tSchema);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    try {
      TSerializer ser = new TSerializer(new TCompactProtocol.Factory());
      byte[] schemaBytes = ser.serialize(tSchema_);
      out.writeInt(schemaBytes.length);
      out.write(schemaBytes);
    } catch (TException e) {
      throw new IOException(e);
    }
  }
}
