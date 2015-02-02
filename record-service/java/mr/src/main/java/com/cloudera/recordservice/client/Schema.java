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

import com.cloudera.recordservice.thrift.TSchema;
import com.cloudera.recordservice.thrift.TType;
import com.cloudera.recordservice.thrift.TTypeId;

/**
 * The Schema class provides metadata for the Record. It contains
 * the information of all columns for the record as well as the number
 * of columns
 */
public class Schema implements Writable {

  // The ColumnType enum is basically used to wrap the
  // Thrift classes as well as maintain a mapping to the
  // associated Writable type
  public static enum ColumnType {
    BOOLEAN(BooleanWritable.class),
    TINYINT(ByteWritable.class),
    SMALLINT(ShortWritable.class),
    INT(IntWritable.class),
    BIGINT(LongWritable.class),
    FLOAT(FloatWritable.class),
    DOUBLE(DoubleWritable.class),
    STRING(Text.class),
    BINARY(BytesWritable.class),
    // TODO : is this ok ?
    TIMESTAMP(LongWritable.class);
    // TODO : need to hande this properly
    // DECIMAL(BigDecimal);

    private Class<? extends Writable> wClass;

    private ColumnType(Class<? extends Writable> wClass) {
      this.wClass = wClass;
    }

    public Writable getWritableInstance() throws InstantiationException,
        IllegalAccessException {
      return wClass.newInstance();
    }
  }

  public class ColumnInfo {
    private final TType tType_;
    ColumnInfo(TType type) {
      tType_ = type;
    }
    public ColumnType getType() {
      return ColumnType.valueOf(tType_.getType_id().toString());
    }

    public int getPrecision() {
      if (tType_.isSetPrecision()) {
        return tType_.getPrecision();
      } else {
        if (tType_.getType_id() != TTypeId.DECIMAL) {
          throw new UnsupportedOperationException(
              "Type does not have a precision !!");
        }
      }
      return -1;
    }

    public int getScale() {
      if (tType_.isSetScale()) {
        return tType_.getScale();
      } else {
        if (tType_.getType_id() != TTypeId.DECIMAL) {
          throw new UnsupportedOperationException(
              "Type does not have a scale !!");
        }
      }
      return -1;
    }
  }

  private TSchema tSchema_;

  public Schema() {
  }

  public Schema(TSchema tSchema) {
    this.tSchema_ = tSchema;
  }

  public int getNumColumns() {
    return tSchema_.getColsSize();
  }

  public ColumnInfo getColumnInfo(int columnIndex) {
    return new ColumnInfo(tSchema_.getCols().get(columnIndex));
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    TSchema tSchema = new TSchema();
    int numBytes = in.readInt();
    byte[] schemaBytes = new byte[numBytes];
    in.readFully(schemaBytes);
    TDeserializer deSer = new TDeserializer(new TCompactProtocol.Factory());
    try {
      deSer.deserialize(tSchema, schemaBytes);
      this.tSchema_ = tSchema;
    } catch (TException e) {
      new IOException(e);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    TSerializer ser = new TSerializer(new TCompactProtocol.Factory());
    try {
      byte[] schemaBytes = ser.serialize(tSchema_);
      out.writeInt(schemaBytes.length);
      out.write(schemaBytes);
    } catch (TException e) {
      throw new IOException(e);
    }
  }

}
