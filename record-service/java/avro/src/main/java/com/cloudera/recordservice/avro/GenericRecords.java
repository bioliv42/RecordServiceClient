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

package com.cloudera.recordservice.avro;

import java.io.IOException;

import org.apache.avro.generic.GenericData.Record;

import com.cloudera.recordservice.client.Records;
import com.google.common.base.Preconditions;

/**
 * This class takes a Records object and provides an iterator interface to
 * return generic records.
 *
 * TODO: reuse records?
 * TODO: NULLs
 * TODO: map STRING to BYTES?
 */
public class GenericRecords {
  private Records records_;
  private org.apache.avro.Schema avroSchema_;
  private com.cloudera.recordservice.thrift.TSchema schema_;

  public GenericRecords(Records records) {
    records_ = records  ;
    schema_ = records_.getSchema();
    avroSchema_ = SchemaUtils.convertSchema(schema_);
  }

  /**
   * Returns the generated avro schema.
   */
  public org.apache.avro.Schema getSchema() { return avroSchema_; }

  /**
   * Returns true if there are more records, false otherwise.
   */
  public boolean hasNext() throws IOException {
    return records_.hasNext();
  }

  /**
   * Returns and advances to the next record. Throws exception if
   * there are no more records.
   */
  public Record next() throws IOException {
    Records.Record rsRecord = records_.next();
    Record record = new Record(avroSchema_);
    for (int i = 0; i < schema_.getColsSize(); ++i) {
      switch(schema_.getCols().get(i).type.type_id) {
      case BOOLEAN: record.put(i, rsRecord.getBoolean(i)); break;
      case TINYINT: record.put(i, (int)rsRecord.getByte(i)); break;
      case SMALLINT: record.put(i, (int)rsRecord.getShort(i)); break;
      case INT: record.put(i, rsRecord.getInt(i)); break;
      case BIGINT: record.put(i, rsRecord.getLong(i)); break;
      case FLOAT: record.put(i, rsRecord.getFloat(i)); break;
      case DOUBLE: record.put(i, rsRecord.getDouble(i)); break;
      case STRING:
      case VARCHAR:
      case CHAR:
        record.put(i, rsRecord.getByteArray(i).toString()); break;
      default:
        Preconditions.checkState(false,
            "Unsupported type: " + schema_.getCols().get(i).type);
      }
    }
    return record;
  }

  /**
   * Closes the underlying task. Must be called for every GenericRecords object
   * created. Invalid to call other APIs after this. Idempotent.
   */
  public void close() {
    records_.close();
  }
}
