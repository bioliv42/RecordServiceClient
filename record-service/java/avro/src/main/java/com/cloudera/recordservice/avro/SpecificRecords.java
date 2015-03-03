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
import java.util.HashMap;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;

import com.cloudera.recordservice.client.Rows;
import com.cloudera.recordservice.thrift.TTypeId;
import com.google.common.base.Preconditions;

/**
 * This class takes a Rows object and provides an iterator interface to
 * return specific records.
 *
 * TODO: reuse records?
 * TODO: NULLs
 * TODO: map STRING to BYTES?
 */
public class SpecificRecords<T extends SpecificRecordBase> {
  private Rows rows_;
  private org.apache.avro.Schema avroSchema_;
  private com.cloudera.recordservice.thrift.TSchema schema_;
  private Class<T> class_;

  // For each field in the RecordService record (by ordinal), the corresponding
  // index in T. If matching by ordinal, this is just the identity. If matching
  // by name, this can be different.
  private int[] rsIndexToRecordIndex_;

  public enum ResolveBy {
    ORDINAL,
    NAME,
  }

  public SpecificRecords(Class<T> cl, Rows rows, ResolveBy resolveBy) {
    class_ = cl;
    rows_ = rows;
    schema_ = rows_.getSchema();
    try {
      // The first field is the static schema field.
      // TODO: should we try harder to verify? This is auto-generated and
      // unlikely to change.
      avroSchema_ = (Schema)cl.getFields()[0].get(null);
    } catch (Exception e) {
      throw new RuntimeException("Invalid Record class.", e);
    }
    resolveSchema(resolveBy);
  }

  /**
   * Returns the generated avro schema.
   */
  public org.apache.avro.Schema getSchema() { return avroSchema_; }

  /**
   * Returns true if there are more records, false otherwise.
   */
  public boolean hasNext() throws IOException {
    return rows_.hasNext();
  }

  /**
   * Returns and advances to the next record. Throws exception if
   * there are no more records.
   */
  public T next() throws IOException {
    T record = null;
    try {
      record = class_.newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Could not create new record instance.", e);
    }

    Rows.Row row = rows_.next();
    for (int i = 0; i < schema_.getColsSize(); ++i) {
      int rsIndex = rsIndexToRecordIndex_[i];
      switch(schema_.getCols().get(i).type.type_id) {
        case BOOLEAN: record.put(rsIndex, row.getBoolean(i)); break;
        case TINYINT: record.put(rsIndex, (int)row.getByte(i)); break;
        case SMALLINT: record.put(rsIndex, (int)row.getShort(i)); break;
        case INT: record.put(rsIndex, row.getInt(i)); break;
        case BIGINT: record.put(rsIndex, row.getLong(i)); break;
        case FLOAT: record.put(rsIndex, row.getFloat(i)); break;
        case DOUBLE: record.put(rsIndex, row.getDouble(i)); break;
        case STRING: record.put(rsIndex, row.getByteArray(i).toString()); break;
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
    rows_.close();
  }

  // Verifies that
  // avroSchema_[recordIndex] is compatible with schema_[recordServiceIndex]
  private void resolveType(int recordIndex, int recordServiceIndex) {
    List<Schema.Field> fields = avroSchema_.getFields();
    // TODO: support avro's schema resolution rules with type promotion.
    TTypeId rsType = schema_.cols.get(recordIndex).type.type_id;
    Schema.Type t = fields.get(recordIndex).schema().getType();

    switch (t) {
      case ARRAY:
      case MAP:
      case RECORD:
      case UNION:
        throw new RuntimeException("Nullable and nested schemas are not supported");

      case ENUM:
      case NULL:
      case BYTES:
      case FIXED:
        // What to do with these?
        throw new RuntimeException("???");

      case BOOLEAN:
        if (rsType == TTypeId.BOOLEAN) return;
        break;
      case INT:
        if (rsType == TTypeId.TINYINT || rsType == TTypeId.SMALLINT ||
            rsType == TTypeId.INT) {
          return;
        }
        break;
      case LONG:
        if (rsType == TTypeId.BIGINT) return;
        break;
      case FLOAT:
        if (rsType == TTypeId.FLOAT) return;
        break;
      case DOUBLE:
        if (rsType == TTypeId.DOUBLE) return;
        break;
      case STRING:
        if (rsType == TTypeId.STRING) return;
        break;
      default:
        throw new RuntimeException("Unsupported type: " + t);
    }

    throw new RuntimeException("Field at position " + recordIndex +
          " have incompatible types. RecordService returned " + rsType +
          ". Record expects " + t);
  }

  // Verifies and resolves the schema of T with the RecordService schema.
  private void resolveSchema(ResolveBy resolveBy) {
    if (avroSchema_.getType() != Schema.Type.RECORD) {
      throw new RuntimeException(
          "Incompatible schema: generic type must be a RECORD.");
    }

    List<Schema.Field> fields = avroSchema_.getFields();
    if (fields.size() != schema_.getColsSize()) {
      // TODO: support avro's  schema evolution rules.
      throw new RuntimeException(
          "Incompatible schema: the number of fields do not match. Record contains " +
          fields.size() + ". RecordService returned " + schema_.getColsSize());
    }
    rsIndexToRecordIndex_ = new int[fields.size()];

    if (resolveBy == ResolveBy.ORDINAL) {
      for (int i = 0; i < fields.size(); ++i) {
        resolveType(i, i);
        rsIndexToRecordIndex_[i] = i;
      }
    } else if (resolveBy == ResolveBy.NAME) {
      HashMap<String, Integer> rsFields = new HashMap<String, Integer>();
      for (int i = 0; i < schema_.getColsSize(); ++i) {
        // TODO: case sensitive?
        rsFields.put(schema_.getCols().get(i).name.toLowerCase(), i);
      }

      for (int i = 0; i < fields.size(); ++i) {
        String fieldName = fields.get(i).name();
        if (!rsFields.containsKey(fieldName)) {
          throw new RuntimeException("Incompatible schema: field in record '" +
              fieldName + "' was not part of RecordService schema");
        }
        int rsFieldIndex = rsFields.get(fieldName);
        resolveType(i, rsFieldIndex);
        rsIndexToRecordIndex_[i] = rsFieldIndex;
      }
    } else {
      throw new RuntimeException("Not implemented");
    }
  }
}
