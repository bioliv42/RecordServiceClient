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
import org.apache.avro.Schema.Type;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecordBase;

import com.cloudera.recordservice.client.Records;
import com.cloudera.recordservice.thrift.TRecordServiceException;
import com.cloudera.recordservice.thrift.TTypeId;

/**
 * This class takes a Rows object and provides an iterator interface to
 * return specific records.
 *
 * TODO: reuse records?
 * TODO: NULLs
 * TODO: map STRING to BYTES?
 */
public class SpecificRecords<T> {
  private Records records_;
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

  @SuppressWarnings("unchecked")
  public SpecificRecords(Schema readerSchema, Records records, ResolveBy resolveBy) {
    avroSchema_ = readerSchema;
    class_ = new SpecificData().getClass(avroSchema_);
    records_ = records;
    schema_ = records_.getSchema();
    resolveSchema(resolveBy);
  }

  /**
   * Returns the generated avro schema.
   */
  public org.apache.avro.Schema getSchema() { return avroSchema_; }

  /**
   * Returns true if there are more records, false otherwise.
   */
  public boolean hasNext() throws IOException, TRecordServiceException {
    return records_.hasNext();
  }

  /**
   * Returns and advances to the next record. Throws exception if
   * there are no more records.
   */
  @SuppressWarnings("unchecked")
  public T next() throws IOException, TRecordServiceException {
    SpecificRecordBase record = null;
    try {
      record = (SpecificRecordBase)class_.newInstance();
    } catch (ClassCastException e) {
      throw new RuntimeException("Template paramter 'T' must be a SpecificRecord");
    } catch (Exception e) {
      throw new RuntimeException("Could not create new record instance.", e);
    }

    Records.Record rsRecord = records_.next();
    for (int i = 0; i < schema_.getColsSize(); ++i) {
      int rsIndex = rsIndexToRecordIndex_[i];
      if (rsRecord.isNull(i)) {
        record.put(rsIndex, null);
        continue;
      }
      switch(schema_.getCols().get(i).type.type_id) {
        case BOOLEAN: record.put(rsIndex, rsRecord.getBoolean(i)); break;
        case TINYINT: record.put(rsIndex, (int)rsRecord.getByte(i)); break;
        case SMALLINT: record.put(rsIndex, (int)rsRecord.getShort(i)); break;
        case INT: record.put(rsIndex, rsRecord.getInt(i)); break;
        case BIGINT: record.put(rsIndex, rsRecord.getLong(i)); break;
        case FLOAT: record.put(rsIndex, rsRecord.getFloat(i)); break;
        case DOUBLE: record.put(rsIndex, rsRecord.getDouble(i)); break;

        case STRING:
        case VARCHAR:
        case CHAR:
          record.put(rsIndex, rsRecord.getByteArray(i).toString()); break;

        default:
          throw new RuntimeException(
              "Unsupported type: " + schema_.getCols().get(i).type);
        }
    }
    return (T)record;
  }

  /**
   * Closes the underlying task. Must be called for every GenericRecords object
   * created. Invalid to call other APIs after this. Idempotent.
   */
  public void close() {
    records_.close();
  }

  // Verifies that
  // avroSchema_[recordIndex] is compatible with schema_[recordServiceIndex]
  private void resolveType(int recordIndex, int recordServiceIndex) {
    List<Schema.Field> fields = avroSchema_.getFields();
    // TODO: support avro's schema resolution rules with type promotion.
    TTypeId rsType = schema_.cols.get(recordIndex).type.type_id;
    Schema.Type t = fields.get(recordIndex).schema().getType();

    if (t == Type.UNION) {
      // Unions are special because they are used to support NULLable types. In this
      // case the union has two types, one of which is NULL.
      List<Schema> children = fields.get(recordIndex).schema().getTypes();
      if (children.size() != 2) {
        throw new RuntimeException("Only union schemas with NULL are supported.");
      }
      Schema.Type t1 = children.get(0).getType();
      Schema.Type t2 = children.get(1).getType();
      if (t1 != Type.NULL && t2 != Type.NULL) {
        throw new RuntimeException("Only union schemas with NULL are supported.");
      }

      if (t1 == Type.NULL) t = t2;
      if (t2 == Type.NULL) t = t1;
    }

    switch (t) {
      case ARRAY:
      case MAP:
      case RECORD:
      case UNION:
        throw new RuntimeException("Nested schemas are not supported");

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
