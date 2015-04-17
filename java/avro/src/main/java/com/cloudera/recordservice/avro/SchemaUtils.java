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

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

import com.cloudera.recordservice.thrift.TSchema;

public class SchemaUtils {
  /**
   * Converts a TSchema to a avro schema.
   */
  public static org.apache.avro.Schema convertSchema(TSchema schema) {
    List<Schema.Field> fields = new ArrayList<Schema.Field>();
    for (int i = 0; i < schema.getColsSize(); ++i) {
      Schema fieldSchema = null;
      switch (schema.getCols().get(i).type.type_id) {
        case BOOLEAN: fieldSchema = Schema.create(Type.BOOLEAN); break;
        case TINYINT:
        case SMALLINT:
        case INT: fieldSchema = Schema.create(Type.INT); break;
        case BIGINT: fieldSchema = Schema.create(Type.LONG); break;
        case FLOAT: fieldSchema = Schema.create(Type.FLOAT); break;
        case DOUBLE: fieldSchema = Schema.create(Type.DOUBLE); break;
        case STRING:
        case VARCHAR:
        case CHAR:
          fieldSchema = Schema.create(Type.STRING); break;
        default:
          throw new RuntimeException(
              "Unsupported type: " + schema.getCols().get(i).type);
      }
      fields.add(new Schema.Field(
          schema.getCols().get(i).getName(), fieldSchema, "", null));
    }
    return Schema.createRecord(fields);
  }
}
