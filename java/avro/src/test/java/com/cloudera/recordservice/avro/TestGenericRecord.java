// Confidential Cloudera Information: Covered by NDA.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.junit.Test;

import com.cloudera.recordservice.core.RecordServicePlannerClient;
import com.cloudera.recordservice.core.Request;
import com.cloudera.recordservice.core.TestBase;
import com.cloudera.recordservice.core.WorkerClientUtil;
import com.cloudera.recordservice.thrift.TPlanRequestResult;
import com.cloudera.recordservice.thrift.TRecordServiceException;

public class TestGenericRecord extends TestBase {
  static final int PLANNER_PORT = 40000;

  @Test
  public void testNation() throws TRecordServiceException, IOException {
    TPlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .planRequest("localhost", PLANNER_PORT,
            Request.createSqlRequest("select * from tpch.nation"));

    // Verify schema
    Schema avroSchema = SchemaUtils.convertSchema(plan.schema);
    assertTrue(avroSchema.getName() == null);
    assertEquals(avroSchema.getType(), Schema.Type.RECORD);
    List<Schema.Field> fields = avroSchema.getFields();
    assertEquals(fields.size(), 4);
    assertEquals(fields.get(0).name(), "n_nationkey");
    assertEquals(fields.get(0).schema().getType(), Schema.Type.INT);
    assertEquals(fields.get(1).name(), "n_name");
    assertEquals(fields.get(1).schema().getType(), Schema.Type.STRING);
    assertEquals(fields.get(2).name(), "n_regionkey");
    assertEquals(fields.get(2).schema().getType(), Schema.Type.INT);
    assertEquals(fields.get(3).name(), "n_comment");
    assertEquals(fields.get(3).schema().getType(), Schema.Type.STRING);

    // Execute the task
    assertEquals(plan.tasks.size(), 1);
    GenericRecords records = null;
    try {
      records = new GenericRecords(WorkerClientUtil.execTask(plan, 0));
      int numRecords = 0;
      while (records.hasNext()) {
        GenericData.Record record = records.next();
        ++numRecords;
        if (numRecords == 2) {
          assertEquals(record.get("n_nationkey"), record.get(0));
          assertEquals(record.get("n_name"), record.get(1));
          assertEquals(record.get("n_regionkey"), record.get(2));
          assertEquals(record.get("n_comment"), record.get(3));

          assertEquals(record.get("n_nationkey"), 1);
          assertEquals(record.get("n_name"), "ARGENTINA");
          assertEquals(record.get("n_regionkey"), 1);
          assertEquals(record.get("n_comment"), "al foxes promise slyly according to the regular accounts. bold requests alon");
        }
      }
      assertEquals(numRecords, 25);
    } finally {
      if (records != null) records.close();
    }
  }
}
