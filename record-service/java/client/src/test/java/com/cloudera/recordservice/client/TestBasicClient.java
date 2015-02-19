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

package com.cloudera.recordservice.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.junit.Test;

import com.cloudera.recordservice.thrift.TPlanRequestResult;
import com.cloudera.recordservice.thrift.TTypeId;


public class TestBasicClient {

  static final int PLANNER_PORT = 40000;
  static final int WORKER_PORT = 40100;

  @Test
  public void testConnection() throws TTransportException {
    RecordServicePlannerClient planner = new RecordServicePlannerClient();
    planner.connect("localhost", PLANNER_PORT);
    RecordServiceWorkerClient worker = new RecordServiceWorkerClient();
    worker.connect("localhost", WORKER_PORT);
    planner.close();
    worker.close();
  }

  @Test
  public void testNation() throws TException {
    RecordServicePlannerClient planner = new RecordServicePlannerClient();
    planner.connect("localhost", PLANNER_PORT);
    RecordServiceWorkerClient worker = new RecordServiceWorkerClient();
    worker.connect("localhost", WORKER_PORT);

    // Plan the request
    TPlanRequestResult plan = planner.planRequest("select * from tpch.nation");

    // Verify schema
    assertEquals(plan.schema.cols.size(), 4);
    assertEquals(plan.schema.cols.get(0).name, "n_nationkey");
    assertEquals(plan.schema.cols.get(0).type.type_id, TTypeId.SMALLINT);
    assertEquals(plan.schema.cols.get(1).name, "n_name");
    assertEquals(plan.schema.cols.get(1).type.type_id, TTypeId.STRING);
    assertEquals(plan.schema.cols.get(2).name, "n_regionkey");
    assertEquals(plan.schema.cols.get(2).type.type_id, TTypeId.SMALLINT);
    assertEquals(plan.schema.cols.get(3).name, "n_comment");
    assertEquals(plan.schema.cols.get(3).type.type_id, TTypeId.STRING);

    // Execute the task
    assertEquals(plan.tasks.size(), 1);
    assertEquals(plan.tasks.get(0).hosts.size(), 3);
    Rows rows = worker.execAndFetch(plan.tasks.get(0).task);
    int numRows = 0;
    while (rows.hasNext()) {
      Rows.Row row = rows.next();
      ++numRows;
      if (numRows == 1) {
        assertEquals(row.getShort(0), 0);
        assertEquals(row.getByteArray(1).toString(), "ALGERIA");
        assertEquals(row.getShort(2), 0);
        assertEquals(row.getByteArray(3).toString(),
            " haggle. carefully final deposits detect slyly agai");
      }
    }
    rows.close();

    assertEquals(numRows, 25);
    planner.close();
    worker.close();
  }

  @Test
  public void testAllTypes() throws TException {
    RecordServicePlannerClient planner = new RecordServicePlannerClient();
    planner.connect("localhost", PLANNER_PORT);
    RecordServiceWorkerClient worker = new RecordServiceWorkerClient();
    worker.connect("localhost", WORKER_PORT);

    // Plan the request
    TPlanRequestResult plan = planner.planRequest("select * from rs.alltypes");

    // Verify schema
    assertEquals(plan.schema.cols.size(), 8);
    assertEquals(plan.schema.cols.get(0).name, "bool_col");
    assertEquals(plan.schema.cols.get(0).type.type_id, TTypeId.BOOLEAN);
    assertEquals(plan.schema.cols.get(1).name, "tinyint_col");
    assertEquals(plan.schema.cols.get(1).type.type_id, TTypeId.TINYINT);
    assertEquals(plan.schema.cols.get(2).name, "smallint_col");
    assertEquals(plan.schema.cols.get(2).type.type_id, TTypeId.SMALLINT);
    assertEquals(plan.schema.cols.get(3).name, "int_col");
    assertEquals(plan.schema.cols.get(3).type.type_id, TTypeId.INT);
    assertEquals(plan.schema.cols.get(4).name, "bigint_col");
    assertEquals(plan.schema.cols.get(4).type.type_id, TTypeId.BIGINT);
    assertEquals(plan.schema.cols.get(5).name, "float_col");
    assertEquals(plan.schema.cols.get(5).type.type_id, TTypeId.FLOAT);
    assertEquals(plan.schema.cols.get(6).name, "double_col");
    assertEquals(plan.schema.cols.get(6).type.type_id, TTypeId.DOUBLE);
    assertEquals(plan.schema.cols.get(7).name, "string_col");
    assertEquals(plan.schema.cols.get(7).type.type_id, TTypeId.STRING);

    // Execute the task
    assertEquals(plan.tasks.size(), 2);
    for (int t = 0; t < 2; ++t) {
      assertEquals(plan.tasks.get(t).hosts.size(), 3);
      Rows rows = worker.execAndFetch(plan.tasks.get(t).task);
      assertTrue(rows.hasNext());
      Rows.Row row = rows.next();

      if (row.getBoolean(0)) {
        assertEquals(row.getByte(1), 0);
        assertEquals(row.getShort(2), 1);
        assertEquals(row.getInt(3), 2);
        assertEquals(row.getLong(4), 3);
        assertEquals(row.getFloat(5), 4.0, 0.1);
        assertEquals(row.getDouble(6), 5.0, 0.1);
        assertEquals(row.getByteArray(7).toString(), "hello");
      } else {
        assertEquals(row.getByte(1), 6);
        assertEquals(row.getShort(2), 7);
        assertEquals(row.getInt(3), 8);
        assertEquals(row.getLong(4), 9);
        assertEquals(row.getFloat(5), 10.0, 0.1);
        assertEquals(row.getDouble(6), 11.0, 0.1);
        assertEquals(row.getByteArray(7).toString(), "world");
      }

      // TODO: the rows API needs to be renamed or carefully documented.
      // Calling hasNext()/get*() mutate the objects.
      assertFalse(rows.hasNext());
      rows.close();
    }

    planner.close();
    worker.close();
  }
}
