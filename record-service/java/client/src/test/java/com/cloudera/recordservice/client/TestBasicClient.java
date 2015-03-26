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

import java.io.IOException;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.TimeZone;

import org.apache.thrift.TException;
import org.junit.Test;

import com.cloudera.recordservice.thrift.TGetSchemaResult;
import com.cloudera.recordservice.thrift.TPlanRequestResult;
import com.cloudera.recordservice.thrift.TRecordServiceException;
import com.cloudera.recordservice.thrift.TSchema;
import com.cloudera.recordservice.thrift.TStats;
import com.cloudera.recordservice.thrift.TTaskStatus;
import com.cloudera.recordservice.thrift.TTypeId;
import com.cloudera.recordservice.thrift.TUniqueId;
import com.google.common.collect.Lists;

// TODO: add more API misuse tests.
// TODO: add more stats tests.
public class TestBasicClient {

  static final int PLANNER_PORT = 40000;
  static final int WORKER_PORT = 40100;

  @Test
  public void testPlannerConnection()
      throws RuntimeException, IOException, TRecordServiceException {
    RecordServicePlannerClient planner =
        new RecordServicePlannerClient("localhost", PLANNER_PORT);

    // Test calling the APIs after close.
    planner.close();
    boolean threwException = false;
    try {
      planner.getProtocolVersion();
    } catch (RuntimeException e) {
      threwException = true;
      assertTrue(e.getMessage().contains("Client not connected."));
    } finally {
      assertTrue(threwException);
    }

    threwException = false;
    try {
      planner.planRequest(Request.createSqlRequest("ABCD"));
    } catch (RuntimeException e) {
      threwException = true;
      assertTrue(e.getMessage().contains("Client not connected."));
    } finally {
      assertTrue(threwException);
    }

    planner = new RecordServicePlannerClient("localhost", PLANNER_PORT);
    assertEquals(planner.getProtocolVersion(), ProtocolVersion.V1);
    // Call it again and make sure it's fine.
    assertEquals(planner.getProtocolVersion(), ProtocolVersion.V1);

    // Plan a request.
    planner.planRequest(Request.createSqlRequest("select * from tpch.nation"));

    // Try connecting to a bad planner.
    threwException = false;
    try {
      new RecordServicePlannerClient("localhost", 12345);
    } catch (IOException e) {
      threwException = true;
      assertTrue(e.getMessage().contains("Could not connect to RecordServicePlanner"));
    } finally {
      assertTrue(threwException);
    }
  }

  @Test
  public void testWorkerConnection() throws RuntimeException, TException {
    RecordServiceWorkerClient worker = new RecordServiceWorkerClient();

    boolean threwException = false;
    try {
      worker.getProtocolVersion();
    } catch (RuntimeException e) {
      threwException = true;
      assertTrue(e.getMessage().contains("Client not connected."));
    } finally {
      assertTrue(threwException);
    }

    worker.connect("localhost", WORKER_PORT);
    threwException = false;
    try {
      worker.connect("localhost", PLANNER_PORT);
    } catch (RuntimeException e) {
      threwException = true;
      assertTrue(e.getMessage().contains(
          "Already connected. Must call close() first."));
    } finally {
      assertTrue(threwException);
    }

    assertEquals(worker.getProtocolVersion(), ProtocolVersion.V1);
    // Call it again and make sure it's fine.
    assertEquals(worker.getProtocolVersion(), ProtocolVersion.V1);

    worker.close();

    // Should be able to connect now.
    worker.connect("localhost", PLANNER_PORT);
    worker.close();
  }

  @Test
  public void testTaskClose() throws TException, IOException {
    // Plan the request
    TPlanRequestResult plan = RecordServicePlannerClient.planRequest(
        "localhost", PLANNER_PORT,
        Request.createSqlRequest("select * from tpch.nation"));

    RecordServiceWorkerClient worker = new RecordServiceWorkerClient();
    worker.connect("localhost", WORKER_PORT);
    assertEquals(worker.numActiveTasks(), 0);

    worker.execTask(plan.tasks.get(0).task);
    assertEquals(worker.numActiveTasks(), 1);
    worker.execTask(plan.tasks.get(0).task);
    TUniqueId handle = worker.execTask(plan.tasks.get(0).task);
    assertEquals(worker.numActiveTasks(), 3);
    worker.closeTask(handle);
    assertEquals(worker.numActiveTasks(), 2);

    // Close again. Should be fine.
    worker.closeTask(handle);
    assertEquals(worker.numActiveTasks(), 2);

    // Closing the worker should close them all.
    worker.close();
    assertEquals(worker.numActiveTasks(), 0);
  }

  @Test
  public void testBadHandle() throws TException, IOException {
    RecordServiceWorkerClient worker = new RecordServiceWorkerClient();
    worker.connect("localhost", WORKER_PORT);

    TUniqueId badHandle = new TUniqueId();
    worker.closeTask(badHandle);

    boolean threwException = false;
    try {
      worker.fetch(badHandle);
    } catch (RuntimeException e) {
      threwException = true;
      assertTrue(e.getMessage().contains("Invalid task handle."));
    }
    assertTrue(threwException);

    threwException = false;
    try {
      worker.getTaskStatus(badHandle);
    } catch (RuntimeException e) {
      threwException = true;
      assertTrue(e.getMessage().contains("Invalid task handle."));
    }
    assertTrue(threwException);

  }

  @Test
  public void testNation() throws TException, IOException {
    // Plan the request
    TPlanRequestResult plan = RecordServicePlannerClient.planRequest(
        "localhost", PLANNER_PORT,
        Request.createSqlRequest("select * from tpch.nation"));
    assertTrue(plan.warnings.isEmpty());

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
    RecordServiceWorkerClient worker = new RecordServiceWorkerClient();
    worker.connect("localhost", WORKER_PORT);

    assertEquals(plan.tasks.size(), 1);
    assertEquals(plan.tasks.get(0).local_hosts.size(), 3);
    for (int i = 0; i < 2; ++i) {
      Records records = worker.execAndFetch(plan.tasks.get(0).task);
      int numRows = 0;
      while (records.hasNext()) {
        Records.Record record = records.next();
        ++numRows;
        if (numRows == 1) {
          assertEquals(record.getShort(0), 0);
          assertEquals(record.getByteArray(1).toString(), "ALGERIA");
          assertEquals(record.getShort(2), 0);
          assertEquals(record.getByteArray(3).toString(),
              " haggle. carefully final deposits detect slyly agai");
        }
      }

      // Verify status
      TTaskStatus status = records.getStatus();
      assertTrue(status.data_errors.isEmpty());
      assertTrue(status.warnings.isEmpty());

      TStats stats = status.stats;
      assertEquals(stats.completion_percentage, 100, 0.1);
      assertEquals(stats.num_rows_read, 25);
      assertEquals(stats.num_rows_returned, 25);

      records.close();

      assertEquals(numRows, 25);

      // Close and run this again. The worker object should still work.
      worker.close();
      worker.connect("localhost", WORKER_PORT);
    }
    worker.close();
  }

  @Test
  public void testNationWithUtility() throws TException, IOException {
    // Plan the request
    TPlanRequestResult plan = RecordServicePlannerClient.planRequest(
        "localhost", PLANNER_PORT,
        Request.createSqlRequest("select * from tpch.nation"));

    for (int i = 0; i < plan.tasks.size(); ++i) {
      Records records = WorkerClientUtil.execTask(plan, i);
      int numRows = 0;
      while (records.hasNext()) {
        Records.Record record = records.next();
        ++numRows;
        if (numRows == 1) {
          assertEquals(record.getShort(0), 0);
          assertEquals(record.getByteArray(1).toString(), "ALGERIA");
          assertEquals(record.getShort(2), 0);
          assertEquals(record.getByteArray(3).toString(),
              " haggle. carefully final deposits detect slyly agai");
        }
      }
      records.close();
      assertEquals(numRows, 25);
    }
  }

  /*
   * Verifies that the schema matches the alltypes table schema.
   */
  private void verifyAllTypesSchema(TSchema schema) {
    assertEquals(schema.cols.size(), 12);
    assertEquals(schema.cols.get(0).name, "bool_col");
    assertEquals(schema.cols.get(0).type.type_id, TTypeId.BOOLEAN);
    assertEquals(schema.cols.get(1).name, "tinyint_col");
    assertEquals(schema.cols.get(1).type.type_id, TTypeId.TINYINT);
    assertEquals(schema.cols.get(2).name, "smallint_col");
    assertEquals(schema.cols.get(2).type.type_id, TTypeId.SMALLINT);
    assertEquals(schema.cols.get(3).name, "int_col");
    assertEquals(schema.cols.get(3).type.type_id, TTypeId.INT);
    assertEquals(schema.cols.get(4).name, "bigint_col");
    assertEquals(schema.cols.get(4).type.type_id, TTypeId.BIGINT);
    assertEquals(schema.cols.get(5).name, "float_col");
    assertEquals(schema.cols.get(5).type.type_id, TTypeId.FLOAT);
    assertEquals(schema.cols.get(6).name, "double_col");
    assertEquals(schema.cols.get(6).type.type_id, TTypeId.DOUBLE);
    assertEquals(schema.cols.get(7).name, "string_col");
    assertEquals(schema.cols.get(7).type.type_id, TTypeId.STRING);
    assertEquals(schema.cols.get(8).name, "varchar_col");
    assertEquals(schema.cols.get(8).type.type_id, TTypeId.VARCHAR);
    assertEquals(schema.cols.get(8).type.len, 10);
    assertEquals(schema.cols.get(9).name, "char_col");
    assertEquals(schema.cols.get(9).type.type_id, TTypeId.CHAR);
    assertEquals(schema.cols.get(9).type.len, 5);
    assertEquals(schema.cols.get(10).name, "timestamp_col");
    assertEquals(schema.cols.get(10).type.type_id, TTypeId.TIMESTAMP_NANOS);
    assertEquals(schema.cols.get(11).name, "decimal_col");
    assertEquals(schema.cols.get(11).type.type_id, TTypeId.DECIMAL);
    assertEquals(schema.cols.get(11).type.precision, 24);
    assertEquals(schema.cols.get(11).type.scale, 10);
  }

  @Test
  public void testAllTypes() throws TException, IOException {
    RecordServiceWorkerClient worker = new RecordServiceWorkerClient();
    worker.connect("localhost", WORKER_PORT);

    // Just ask for the schema.
    TGetSchemaResult schemaResult = RecordServicePlannerClient.getSchema(
        "localhost", PLANNER_PORT,
        Request.createTableRequest("rs.alltypes"));
    verifyAllTypesSchema(schemaResult.schema);

    // Plan the request
    TPlanRequestResult plan = RecordServicePlannerClient.planRequest(
        "localhost", PLANNER_PORT,
        Request.createSqlRequest("select * from rs.alltypes"));

    verifyAllTypesSchema(plan.schema);

    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
    format.setTimeZone(TimeZone.getTimeZone("GMT"));

    // Execute the task
    assertEquals(plan.tasks.size(), 2);
    for (int t = 0; t < 2; ++t) {
      assertEquals(plan.tasks.get(t).local_hosts.size(), 3);
      Records records = worker.execAndFetch(plan.tasks.get(t).task);
      assertTrue(records.hasNext());
      Records.Record record = records.next();

      if (record.getBoolean(0)) {
        assertEquals(record.getByte(1), 0);
        assertEquals(record.getShort(2), 1);
        assertEquals(record.getInt(3), 2);
        assertEquals(record.getLong(4), 3);
        assertEquals(record.getFloat(5), 4.0, 0.1);
        assertEquals(record.getDouble(6), 5.0, 0.1);
        assertEquals(record.getByteArray(7).toString(), "hello");
        assertEquals(record.getByteArray(8).toString(), "vchar1");
        assertEquals(record.getByteArray(9).toString(), "char1");
        assertEquals(
            format.format(record.getTimestampNanos(10).toTimeStamp()), "2015-01-01");
        assertEquals(record.getDecimal(11).toBigDecimal(),
            new BigDecimal("3.1415920000"));
      } else {
        assertEquals(record.getByte(1), 6);
        assertEquals(record.getShort(2), 7);
        assertEquals(record.getInt(3), 8);
        assertEquals(record.getLong(4), 9);
        assertEquals(record.getFloat(5), 10.0, 0.1);
        assertEquals(record.getDouble(6), 11.0, 0.1);
        assertEquals(record.getByteArray(7).toString(), "world");
        assertEquals(record.getByteArray(8).toString(), "vchar2");
        assertEquals(record.getByteArray(9).toString(), "char2");
        assertEquals(
            format.format(record.getTimestampNanos(10).toTimeStamp()), "2016-01-01");
        assertEquals(record.getDecimal(11).toBigDecimal(),
            new BigDecimal("1234.5678900000"));
      }

      // TODO: the Records API needs to be renamed or carefully documented.
      // Calling hasNext()/get*() mutate the objects.
      assertFalse(records.hasNext());
      records.close();
    }

    worker.close();
  }

  @Test
  public void testAllTypesEmpty() throws TRecordServiceException, IOException {
    TPlanRequestResult plan = RecordServicePlannerClient.planRequest(
        "localhost", PLANNER_PORT,
        Request.createSqlRequest("select * from rs.alltypes_empty"));
    assertEquals(plan.tasks.size(), 0);
    verifyAllTypesSchema(plan.schema);
  }

  @Test
  public void testConstant() throws TException, IOException {
    boolean exceptionThrown = false;
    try {
      RecordServicePlannerClient.planRequest("localhost", PLANNER_PORT,
          Request.createSqlRequest("select 1"));
    } catch (TRecordServiceException e) {
      assertTrue(e.message.contains("Could not plan request."));
      assertTrue(e.detail.contains("No scan nodes found for this query"));
      exceptionThrown = true;
    }
    assertTrue(exceptionThrown);
  }

  // Returns all the strings from running plan as a list. The plan must
  // have a schema that returns a single string column.
  List<String> getAllStrings(TPlanRequestResult plan) throws TException, IOException {
    List<String> results = Lists.newArrayList();
    assertEquals(plan.schema.cols.size(), 1);
    assertEquals(plan.schema.cols.get(0).type.type_id, TTypeId.STRING);
    for (int i = 0; i < plan.tasks.size(); ++i) {
      Records records = null;
      try {
        records = WorkerClientUtil.execTask(plan, i);
        while (records.hasNext()) {
          Records.Record record = records.next();
          results.add(record.getByteArray(0).toString());
        }
      } finally {
        if (records != null) records.close();
      }
    }
    return results;
  }

  @Test
  public void testNationPath() throws IOException, TException {
    TPlanRequestResult plan = RecordServicePlannerClient.planRequest(
        "localhost", PLANNER_PORT,
        Request.createPathRequest("/test-warehouse/tpch.nation/"));
    assertEquals(plan.tasks.size(), 1);
    List<String> lines = getAllStrings(plan);
    assertEquals(lines.size(), 25);
    assertEquals(lines.get(6), "6|FRANCE|3|refully final requests. regular, ironi");
  }

  @Test
  public void testNationPathFiltering() throws IOException, TException {
    TPlanRequestResult plan = RecordServicePlannerClient.planRequest(
        "localhost", PLANNER_PORT,
        Request.createPathRequest("/test-warehouse/tpch.nation/",
            "select * from __PATH__ where record like '6|FRANCE%'"));
    assertEquals(plan.tasks.size(), 1);
    List<String> lines = getAllStrings(plan);
    assertEquals(lines.size(), 1);
    assertEquals(lines.get(0), "6|FRANCE|3|refully final requests. regular, ironi");
  }

  @Test
  public void testNationView() throws IOException, TException {
    TPlanRequestResult plan = RecordServicePlannerClient.planRequest(
        "localhost", PLANNER_PORT,
        Request.createTableRequest("rs.nation_projection"));
    assertEquals(plan.tasks.size(), 1);
    assertEquals(plan.schema.cols.size(), 2);
    assertEquals(plan.schema.cols.get(0).name, "n_nationkey");
    assertEquals(plan.schema.cols.get(1).name, "n_name");

    for (int i = 0; i < plan.tasks.size(); ++i) {
      Records records = WorkerClientUtil.execTask(plan, i);
      int numRows = 0;
      while (records.hasNext()) {
        Records.Record record = records.next();
        ++numRows;
        switch (numRows) {
        case 1:
          assertEquals(record.getShort(0), 0);
          assertEquals(record.getByteArray(1).toString(), "ALGERIA");
          break;
        case 2:
          assertEquals(record.getShort(0), 1);
          assertEquals(record.getByteArray(1).toString(), "ARGENTINA");
          break;
        case 3:
          assertEquals(record.getShort(0), 2);
          assertEquals(record.getByteArray(1).toString(), "BRAZIL");
          break;
        case 4:
          assertEquals(record.getShort(0), 3);
          assertEquals(record.getByteArray(1).toString(), "CANADA");
          break;
        case 5:
          assertEquals(record.getShort(0), 4);
          assertEquals(record.getByteArray(1).toString(), "EGYPT");
          break;
        }
      }
      records.close();
      assertEquals(numRows, 5);
    }
  }
}
