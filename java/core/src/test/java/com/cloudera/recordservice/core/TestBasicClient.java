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

package com.cloudera.recordservice.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import org.junit.Test;

import com.cloudera.recordservice.thrift.TErrorCode;
import com.cloudera.recordservice.thrift.TFetchResult;
import com.cloudera.recordservice.thrift.TGetSchemaResult;
import com.cloudera.recordservice.thrift.TNetworkAddress;
import com.cloudera.recordservice.thrift.TPlanRequestResult;
import com.cloudera.recordservice.thrift.TRecordServiceException;
import com.cloudera.recordservice.thrift.TSchema;
import com.cloudera.recordservice.thrift.TStats;
import com.cloudera.recordservice.thrift.TTask;
import com.cloudera.recordservice.thrift.TTaskStatus;
import com.cloudera.recordservice.thrift.TTypeId;
import com.google.common.collect.Lists;

// TODO: add more stats tests.
public class TestBasicClient {
  static final int PLANNER_PORT = 40000;
  static final int WORKER_PORT = 40100;

  public TestBasicClient() {
    // Setup log4j for testing.
    org.apache.log4j.BasicConfigurator.configure();
  }

  void fetchAndVerifyCount(Records records, int expectedCount)
      throws TRecordServiceException, IOException {
    int count = 0;
    while (records.hasNext()) {
      ++count;
      records.next();
    }
    assertEquals(expectedCount, count);
  }

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

    // Closing repeatedly is fine.
    planner.close();
    planner.close();

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

    threwException = false;
    try {
      RecordServicePlannerClient.planRequest("Bad", 1234, null);
    } catch (IOException e) {
      threwException = true;
      assertTrue(e.getMessage().contains("Could not connect to RecordServicePlanner"));
    } finally {
      assertTrue(threwException);
    }

    // Try a bad port that is another thrift service.
    threwException = false;
    try {
      new RecordServicePlannerClient("localhost", 21000);
    } catch (IOException e) {
      threwException = true;
      assertTrue(e.getMessage(), e.getMessage().contains(
          "is not running the RecordServicePlanner"));
    } finally {
      assertTrue(threwException);
    }
  }

  @Test
  public void testWorkerConnection()
      throws RuntimeException, IOException, TRecordServiceException {
    RecordServiceWorkerClient worker =
        new RecordServiceWorkerClient.Builder().connect("localhost", WORKER_PORT);

    assertEquals(worker.getProtocolVersion(), ProtocolVersion.V1);
    // Call it again and make sure it's fine.
    assertEquals(worker.getProtocolVersion(), ProtocolVersion.V1);

    assertEquals(worker.numActiveTasks(), 0);
    worker.close();

    // Close again.
    worker.close();
  }

  @Test
  public void connectionDropTest() throws IOException, TRecordServiceException {
    RecordServicePlannerClient planner =
        new RecordServicePlannerClient("localhost", PLANNER_PORT);
    TPlanRequestResult plan =
        planner.planRequest(Request.createTableScanRequest("tpch.nation"));
    TTask task = plan.getTasks().get(0);

    // Simulate dropping the connection.
    planner.closeConnectionForTesting();

    // Try using the planner connection.
    boolean exceptionThrown = false;
    try {
      planner.planRequest(Request.createTableScanRequest("tpch.nation"));
    } catch (IOException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("Could not reach service."));
    }
    assertTrue(exceptionThrown);

    exceptionThrown = false;
    try {
      planner.getSchema(Request.createTableScanRequest("tpch.nation"));
    } catch (IOException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("Could not reach service."));
    }
    assertTrue(exceptionThrown);

    // Close should still do something reasonable.
    planner.close();

    // Testing failures, don't retry.
    RecordServiceWorkerClient worker = new RecordServiceWorkerClient.Builder()
        .setMaxAttempts(1).setSleepDuration(0)
        .connect("localhost", WORKER_PORT);

    fetchAndVerifyCount(worker.execAndFetch(task), 25);

    // Try this again.
    fetchAndVerifyCount(worker.execAndFetch(task), 25);

    // Simulate dropping the worker connection
    worker.closeConnectionForTesting();

    // Try executing a task.
    exceptionThrown = false;
    try {
      worker.execAndFetch(task);
    } catch (IOException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("Could not reach service."));
    }
    assertTrue(exceptionThrown);

    exceptionThrown = false;
    try {
      worker.execTask(task);
    } catch (IOException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("Could not reach service."));
    }
    assertTrue(exceptionThrown);

    // Reestablish connection.
    worker.close();
    worker = new RecordServiceWorkerClient.Builder()
        .setMaxAttempts(1).setSleepDuration(0)
        .setFetchSize(1)
        .connect("localhost", WORKER_PORT);

    // Execute a fetch once.
    RecordServiceWorkerClient.TaskState handle = worker.execTask(task);
    worker.fetch(handle);
    worker.getTaskStatus(handle);

    // Drop the connection.
    worker.closeConnectionForTesting();

    // Try to fetch more.
    // TODO: what does retry, fault tolerance here look like?
    exceptionThrown = false;
    try {
      worker.fetch(handle);
    } catch (IOException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("Could not reach service."));
    }
    assertTrue(exceptionThrown);

    // Try to get stats
    exceptionThrown = false;
    try {
      worker.getTaskStatus(handle);
    } catch (IOException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("Could not reach service."));
    }
    assertTrue(exceptionThrown);

    // Try to close the task. The connection is bad so this won't close the
    // task on the server side.
    worker.closeTask(handle);

    // Closing the worker should behave reasonably.
    worker.close();
  }

  @Test
  public void workerMisuseTest() throws IOException, TRecordServiceException {
    // Connect to a non-existent service
    boolean threwException = false;
    try {
      new RecordServiceWorkerClient.Builder().connect("bad", PLANNER_PORT);
    } catch (IOException e) {
      threwException = true;
      assertTrue(e.getMessage().contains("Could not connect to RecordServiceWorker"));
    } finally {
      assertTrue(threwException);
    }

    // Connect to non-worker thrift service.
    threwException = false;
    try {
      new RecordServiceWorkerClient.Builder().connect("localhost", 21000);
    } catch (IOException e) {
      threwException = true;
      assertTrue(e.getMessage().contains("is not running the RecordServiceWorker"));
    } finally {
      assertTrue(threwException);
    }

    // Connect to the worker and send it bad tasks.
    RecordServiceWorkerClient worker =
        new RecordServiceWorkerClient.Builder().connect("localhost", WORKER_PORT);
    threwException = false;
    try {
      TTask task = new TTask();
      task.task = ByteBuffer.allocate(100);
      worker.execTask(task);
    } catch (TRecordServiceException e) {
      threwException = true;
      assertEquals(e.code, TErrorCode.INVALID_TASK);
      assertTrue(e.getMessage().contains("Task is corrupt."));
    } finally {
      assertTrue(threwException);
    }
  }

  @Test
  public void testTaskClose() throws TRecordServiceException, IOException {
    // Plan the request
    TPlanRequestResult plan = RecordServicePlannerClient.planRequest(
        "localhost", PLANNER_PORT,
        Request.createSqlRequest("select * from tpch.nation"));

    RecordServiceWorkerClient worker =
        new RecordServiceWorkerClient.Builder().connect("localhost", WORKER_PORT);
    assertEquals(worker.numActiveTasks(), 0);

    worker.execTask(plan.tasks.get(0));
    assertEquals(worker.numActiveTasks(), 1);
    worker.execTask(plan.tasks.get(0));
    RecordServiceWorkerClient.TaskState handle = worker.execTask(plan.tasks.get(0));
    assertEquals(worker.numActiveTasks(), 3);
    worker.closeTask(handle);
    assertEquals(worker.numActiveTasks(), 2);

    // Try to get task status with a closed handle.
    boolean exceptionThrown = false;
    try {
      worker.getTaskStatus(handle);
    } catch (IllegalArgumentException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("Invalid task handle."));
    }
    assertTrue(exceptionThrown);

    // Close again. Should be fine.
    worker.closeTask(handle);
    assertEquals(worker.numActiveTasks(), 2);

    // Closing the worker should close them all.
    worker.close();
    assertEquals(worker.numActiveTasks(), 0);
  }

  @Test
  public void testNation() throws TRecordServiceException, IOException {
    // Plan the request
    TPlanRequestResult plan = RecordServicePlannerClient.planRequest(
        "localhost", PLANNER_PORT,
        Request.createSqlRequest("select * from tpch.nation"));
    assertTrue(plan.warnings.isEmpty());

    // Verify schema
    verifyNationSchema(plan.schema, false);

    // Execute the task
    RecordServiceWorkerClient worker =
        new RecordServiceWorkerClient.Builder().connect("localhost", WORKER_PORT);

    assertEquals(plan.tasks.size(), 1);
    assertEquals(plan.tasks.get(0).local_hosts.size(), 3);

    Records records = worker.execAndFetch(plan.tasks.get(0));
    int numRecords = 0;
    while (records.hasNext()) {
      Records.Record record = records.next();
      ++numRecords;
      if (numRecords == 1) {
        assertFalse(record.isNull(0));
        assertFalse(record.isNull(1));
        assertFalse(record.isNull(2));
        assertFalse(record.isNull(3));

        assertEquals(record.getShort(0), 0);
        assertEquals(record.getByteArray(1).toString(), "ALGERIA");
        assertEquals(record.getShort(2), 0);
        assertEquals(record.getByteArray(3).toString(),
            " haggle. carefully final deposits detect slyly agai");
      }
    }

    // Reading off the end should fail gracefully.
    boolean exceptionThrown = false;
    try {
      records.next();
    } catch (IOException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage(), e.getMessage().contains("End of stream"));
    }
    assertTrue(exceptionThrown);

    // Verify status
    TTaskStatus status = records.getStatus();
    assertTrue(status.data_errors.isEmpty());
    assertTrue(status.warnings.isEmpty());

    TStats stats = status.stats;
    assertEquals(stats.task_progress, 1, 0.01);
    assertEquals(stats.num_records_read, 25);
    assertEquals(stats.num_records_returned, 25);
    assertEquals(records.progress(), 1, 0.01);

    records.close();

    assertEquals(numRecords, 25);

    // Close and run this again. The worker object should still work.
    assertEquals(worker.numActiveTasks(), 0);
    worker.close();

    assertEquals(worker.numActiveTasks(), 0);
    worker.close();
  }

  @Test
  public void testNationWithUtility() throws TRecordServiceException, IOException {
    // Plan the request
    TPlanRequestResult plan = RecordServicePlannerClient.planRequest(
        "localhost", PLANNER_PORT,
        Request.createSqlRequest("select * from tpch.nation"));

    for (int i = 0; i < plan.tasks.size(); ++i) {
      Records records = WorkerClientUtil.execTask(plan, i);
      int numRecords = 0;
      while (records.hasNext()) {
        Records.Record record = records.next();
        ++numRecords;
        if (numRecords == 1) {
          assertEquals(record.getShort(0), 0);
          assertEquals(record.getByteArray(1).toString(), "ALGERIA");
          assertEquals(record.getShort(2), 0);
          assertEquals(record.getByteArray(3).toString(),
              " haggle. carefully final deposits detect slyly agai");
        }
      }
      records.close();
      assertEquals(numRecords, 25);

      // Closing records again is idempotent
      records.close();

      // Try using records object after close.
      boolean exceptionThrown = false;
      try {
        records.getStatus();
      } catch (RuntimeException e) {
        exceptionThrown = true;
        assertTrue(e.getMessage(), e.getMessage().contains("Task already closed."));
      }
      assertTrue(exceptionThrown);
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

  /*
   * Verifies that the schema matches the nation table schema.
   */
  private void verifyNationSchema(TSchema schema, boolean parquet) {
    assertEquals(schema.cols.size(), 4);
    assertEquals(schema.cols.get(0).name, "n_nationkey");
    assertEquals(schema.cols.get(0).type.type_id,
        parquet ? TTypeId.INT : TTypeId.SMALLINT);
    assertEquals(schema.cols.get(1).name, "n_name");
    assertEquals(schema.cols.get(1).type.type_id, TTypeId.STRING);
    assertEquals(schema.cols.get(2).name, "n_regionkey");
    assertEquals(schema.cols.get(2).type.type_id,
        parquet ? TTypeId.INT : TTypeId.SMALLINT);
    assertEquals(schema.cols.get(3).name, "n_comment");
    assertEquals(schema.cols.get(3).type.type_id, TTypeId.STRING);
  }

  @Test
  public void testAllTypes() throws TRecordServiceException, IOException {
    RecordServiceWorkerClient worker =
        new RecordServiceWorkerClient.Builder().connect("localhost", WORKER_PORT);

    // Just ask for the schema.
    TGetSchemaResult schemaResult = RecordServicePlannerClient.getSchema(
        "localhost", PLANNER_PORT,
        Request.createTableScanRequest("rs.alltypes"));
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
      Records records = worker.execAndFetch(plan.tasks.get(t));
      verifyAllTypesSchema(records.getSchema());
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

    assertEquals(worker.numActiveTasks(), 0);
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

  // Returns all the strings from running plan as a list. The plan must
  // have a schema that returns a single string column.
  List<String> getAllStrings(TPlanRequestResult plan)
      throws TRecordServiceException, IOException {
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
  public void testNationPath() throws IOException, TRecordServiceException {
    TPlanRequestResult plan = RecordServicePlannerClient.planRequest(
        "localhost", PLANNER_PORT,
        Request.createPathRequest("/test-warehouse/tpch.nation/"));
    assertEquals(plan.tasks.size(), 1);
    List<String> lines = getAllStrings(plan);
    assertEquals(lines.size(), 25);
    assertEquals(lines.get(6), "6|FRANCE|3|refully final requests. regular, ironi");
  }

  @Test
  public void testNationPathParquet() throws IOException, TRecordServiceException {
    TPlanRequestResult plan = RecordServicePlannerClient.planRequest(
        "localhost", PLANNER_PORT,
        Request.createPathRequest("/test-warehouse/tpch_nation_parquet/nation.parq"));
    assertEquals(plan.tasks.size(), 1);
    verifyNationSchema(plan.schema, true);
    fetchAndVerifyCount(WorkerClientUtil.execTask(plan, 0), 25);
  }

  void testNationPathGlobbing(String path, boolean expectMatch)
      throws IOException, TRecordServiceException {
    try {
      TPlanRequestResult plan = RecordServicePlannerClient.planRequest(
          "localhost", PLANNER_PORT, Request.createPathRequest(path));
      assertEquals(plan.tasks.size(), expectMatch ? 1 : 0);
    } catch (TRecordServiceException e) {
      assertFalse(expectMatch);
      assertTrue(e.code == TErrorCode.INVALID_REQUEST);
    }
  }

  @Test
  public void testNationPathGlobbing() throws IOException, TRecordServiceException {
    // Non-matches
    testNationPathGlobbing("/test-warehouse/tpch.nation/*t", false);
    testNationPathGlobbing("/test-warehouse/tpch.nation/tbl", false);
    testNationPathGlobbing("/test-warehouse/tpch.nation*", false);
    // TODO: this should work.
    testNationPathGlobbing("/test-warehouse/tpch.*/*", false);

    // No trailing slash is okay
    testNationPathGlobbing("/test-warehouse/tpch.nation", true);
    // Trailing slashes is okay
    testNationPathGlobbing("/test-warehouse/tpch.nation/", true);
    // Multiple trailing slashes is okay
    testNationPathGlobbing("/test-warehouse/tpch.nation///", true);

    // Match for *
    testNationPathGlobbing("/test-warehouse/tpch.nation/*", true);
    testNationPathGlobbing("/test-warehouse/tpch.nation/*.tbl", true);
    testNationPathGlobbing("/test-warehouse/tpch.nation/*.tb*", true);
  }

  @Test
  public void testNationPathFiltering() throws IOException, TRecordServiceException {
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
  public void testNationView() throws IOException, TRecordServiceException {
    TPlanRequestResult plan = RecordServicePlannerClient.planRequest(
        "localhost", PLANNER_PORT,
        Request.createTableScanRequest("rs.nation_projection"));
    assertEquals(plan.tasks.size(), 1);
    assertEquals(plan.schema.cols.size(), 2);
    assertEquals(plan.schema.cols.get(0).name, "n_nationkey");
    assertEquals(plan.schema.cols.get(1).name, "n_name");

    for (int i = 0; i < plan.tasks.size(); ++i) {
      Records records = WorkerClientUtil.execTask(plan, i);
      int numRecords = 0;
      while (records.hasNext()) {
        Records.Record record = records.next();
        ++numRecords;
        switch (numRecords) {
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
      assertEquals(numRecords, 5);
    }
  }

  @Test
  public void testFetchSize() throws IOException, TRecordServiceException {
    TPlanRequestResult plan = RecordServicePlannerClient.planRequest(
        "localhost", PLANNER_PORT,
        Request.createTableScanRequest("tpch.nation"));

    TNetworkAddress addr = plan.tasks.get(0).local_hosts.get(0);
    RecordServiceWorkerClient worker = new RecordServiceWorkerClient.Builder()
        .setFetchSize(1)
        .connect(addr.hostname, addr.port);

    RecordServiceWorkerClient.TaskState handle = worker.execTask(plan.tasks.get(0));
    int numRecords = 0;
    while (true) {
      TFetchResult result = worker.fetch(handle);
      numRecords += result.num_records;
      assertTrue(result.num_records == 0 || result.num_records == 1);
      if (result.done) break;
    }
    assertEquals(numRecords, 25);
    worker.close();
  }

  @Test
  public void testMemLimitExceeded() throws IOException, TRecordServiceException {
    TPlanRequestResult plan = RecordServicePlannerClient.planRequest(
        "localhost", PLANNER_PORT,
        Request.createTableScanRequest("tpch.nation"));
    TNetworkAddress addr = plan.tasks.get(0).local_hosts.get(0);
    RecordServiceWorkerClient worker = new RecordServiceWorkerClient.Builder()
        .setMemLimit(new Long(200))
        .connect(addr.hostname, addr.port);

    RecordServiceWorkerClient.TaskState handle = worker.execTask(plan.tasks.get(0));
    boolean exceptionThrown = false;
    try {
      worker.fetch(handle);
    } catch (TRecordServiceException e) {
      exceptionThrown = true;
      assertEquals(e.code, TErrorCode.OUT_OF_MEMORY);
    }
    assertTrue(exceptionThrown);
    worker.closeTask(handle);

    // Try again going through the utility.
    exceptionThrown = false;
    try {
      worker.execAndFetch(plan.tasks.get(0));
    } catch (TRecordServiceException e) {
      exceptionThrown = true;
      assertEquals(e.code, TErrorCode.OUT_OF_MEMORY);
    }
    assertTrue(exceptionThrown);

    assertEquals(worker.numActiveTasks(), 0);
    worker.close();
  }

  @Test
  public void testEmptyProjection() throws IOException, TRecordServiceException {
    TPlanRequestResult plan = RecordServicePlannerClient.planRequest(
        "localhost", PLANNER_PORT,
        Request.createProjectionRequest("tpch.nation", null));
    assertEquals(plan.tasks.size(), 1);

    // Verify schema
    assertEquals(plan.schema.cols.size(), 1);
    assertEquals(plan.schema.cols.get(0).name, "count(*)");
    assertEquals(plan.schema.cols.get(0).type.type_id, TTypeId.BIGINT);

    // Verify count(*) result.
    Records records = WorkerClientUtil.execTask(plan, 0);
    assertTrue(records.hasNext());
    Records.Record result = records.next();
    assertEquals(result.getLong(0), 25);
    assertFalse(records.hasNext());
    records.close();

    // Empty column list should do the same
    plan = RecordServicePlannerClient.planRequest(
        "localhost", PLANNER_PORT,
        Request.createProjectionRequest("tpch.nation", new ArrayList<String>()));
    assertEquals(plan.tasks.size(), 1);

    plan = RecordServicePlannerClient.planRequest(
        "localhost", PLANNER_PORT,
        Request.createSqlRequest("select count(*), count(*) from tpch.nation"));
    assertEquals(plan.tasks.size(), 1);
    assertEquals(plan.schema.cols.size(), 2);
    assertEquals(plan.schema.cols.get(0).name, "count(*)");
    assertEquals(plan.schema.cols.get(1).name, "count(*)");
    assertEquals(plan.schema.cols.get(0).type.type_id, TTypeId.BIGINT);
    assertEquals(plan.schema.cols.get(1).type.type_id, TTypeId.BIGINT);
  }

  @Test
  public void testProjection() throws IOException, TRecordServiceException {
    TPlanRequestResult plan = RecordServicePlannerClient.planRequest(
        "localhost", PLANNER_PORT,
        Request.createProjectionRequest("tpch.nation", Lists.newArrayList("n_comment")));
    assertEquals(plan.tasks.size(), 1);
    assertEquals(plan.schema.cols.size(), 1);
    assertEquals(plan.schema.cols.get(0).name, "n_comment");
    assertEquals(plan.schema.cols.get(0).type.type_id, TTypeId.STRING);
  }

  @Test
  public void testLimit() throws IOException, TRecordServiceException {
    TPlanRequestResult plan = RecordServicePlannerClient.planRequest(
        "localhost", PLANNER_PORT,
        Request.createTableScanRequest("tpch.nation"));
    TNetworkAddress addr = plan.tasks.get(0).local_hosts.get(0);

    // Set with a higher than count limit.
    RecordServiceWorkerClient worker = new RecordServiceWorkerClient.Builder()
        .setLimit(new Long(30))
        .connect(addr.hostname, addr.port);
    fetchAndVerifyCount(worker.execAndFetch(plan.tasks.get(0)), 25);
    worker.close();

    worker = new RecordServiceWorkerClient.Builder()
        .setLimit(null)
        .connect(addr.hostname, addr.port);
    fetchAndVerifyCount(worker.execAndFetch(plan.tasks.get(0)), 25);
    worker.close();

    worker = new RecordServiceWorkerClient.Builder()
        .setLimit(new Long(10))
        .connect(addr.hostname, addr.port);
    fetchAndVerifyCount(worker.execAndFetch(plan.tasks.get(0)), 10);
    worker.close();

    worker = new RecordServiceWorkerClient.Builder()
        .setLimit(new Long(1))
        .connect(addr.hostname, addr.port);
    fetchAndVerifyCount(worker.execAndFetch(plan.tasks.get(0)), 1);
    worker.close();
  }

  @Test
  public void testNonLocalWorker() throws IOException, TRecordServiceException {
    TPlanRequestResult plan = RecordServicePlannerClient.planRequest(
        "localhost", PLANNER_PORT,
        Request.createTableScanRequest("tpch.nation"));
    assertEquals(plan.tasks.size(), 1);

    // Clear the local hosts.
    TTask task = plan.tasks.get(0);
    task.local_hosts.clear();
    Records records = WorkerClientUtil.execTask(plan, 0);
    fetchAndVerifyCount(records, 25);
    records.close();

    // Clear all hosts.
    boolean exceptionThrown = false;
    plan.hosts.clear();
    try {
      records = WorkerClientUtil.execTask(plan, 0);
    } catch (RuntimeException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage(), e.getMessage().contains("No hosts are provided"));
    }
    assertTrue(exceptionThrown);

    // Try invalid task id
    exceptionThrown = false;
    try {
      WorkerClientUtil.execTask(plan, 1);
    } catch (RuntimeException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage(), e.getMessage().contains("Invalid task id."));
    }
    assertTrue(exceptionThrown);

    exceptionThrown = false;
    try {
      WorkerClientUtil.execTask(plan, -1);
    } catch (RuntimeException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage(), e.getMessage().contains("Invalid task id."));
    }
    assertTrue(exceptionThrown);
  }
}
