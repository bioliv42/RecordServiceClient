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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.recordservice.thrift.TColumnDesc;
import com.cloudera.recordservice.thrift.TErrorCode;
import com.cloudera.recordservice.thrift.TFetchResult;
import com.cloudera.recordservice.thrift.TGetSchemaResult;
import com.cloudera.recordservice.thrift.TLoggingLevel;
import com.cloudera.recordservice.thrift.TNetworkAddress;
import com.cloudera.recordservice.thrift.TPlanRequestResult;
import com.cloudera.recordservice.thrift.TRecordServiceException;
import com.cloudera.recordservice.thrift.TSchema;
import com.cloudera.recordservice.thrift.TStats;
import com.cloudera.recordservice.thrift.TTask;
import com.cloudera.recordservice.thrift.TTaskStatus;
import com.cloudera.recordservice.thrift.TType;
import com.cloudera.recordservice.thrift.TTypeId;
import com.google.common.collect.Lists;

// TODO: add more stats tests.
// TODO: add testes to verify that we don't retry when the error is not retryable.
public class TestBasicClient extends TestBase {
  static final int PLANNER_PORT = 40000;
  static final int WORKER_PORT = 40100;

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
    RecordServicePlannerClient planner = new RecordServicePlannerClient.Builder()
        .connect("localhost", PLANNER_PORT);
    // Test calling the APIs after close.
    planner.close();
    boolean exceptionThrown = false;
    try {
      planner.getProtocolVersion();
    } catch (RuntimeException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("Client not connected."));
    } finally {
      assertTrue(exceptionThrown);
    }

    exceptionThrown = false;
    try {
      planner.planRequest(Request.createSqlRequest("ABCD"));
    } catch (RuntimeException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("Client not connected."));
    } finally {
      assertTrue(exceptionThrown);
    }

    planner = new RecordServicePlannerClient.Builder().connect("localhost", PLANNER_PORT);
    assertEquals(ProtocolVersion.V1, planner.getProtocolVersion());
    // Call it again and make sure it's fine.
    assertEquals(ProtocolVersion.V1, planner.getProtocolVersion());

    // Plan a request.
    planner.planRequest(Request.createSqlRequest("select * from tpch.nation"));

    // Closing repeatedly is fine.
    planner.close();
    planner.close();

    // Try connecting to a bad planner.
    exceptionThrown = false;
    try {
      new RecordServicePlannerClient.Builder().connect("localhost", 12345);
    } catch (IOException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("Could not connect to RecordServicePlanner"));
    } finally {
      assertTrue(exceptionThrown);
    }

    exceptionThrown = false;
    try {
      new RecordServicePlannerClient.Builder().planRequest("Bad", 1234, null);
    } catch (IOException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("Could not connect to RecordServicePlanner"));
    } finally {
      assertTrue(exceptionThrown);
    }

    // Try a bad port that is another thrift service.
    exceptionThrown = false;
    try {
      new RecordServicePlannerClient.Builder().connect("localhost", 21000);
    } catch (IOException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage(), e.getMessage().contains(
          "is not running the RecordServicePlanner"));
    } finally {
      assertTrue(exceptionThrown);
    }
  }

  @Test
  public void testWorkerConnection()
      throws RuntimeException, IOException, TRecordServiceException {
    RecordServiceWorkerClient worker =
        new RecordServiceWorkerClient.Builder().connect("localhost", WORKER_PORT);

    assertEquals(ProtocolVersion.V1, worker.getProtocolVersion());
    // Call it again and make sure it's fine.
    assertEquals(ProtocolVersion.V1, worker.getProtocolVersion());

    assertEquals(0, worker.numActiveTasks());
    worker.close();

    // Close again.
    worker.close();
  }

  @Test
  public void testConnectionDrop() throws IOException, TRecordServiceException {
    RecordServicePlannerClient planner = new RecordServicePlannerClient.Builder()
        .setMaxAttempts(1).setSleepDurationMs(0)
        .connect("localhost", PLANNER_PORT);
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
    // Simulate dropping the connection.
    planner.closeConnectionForTesting();
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
        .setMaxAttempts(1).setSleepDurationMs(0)
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
        .setMaxAttempts(1).setSleepDurationMs(0)
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
  public void testPlannerTimeout() throws IOException, TRecordServiceException {
    boolean exceptionThrown = false;
    try {
      new RecordServicePlannerClient.Builder()
          .setMaxAttempts(1).setSleepDurationMs(0).setTimeoutMs(1)
          .planRequest("localhost", PLANNER_PORT,
              Request.createTableScanRequest("tpch.nation"));
    } catch (IOException e) {
      exceptionThrown = true;
      assertTrue(e.getCause().getMessage().contains("java.net.SocketTimeoutException"));
    } finally {
      assertTrue(exceptionThrown);
    }

    exceptionThrown = false;
    try {
      new RecordServicePlannerClient.Builder()
          .setMaxAttempts(1).setSleepDurationMs(0).setTimeoutMs(1)
          .getSchema("localhost", PLANNER_PORT,
              Request.createTableScanRequest("tpch.nation"));
    } catch (IOException e) {
      exceptionThrown = true;
      assertTrue(e.getCause().getMessage().contains("java.net.SocketTimeoutException"));
    } finally {
      assertTrue(exceptionThrown);
    }
  }

  @Test
  public void testWorkerTimeout() throws IOException, TRecordServiceException {
    TTask task = new RecordServicePlannerClient.Builder()
        .setMaxAttempts(1).setSleepDurationMs(0)
        .planRequest("localhost", PLANNER_PORT,
            Request.createTableScanRequest("tpch.nation")).getTasks().get(0);

    boolean exceptionThrown = false;

    try {
      RecordServiceWorkerClient worker = new RecordServiceWorkerClient.Builder()
          .setMaxAttempts(1).setSleepDurationMs(0).setTimeoutMs(1)
          .connect("localhost", WORKER_PORT);
      worker.execAndFetch(task);
      worker.close();
    } catch (IOException e) {
      exceptionThrown = true;
      assertTrue(e.getCause().getMessage().contains("java.net.SocketTimeoutException"));
    } finally {
      assertTrue(exceptionThrown);
    }
  }

  @Test
  public void testWorkerMisuse() throws IOException, TRecordServiceException {
    // Connect to a non-existent service
    boolean exceptionThrown = false;
    try {
      new RecordServiceWorkerClient.Builder().connect("bad", PLANNER_PORT);
    } catch (IOException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("Could not connect to RecordServiceWorker"));
    } finally {
      assertTrue(exceptionThrown);
    }

    // Connect to non-worker thrift service.
    exceptionThrown = false;
    try {
      new RecordServiceWorkerClient.Builder().connect("localhost", 21000);
    } catch (IOException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("is not running the RecordServiceWorker"));
    } finally {
      assertTrue(exceptionThrown);
    }

    // Connect to the worker and send it bad tasks.
    RecordServiceWorkerClient worker =
        new RecordServiceWorkerClient.Builder().connect("localhost", WORKER_PORT);
    exceptionThrown = false;
    try {
      TTask task = new TTask();
      task.task = ByteBuffer.allocate(100);
      worker.execTask(task);
    } catch (TRecordServiceException e) {
      exceptionThrown = true;
      assertEquals(TErrorCode.INVALID_TASK, e.getCode());
      assertTrue(e.getMessage().contains("Task is corrupt."));
    } finally {
      assertTrue(exceptionThrown);
    }
  }

  @Test
  public void testTaskClose() throws TRecordServiceException, IOException {
    // Plan the request
    TPlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .planRequest("localhost", PLANNER_PORT,
            Request.createSqlRequest("select * from tpch.nation"));

    RecordServiceWorkerClient worker =
        new RecordServiceWorkerClient.Builder().connect("localhost", WORKER_PORT);
    assertEquals(0, worker.numActiveTasks());

    worker.execTask(plan.tasks.get(0));
    assertEquals(1, worker.numActiveTasks());
    worker.execTask(plan.tasks.get(0));
    RecordServiceWorkerClient.TaskState handle = worker.execTask(plan.tasks.get(0));
    assertEquals(3, worker.numActiveTasks());
    worker.closeTask(handle);
    assertEquals(2, worker.numActiveTasks());

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
    assertEquals(2, worker.numActiveTasks());

    // Closing the worker should close them all.
    worker.close();
    assertEquals(0, worker.numActiveTasks());
  }

  @Test
  public void testNation() throws TRecordServiceException, IOException {
    // Plan the request
    TPlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .planRequest("localhost", PLANNER_PORT,
            Request.createSqlRequest("select * from tpch.nation"));
    assertTrue(plan.warnings.isEmpty());

    // Verify schema
    verifyNationSchema(plan.schema, false);

    // Execute the task
    RecordServiceWorkerClient worker =
        new RecordServiceWorkerClient.Builder().connect("localhost", WORKER_PORT);

    assertEquals(1, plan.tasks.size());
    assertEquals(3, plan.tasks.get(0).local_hosts.size());

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

        assertEquals(0, record.nextShort(0));
        assertEquals("ALGERIA", record.nextByteArray(1).toString());
        assertEquals(0, record.nextShort(2));
        assertEquals(" haggle. carefully final deposits detect slyly agai",
            record.nextByteArray(3).toString());
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
    assertEquals(1, stats.task_progress, 0.01);
    assertEquals(25, stats.num_records_read);
    assertEquals(25, stats.num_records_returned);
    assertEquals(1, records.progress(), 0.01);

    records.close();

    assertEquals(25, numRecords);

    // Close and run this again. The worker object should still work.
    assertEquals(0, worker.numActiveTasks());
    worker.close();

    assertEquals(0, worker.numActiveTasks());
    worker.close();
  }

  @Test
  public void testNationWithUtility() throws TRecordServiceException, IOException {
    // Plan the request
    TPlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .planRequest("localhost", PLANNER_PORT,
            Request.createSqlRequest("select * from tpch.nation"));
    for (int i = 0; i < plan.tasks.size(); ++i) {
      Records records = WorkerClientUtil.execTask(plan, i);
      int numRecords = 0;
      while (records.hasNext()) {
        Records.Record record = records.next();
        ++numRecords;
        if (numRecords == 1) {
          assertEquals(0, record.nextShort(0));
          assertEquals("ALGERIA", record.nextByteArray(1).toString());
          assertEquals(0, record.nextShort(2));
          assertEquals(" haggle. carefully final deposits detect slyly agai",
              record.nextByteArray(3).toString());
        }
      }
      records.close();
      assertEquals(25, numRecords);

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
    assertEquals(12, schema.cols.size());
    assertEquals("bool_col", schema.cols.get(0).name);
    assertEquals(TTypeId.BOOLEAN, schema.cols.get(0).type.type_id);
    assertEquals("tinyint_col", schema.cols.get(1).name);
    assertEquals(TTypeId.TINYINT, schema.cols.get(1).type.type_id);
    assertEquals("smallint_col", schema.cols.get(2).name);
    assertEquals(TTypeId.SMALLINT, schema.cols.get(2).type.type_id);
    assertEquals("int_col", schema.cols.get(3).name);
    assertEquals(TTypeId.INT, schema.cols.get(3).type.type_id);
    assertEquals("bigint_col", schema.cols.get(4).name);
    assertEquals(TTypeId.BIGINT, schema.cols.get(4).type.type_id);
    assertEquals("float_col", schema.cols.get(5).name);
    assertEquals(TTypeId.FLOAT, schema.cols.get(5).type.type_id);
    assertEquals("double_col", schema.cols.get(6).name);
    assertEquals(TTypeId.DOUBLE, schema.cols.get(6).type.type_id);
    assertEquals("string_col", schema.cols.get(7).name);
    assertEquals(TTypeId.STRING, schema.cols.get(7).type.type_id);
    assertEquals("varchar_col", schema.cols.get(8).name);
    assertEquals(TTypeId.VARCHAR, schema.cols.get(8).type.type_id);
    assertEquals(10, schema.cols.get(8).type.len);
    assertEquals("char_col", schema.cols.get(9).name);
    assertEquals(TTypeId.CHAR, schema.cols.get(9).type.type_id);
    assertEquals(5, schema.cols.get(9).type.len);
    assertEquals("timestamp_col", schema.cols.get(10).name);
    assertEquals(TTypeId.TIMESTAMP_NANOS, schema.cols.get(10).type.type_id);
    assertEquals("decimal_col", schema.cols.get(11).name);
    assertEquals(TTypeId.DECIMAL, schema.cols.get(11).type.type_id);
    assertEquals(24, schema.cols.get(11).type.precision);
    assertEquals(10, schema.cols.get(11).type.scale);
  }

  private void verifyAllTypes(TPlanRequestResult plan)
      throws TRecordServiceException, IOException {
    RecordServiceWorkerClient worker =
      new RecordServiceWorkerClient.Builder().connect("localhost", WORKER_PORT);

    verifyAllTypesSchema(plan.schema);

    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
    format.setTimeZone(TimeZone.getTimeZone("GMT"));

    // Execute the task
    assertEquals(2, plan.tasks.size());
    for (int t = 0; t < 2; ++t) {
      assertEquals(3, plan.tasks.get(t).local_hosts.size());
      Records records = worker.execAndFetch(plan.tasks.get(t));
      verifyAllTypesSchema(records.getSchema());
      assertTrue(records.hasNext());
      Records.Record record = records.next();

      if (record.nextBoolean(0)) {
        assertEquals(0, record.nextByte(1));
        assertEquals(1, record.nextShort(2));
        assertEquals(2, record.nextInt(3));
        assertEquals(3, record.nextLong(4));
        assertEquals(4.0, record.nextFloat(5), 0.1);
        assertEquals(5.0, record.nextDouble(6), 0.1);
        assertEquals("hello", record.nextByteArray(7).toString());
        assertEquals("vchar1", record.nextByteArray(8).toString());
        assertEquals("char1", record.nextByteArray(9).toString());
        assertEquals("2015-01-01",
            format.format(record.nextTimestampNanos(10).toTimeStamp()));
        assertEquals(new BigDecimal("3.1415920000"),
            record.nextDecimal(11).toBigDecimal());
      } else {
        assertEquals(6, record.nextByte(1));
        assertEquals(7, record.nextShort(2));
        assertEquals(8, record.nextInt(3));
        assertEquals(9, record.nextLong(4));
        assertEquals(10.0, record.nextFloat(5), 0.1);
        assertEquals(11.0, record.nextDouble(6), 0.1);
        assertEquals("world", record.nextByteArray(7).toString());
        assertEquals("vchar2", record.nextByteArray(8).toString());
        assertEquals("char2", record.nextByteArray(9).toString());
        assertEquals("2016-01-01",
            format.format(record.nextTimestampNanos(10).toTimeStamp()));
        assertEquals(new BigDecimal("1234.5678900000"),
            record.nextDecimal(11).toBigDecimal());
      }

      // TODO: the Records API needs to be renamed or carefully documented.
      // Calling hasNext()/get*() mutate the objects.
      assertFalse(records.hasNext());
      records.close();
    }

    assertEquals(0, worker.numActiveTasks());
    worker.close();
  }

  /*
   * Verifies that the schema matches the nation table schema.
   */
  private void verifyNationSchema(TSchema schema, boolean parquet) {
    assertEquals(4, schema.cols.size());
    assertEquals("n_nationkey", schema.cols.get(0).name);
    assertEquals(parquet ? TTypeId.INT : TTypeId.SMALLINT,
        schema.cols.get(0).type.type_id);
    assertEquals("n_name", schema.cols.get(1).name);
    assertEquals(TTypeId.STRING, schema.cols.get(1).type.type_id);
    assertEquals("n_regionkey", schema.cols.get(2).name);
    assertEquals(parquet ? TTypeId.INT : TTypeId.SMALLINT,
        schema.cols.get(2).type.type_id);
    assertEquals("n_comment", schema.cols.get(3).name);
    assertEquals(TTypeId.STRING, schema.cols.get(3).type.type_id);
  }

  @Test
  public void testAllTypes() throws TRecordServiceException, IOException {
    // Just ask for the schema.
    TGetSchemaResult schemaResult = new RecordServicePlannerClient.Builder()
        .getSchema("localhost", PLANNER_PORT,
            Request.createTableScanRequest("rs.alltypes"));

    verifyAllTypesSchema(schemaResult.schema);

    // Plan the request
    TPlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .planRequest("localhost", PLANNER_PORT,
            Request.createSqlRequest("select * from rs.alltypes"));
    verifyAllTypes(plan);
  }

  @Test
  public void testAllTypesEmpty() throws TRecordServiceException, IOException {
    TPlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .planRequest("localhost", PLANNER_PORT,
            Request.createSqlRequest("select * from rs.alltypes_empty"));
    assertEquals(0, plan.tasks.size());
    verifyAllTypesSchema(plan.schema);
  }

  // Returns all the strings from running plan as a list. The plan must
  // have a schema that returns a single string column.
  List<String> getAllStrings(TPlanRequestResult plan)
      throws TRecordServiceException, IOException {
    List<String> results = Lists.newArrayList();
    assertEquals(1, plan.schema.cols.size());
    assertEquals(TTypeId.STRING, plan.schema.cols.get(0).type.type_id);
    for (int i = 0; i < plan.tasks.size(); ++i) {
      Records records = null;
      try {
        records = WorkerClientUtil.execTask(plan, i);
        while (records.hasNext()) {
          Records.Record record = records.next();
          results.add(record.nextByteArray(0).toString());
        }
      } finally {
        if (records != null) records.close();
      }
    }
    return results;
  }

  @Test
  public void testNationPath() throws IOException, TRecordServiceException {
    TPlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .planRequest("localhost", PLANNER_PORT,
            Request.createPathRequest("/test-warehouse/tpch.nation/"));
    assertEquals(1, plan.tasks.size());
    List<String> lines = getAllStrings(plan);
    assertEquals(25, lines.size());
    assertEquals("6|FRANCE|3|refully final requests. regular, ironi", lines.get(6));
  }

  @Test
  public void testNationPathWithSchema() throws IOException, TRecordServiceException {
    RecordServiceWorkerClient worker =
        new RecordServiceWorkerClient.Builder().connect("localhost", WORKER_PORT);

    // The normal schema is SMALLINT, STRING, SMALLINT, STRING

    // Test with an all string schema.
    TSchema schema = new TSchema();
    schema.cols = new ArrayList<TColumnDesc>();
    schema.cols.add(new TColumnDesc(new TType(TTypeId.STRING), "col1"));
    schema.cols.add(new TColumnDesc(new TType(TTypeId.STRING), "col2"));
    schema.cols.add(new TColumnDesc(new TType(TTypeId.STRING), "col3"));
    schema.cols.add(new TColumnDesc(new TType(TTypeId.STRING), "col4"));
    TPlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .planRequest("localhost", PLANNER_PORT,
            Request.createPathRequest("/test-warehouse/tpch.nation/")
                .setSchema(schema).setFieldDelimiter('|'));
    assertEquals(1, plan.tasks.size());

    // Verify schema.
    assertEquals(schema.cols.size(), plan.schema.cols.size());
    for (int i = 0; i < plan.schema.cols.size(); ++i) {
      assertEquals(schema.cols.get(i).name, plan.schema.cols.get(i).name);
      assertEquals(schema.cols.get(i).type.type_id, plan.schema.cols.get(i).type.type_id);
    }

    Records records = worker.execAndFetch(plan.tasks.get(0));
    int numRecords = 0;
    while (records.hasNext()) {
      Records.Record record = records.next();
      ++numRecords;
      if (numRecords == 1) {
        assertEquals("0", record.nextByteArray(0).toString());
        assertEquals("ALGERIA", record.nextByteArray(1).toString());
        assertEquals("0", record.nextByteArray(2).toString());
        assertEquals(" haggle. carefully final deposits detect slyly agai",
            record.nextByteArray(3).toString());
      }
    }
    assertEquals(25, numRecords);
    records.close();

    // Remap string cols to smallint, these should return null.
    schema.cols.set(1, new TColumnDesc(new TType(TTypeId.SMALLINT), "col2"));
    schema.cols.set(3, new TColumnDesc(new TType(TTypeId.SMALLINT), "col4"));

    plan = new RecordServicePlannerClient.Builder().planRequest("localhost", PLANNER_PORT,
        Request.createPathRequest("/test-warehouse/tpch.nation/")
            .setSchema(schema).setFieldDelimiter('|'));
    assertEquals(1, plan.tasks.size());

    // Verify schema.
    assertEquals(schema.cols.size(), plan.schema.cols.size());
    for (int i = 0; i < plan.schema.cols.size(); ++i) {
      assertEquals(schema.cols.get(i).name, plan.schema.cols.get(i).name);
      assertEquals(schema.cols.get(i).type.type_id, plan.schema.cols.get(i).type.type_id);
    }

    records = worker.execAndFetch(plan.tasks.get(0));
    numRecords = 0;
    while (records.hasNext()) {
      Records.Record record = records.next();
      ++numRecords;
      assertTrue(record.isNull(1));
      assertTrue(record.isNull(3));
      if (numRecords == 1) {
        assertEquals("0", record.nextByteArray(0).toString());
        assertEquals("0", record.nextByteArray(2).toString());
      }
    }
    assertEquals(25, numRecords);
    records.close();

    worker.close();
  }

  @Test
  public void testAllTypesPathWithSchema() throws IOException, TRecordServiceException {
    // Create the exact all types schema.
    TSchema schema = new TSchema();
    schema.cols = new ArrayList<TColumnDesc>();
    schema.cols.add(new TColumnDesc(new TType(TTypeId.BOOLEAN), "bool_col"));
    schema.cols.add(new TColumnDesc(new TType(TTypeId.TINYINT), "tinyint_col"));
    schema.cols.add(new TColumnDesc(new TType(TTypeId.SMALLINT), "smallint_col"));
    schema.cols.add(new TColumnDesc(new TType(TTypeId.INT), "int_col"));
    schema.cols.add(new TColumnDesc(new TType(TTypeId.BIGINT), "bigint_col"));
    schema.cols.add(new TColumnDesc(new TType(TTypeId.FLOAT), "float_col"));
    schema.cols.add(new TColumnDesc(new TType(TTypeId.DOUBLE), "double_col"));
    schema.cols.add(new TColumnDesc(new TType(TTypeId.STRING), "string_col"));
    TType varCharType = new TType(TTypeId.VARCHAR);
    varCharType.setLen(10);
    schema.cols.add(new TColumnDesc(varCharType, "varchar_col"));
    TType charType = new TType(TTypeId.CHAR);
    charType.setLen(5);
    schema.cols.add(new TColumnDesc(charType, "char_col"));
    schema.cols.add(new TColumnDesc(new TType(TTypeId.TIMESTAMP_NANOS), "timestamp_col"));
    TType decimalType = new TType(TTypeId.DECIMAL);
    decimalType.setPrecision(24);
    decimalType.setScale(10);
    schema.cols.add(new TColumnDesc(decimalType, "decimal_col"));

    TPlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .planRequest("localhost", PLANNER_PORT,
              Request.createPathRequest("/test-warehouse/rs.db/alltypes")
                  .setSchema(schema));
    verifyAllTypes(plan);
  }

  @Test
  public void testNationPathParquet() throws IOException, TRecordServiceException {
    TPlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .planRequest("localhost", PLANNER_PORT,
            Request.createPathRequest("/test-warehouse/tpch_nation_parquet/nation.parq"));
    assertEquals(1, plan.tasks.size());
    verifyNationSchema(plan.schema, true);
    fetchAndVerifyCount(WorkerClientUtil.execTask(plan, 0), 25);
  }

  @Test
  public void testPathWithLowTimeout() throws TRecordServiceException{
    boolean exceptionThrown = false;
    try {
      new RecordServicePlannerClient.Builder()
          .setTimeoutMs(2).setMaxAttempts(1)
          .planRequest("localhost", PLANNER_PORT,
              Request.createPathRequest("/test-warehouse/tpch.nation/"));
    } catch (IOException e) {
      assertTrue(e.getCause().getMessage().contains("java.net.SocketTimeoutException"));
      exceptionThrown = true;
    }
    assertTrue(exceptionThrown);

    exceptionThrown = false;
    try {
      new RecordServicePlannerClient.Builder()
          .setTimeoutMs(2).setMaxAttempts(1)
          .planRequest("localhost", PLANNER_PORT,
              Request.createPathRequest(
                  "/test-warehouse/tpch_nation_parquet/nation.parq"));
    } catch (IOException e) {
      assertTrue(e.getCause().getMessage().contains("java.net.SocketTimeoutException"));
      exceptionThrown = true;
    }
    assertTrue(exceptionThrown);
  }

  void testNationPathGlobbing(String path, boolean expectMatch)
      throws IOException, TRecordServiceException {
    try {
      // TODO: figure out why it timeout with the default timeout - 20 secs
      TPlanRequestResult plan = new RecordServicePlannerClient.Builder()
          .setTimeoutMs(0)
          .planRequest("localhost", PLANNER_PORT, Request.createPathRequest(path));
      assertEquals(expectMatch ? 1 : 0, plan.tasks.size());
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
    TPlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .planRequest("localhost", PLANNER_PORT,
            Request.createPathRequest("/test-warehouse/tpch.nation/")
                .setQuery("select * from __PATH__ where record like '6|FRANCE%'"));
    assertEquals(1, plan.tasks.size());
    List<String> lines = getAllStrings(plan);
    assertEquals(1, lines.size());
    assertEquals("6|FRANCE|3|refully final requests. regular, ironi", lines.get(0));
  }

  @Test
  public void testNationView() throws IOException, TRecordServiceException {
    TPlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .planRequest("localhost", PLANNER_PORT,
            Request.createTableScanRequest("rs.nation_projection"));
    assertEquals(1, plan.tasks.size());
    assertEquals(2, plan.schema.cols.size());
    assertEquals("n_nationkey", plan.schema.cols.get(0).name);
    assertEquals("n_name", plan.schema.cols.get(1).name);

    for (int i = 0; i < plan.tasks.size(); ++i) {
      Records records = WorkerClientUtil.execTask(plan, i);
      int numRecords = 0;
      while (records.hasNext()) {
        Records.Record record = records.next();
        ++numRecords;
        switch (numRecords) {
        case 1:
          assertEquals(0, record.nextShort(0));
          assertEquals("ALGERIA", record.nextByteArray(1).toString());
          break;
        case 2:
          assertEquals(1, record.nextShort(0));
          assertEquals("ARGENTINA", record.nextByteArray(1).toString());
          break;
        case 3:
          assertEquals(2, record.nextShort(0));
          assertEquals("BRAZIL", record.nextByteArray(1).toString());
          break;
        case 4:
          assertEquals(3, record.nextShort(0));
          assertEquals("CANADA", record.nextByteArray(1).toString());
          break;
        case 5:
          assertEquals(4, record.nextShort(0));
          assertEquals("EGYPT", record.nextByteArray(1).toString());
          break;
        }
      }
      records.close();
      assertEquals(5, numRecords);
    }
  }

  @Test
  public void testFetchSize() throws IOException, TRecordServiceException {
    TPlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .planRequest("localhost", PLANNER_PORT,
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
    assertEquals(25, numRecords);
    worker.close();
  }

  @Test
  public void testMemLimitExceeded() throws IOException, TRecordServiceException {
    TPlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .planRequest("localhost", PLANNER_PORT,
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
      assertEquals(TErrorCode.OUT_OF_MEMORY, e.code);
    }
    assertTrue(exceptionThrown);
    worker.closeTask(handle);

    // Try again going through the utility.
    exceptionThrown = false;
    try {
      worker.execAndFetch(plan.tasks.get(0));
    } catch (TRecordServiceException e) {
      exceptionThrown = true;
      assertEquals(TErrorCode.OUT_OF_MEMORY, e.code);
    }
    assertTrue(exceptionThrown);

    assertEquals(0, worker.numActiveTasks());
    worker.close();
  }

  @Test
  public void testEmptyProjection() throws IOException, TRecordServiceException {
    TPlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .planRequest("localhost", PLANNER_PORT,
            Request.createProjectionRequest("tpch.nation", null));
    assertEquals(1, plan.tasks.size());

    // Verify schema
    assertEquals(1, plan.schema.cols.size());
    assertEquals("count(*)", plan.schema.cols.get(0).name);
    assertEquals(TTypeId.BIGINT, plan.schema.cols.get(0).type.type_id);

    // Verify count(*) result.
    Records records = WorkerClientUtil.execTask(plan, 0);
    assertTrue(records.hasNext());
    Records.Record result = records.next();
    assertEquals(25, result.nextLong(0));
    assertFalse(records.hasNext());
    records.close();

    // Empty column list should do the same
    plan = new RecordServicePlannerClient.Builder()
        .planRequest("localhost", PLANNER_PORT,
            Request.createProjectionRequest("tpch.nation", new ArrayList<String>()));
    assertEquals(1, plan.tasks.size());

    plan = new RecordServicePlannerClient.Builder()
        .planRequest("localhost", PLANNER_PORT,
            Request.createSqlRequest("select count(*), count(*) from tpch.nation"));

    assertEquals(1, plan.tasks.size());
    assertEquals(2, plan.schema.cols.size());
    assertEquals("count(*)", plan.schema.cols.get(0).name);
    assertEquals("count(*)", plan.schema.cols.get(1).name);
    assertEquals(TTypeId.BIGINT, plan.schema.cols.get(0).type.type_id);
    assertEquals(TTypeId.BIGINT, plan.schema.cols.get(1).type.type_id);
  }

  @Test
  public void testProjection() throws IOException, TRecordServiceException {
    TPlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .planRequest("localhost", PLANNER_PORT, Request.createProjectionRequest(
            "tpch.nation", Lists.newArrayList("n_comment")));
    assertEquals(1, plan.tasks.size());
    assertEquals(1, plan.schema.cols.size());
    assertEquals("n_comment", plan.schema.cols.get(0).name);
    assertEquals(TTypeId.STRING, plan.schema.cols.get(0).type.type_id);
  }

  @Test
  public void testLimit() throws IOException, TRecordServiceException {
    TPlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .planRequest("localhost", PLANNER_PORT,
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
  public void testServerLoggingLevels() throws IOException, TRecordServiceException {
    TPlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .planRequest("localhost", PLANNER_PORT,
            Request.createTableScanRequest("tpch.nation"));
    assertEquals(1, plan.tasks.size());
    TNetworkAddress addr = plan.tasks.get(0).local_hosts.get(0);

    RecordServiceWorkerClient worker = new RecordServiceWorkerClient.Builder()
        .setLoggingLevel(TLoggingLevel.ALL)
        .connect(addr.hostname, addr.port);
    fetchAndVerifyCount(worker.execAndFetch(plan.tasks.get(0)), 25);
    worker.close();

    worker = new RecordServiceWorkerClient.Builder()
        .setLoggingLevel(LoggerFactory.getLogger(TestBasicClient.class))
        .connect(addr.hostname, addr.port);
    fetchAndVerifyCount(worker.execAndFetch(plan.tasks.get(0)), 25);
    worker.close();

    worker = new RecordServiceWorkerClient.Builder()
        .setLoggingLevel((Logger)null)
        .connect(addr.hostname, addr.port);
    fetchAndVerifyCount(worker.execAndFetch(plan.tasks.get(0)), 25);
    worker.close();
  }

  @Test
  public void testNonLocalWorker() throws IOException, TRecordServiceException {
    TPlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .planRequest("localhost", PLANNER_PORT,
            Request.createTableScanRequest("tpch.nation"));
    assertEquals(1, plan.tasks.size());

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
