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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.thrift.TException;
import org.junit.Test;

import com.cloudera.recordservice.avro.SpecificRecords.ResolveBy;
import com.cloudera.recordservice.avro.nation.NationAll;
import com.cloudera.recordservice.avro.nation.NationKeyName;
import com.cloudera.recordservice.client.RecordServicePlannerClient;
import com.cloudera.recordservice.client.RecordServiceWorkerClient;
import com.cloudera.recordservice.client.Rows;
import com.cloudera.recordservice.thrift.TPlanRequestResult;

public class TestSpecificRecord {
  static final int PLANNER_PORT = 40000;
  static final int WORKER_PORT = 40100;

  Rows execAndFetch(ByteBuffer task) throws TException, IOException {
    RecordServiceWorkerClient worker = new RecordServiceWorkerClient();
    worker.connect("localhost", WORKER_PORT);
    return worker.execAndFetch(task);
  }

  @Test
  public void testNationAll() throws TException, IOException {
    TPlanRequestResult plan = RecordServicePlannerClient.planRequest(
        "localhost", PLANNER_PORT, "select * from tpch.nation");

    assertEquals(plan.tasks.size(), 1);
    SpecificRecords<NationAll> records = new SpecificRecords<NationAll>(
        NationAll.class, execAndFetch(plan.tasks.get(0).task),
        ResolveBy.ORDINAL);

    int numRecords = 0;
    while (records.hasNext()) {
      NationAll record = records.next();
      ++numRecords;
      if (numRecords == 3) {
        assertEquals(record.getKey().intValue(), 2);
        assertEquals(record.getName(), "BRAZIL");
        assertEquals(record.getRegionKey().intValue(), 1);
        assertEquals(record.getComment(), "y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special ");
      }
    }
    records.close();
    assertEquals(numRecords, 25);
  }

  @Test
  public void testNationProjection() throws TException, IOException {
    TPlanRequestResult plan = RecordServicePlannerClient.planRequest(
        "localhost", PLANNER_PORT, "select n_nationkey, n_name from tpch.nation");

    assertEquals(plan.tasks.size(), 1);
    SpecificRecords<NationKeyName> records = new SpecificRecords<NationKeyName>(
        NationKeyName.class, execAndFetch(plan.tasks.get(0).task),
        ResolveBy.NAME);

    int numRecords = 0;
    while (records.hasNext()) {
      NationKeyName record = records.next();
      ++numRecords;
      if (numRecords == 4) {
        assertEquals(record.getNName(), "CANADA");
        assertEquals(record.getNNationkey().intValue(), 3);
      }
    }
    records.close();
    assertEquals(numRecords, 25);
  }

}