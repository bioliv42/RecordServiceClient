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

import java.io.IOException;

import org.junit.Test;

import com.cloudera.recordservice.avro.SpecificRecords.ResolveBy;
import com.cloudera.recordservice.avro.nation.NationAll;
import com.cloudera.recordservice.avro.nation.NationKeyName;
import com.cloudera.recordservice.core.RecordServicePlannerClient;
import com.cloudera.recordservice.core.Request;
import com.cloudera.recordservice.core.TestBase;
import com.cloudera.recordservice.core.WorkerClientUtil;
import com.cloudera.recordservice.thrift.TPlanRequestResult;
import com.cloudera.recordservice.thrift.TRecordServiceException;

public class TestSpecificRecord extends TestBase {
  static final int PLANNER_PORT = 40000;

  @Test
  public void testNationAll() throws TRecordServiceException, IOException {
    TPlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .planRequest("localhost", PLANNER_PORT,
            Request.createSqlRequest("select * from tpch.nation"));

    assertEquals(plan.tasks.size(), 1);
    SpecificRecords<NationAll> records = null;
    try {
      records = new SpecificRecords<NationAll>(NationAll.SCHEMA$,
          WorkerClientUtil.execTask(plan, 0), ResolveBy.ORDINAL);
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
      assertEquals(numRecords, 25);
    } finally {
      if (records != null) records.close();
    }
  }

  @Test
  public void testNationProjection() throws TRecordServiceException, IOException {
    TPlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .planRequest("localhost", PLANNER_PORT,
            Request.createSqlRequest("select n_nationkey, n_name from tpch.nation"));

    assertEquals(plan.tasks.size(), 1);
    SpecificRecords<NationKeyName> records = null;
    try {
      records = new SpecificRecords<NationKeyName>(NationKeyName.SCHEMA$,
          WorkerClientUtil.execTask(plan, 0), ResolveBy.NAME);
      int numRecords = 0;
      while (records.hasNext()) {
        NationKeyName record = records.next();
        ++numRecords;
        if (numRecords == 4) {
          assertEquals(record.getNName(), "CANADA");
          assertEquals(record.getNNationkey().intValue(), 3);
        }
      }
      assertEquals(numRecords, 25);
    } finally {
      if (records != null) records.close();
    }
  }
}
