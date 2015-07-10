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

package com.cloudera.recordservice.avro.example;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import com.cloudera.recordservice.avro.GenericRecords;
import com.cloudera.recordservice.avro.SchemaUtils;
import com.cloudera.recordservice.core.RecordServicePlannerClient;
import com.cloudera.recordservice.core.Request;
import com.cloudera.recordservice.core.WorkerClientUtil;
import com.cloudera.recordservice.thrift.TPlanRequestResult;
import com.cloudera.recordservice.thrift.TRecordServiceException;

/**
 * Example utility that converts results returned from the RecordService
 * as avro, output as json.
 */
public class RecordServiceToAvro {
  static final int PLANNER_PORT = 40000;

  public static void main(String[] args) throws TRecordServiceException, IOException {
    String query = "select * from tpch.nation";
    if (args.length == 2) query = args[1];

    TPlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .planRequest("localhost", PLANNER_PORT, Request.createSqlRequest(query));
    Schema avroSchema = SchemaUtils.convertSchema(plan.schema);
    System.out.println("Avro Schema:\n" + avroSchema);

    System.out.println("Records:");
    for (int t = 0; t < plan.tasks.size(); ++t) {
      GenericRecords records = null;
      try {
        records = new GenericRecords(WorkerClientUtil.execTask(plan, t));
        while (records.hasNext()) {
          GenericData.Record record = records.next();
          System.out.println(record);
        }
      } finally {
        if (records != null) records.close();
      }
    }
  }
}
