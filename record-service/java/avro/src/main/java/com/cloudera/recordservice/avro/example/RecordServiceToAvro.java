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
import org.apache.thrift.TException;

import com.cloudera.recordservice.avro.GenericRecords;
import com.cloudera.recordservice.avro.SchemaUtils;
import com.cloudera.recordservice.client.RecordServicePlannerClient;
import com.cloudera.recordservice.client.RecordServiceWorkerClient;
import com.cloudera.recordservice.client.Rows;
import com.cloudera.recordservice.thrift.TPlanRequestResult;

/**
 * Example utility that converts results returned from the RecordService
 * as avro, output as json.
 */
public class RecordServiceToAvro {
  static final int PLANNER_PORT = 40000;
  static final int WORKER_PORT = 40100;

  public static void main(String[] args) throws TException, IOException {
    String query = "select * from tpch.nation";
    if (args.length == 2) query = args[1];

    RecordServicePlannerClient planner = new RecordServicePlannerClient();
    planner.connect("localhost", PLANNER_PORT);
    RecordServiceWorkerClient worker = new RecordServiceWorkerClient();
    worker.connect("localhost", WORKER_PORT);

    TPlanRequestResult plan = planner.planRequest(query);
    Schema avroSchema = SchemaUtils.convertSchema(plan.schema);
    System.out.println("Avro Schema:\n" + avroSchema);

    System.out.println("Records:");
    for (int t = 0; t < plan.tasks.size(); ++t) {
      Rows rows = worker.execAndFetch(plan.tasks.get(t).task);
      GenericRecords records = new GenericRecords(rows);
      while (records.hasNext()) {
        GenericData.Record record = records.next();
        System.out.println(record);
      }
      rows.close();
    }

    planner.close();
    worker.close();
  }
}
