// Copyright 2012 Cloudera Inc.
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

#include <gtest/gtest.h>
#include <stdio.h>
#include <exception>

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>

#include "gen-cpp/RecordServicePlanner.h"
#include "gen-cpp/RecordServiceWorker.h"

using namespace boost;
using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

using namespace recordservice;

const char* PLANNER_HOST = "localhost";
const int RECORD_SERVICE_PLANNER_PORT = 40000;
const int RECORD_SERVICE_WORKER_PORT = 40100;

static struct {
  int16_t c0;
  string c1;
  int16_t c2;
  string c3;
} NATION_TBL[] = {
  {0, "ALGERIA", 0, " haggle. carefully final deposits detect slyly agai" },
};

shared_ptr<RecordServicePlannerClient> CreatePlannerConnection() {
  shared_ptr<TTransport> planner_socket(
      new TSocket(PLANNER_HOST, RECORD_SERVICE_PLANNER_PORT));
  shared_ptr<TTransport> planner_transport(new TBufferedTransport(planner_socket));
  shared_ptr<TProtocol> planner_protocol(new TBinaryProtocol(planner_transport));
  shared_ptr<RecordServicePlannerClient> client(
      new RecordServicePlannerClient(planner_protocol));
  planner_transport->open();
  return client;
}

shared_ptr<RecordServiceWorkerClient> CreateWorkerConnection() {
  shared_ptr<TTransport> worker_socket(
      new TSocket("localhost", RECORD_SERVICE_WORKER_PORT));
  shared_ptr<TTransport> worker_transport(new TBufferedTransport(worker_socket));
  shared_ptr<TProtocol> worker_protocol(new TBinaryProtocol(worker_transport));
  worker_transport->open();
  shared_ptr<RecordServiceWorkerClient> worker(
      new RecordServiceWorkerClient(worker_protocol));
  return worker;
}

TEST(ClientTest, Nation) {
  try {
    shared_ptr<RecordServicePlannerClient> planner = CreatePlannerConnection();
    shared_ptr<RecordServiceWorkerClient> worker = CreateWorkerConnection();

    TPlanRequestResult plan_result;
    TPlanRequestParams plan_params;
    plan_params.request_type = TRequestType::Sql;
    plan_params.__set_sql_stmt("select * from tpch.nation");
    planner->PlanRequest(plan_result, plan_params);

    EXPECT_EQ(plan_result.schema.cols.size(), 4);
    EXPECT_EQ(plan_result.schema.cols[0].type.type_id, TTypeId::SMALLINT);
    EXPECT_EQ(plan_result.schema.cols[1].type.type_id, TTypeId::STRING);
    EXPECT_EQ(plan_result.schema.cols[2].type.type_id, TTypeId::SMALLINT);
    EXPECT_EQ(plan_result.schema.cols[3].type.type_id, TTypeId::STRING);

    EXPECT_EQ(plan_result.tasks.size(), 1);
    EXPECT_EQ(plan_result.tasks[0].local_hosts.size(), 3) << "Expecting 3x replication";
    int worker_records = 0;

    TExecTaskResult exec_result;
    TExecTaskParams exec_params;
    exec_params.task = plan_result.tasks[0].task;
    worker->ExecTask(exec_result, exec_params);

    TFetchResult fetch_result;
    TFetchParams fetch_params;
    fetch_params.handle = exec_result.handle;

    do {
      worker->Fetch(fetch_result, fetch_params);
      if (worker_records == 0) {
        // Verify the first row. TODO: verify them all.
        EXPECT_EQ(NATION_TBL[0].c0, *reinterpret_cast<const int16_t*>(
            fetch_result.columnar_records.cols[0].data.data()));

        EXPECT_EQ(NATION_TBL[0].c1.size(), *reinterpret_cast<const int32_t*>(
            fetch_result.columnar_records.cols[1].data.data()));
        EXPECT_EQ(NATION_TBL[0].c1, string(
            fetch_result.columnar_records.cols[1].data.data() + sizeof(int32_t),
            NATION_TBL[0].c1.size()));

        EXPECT_EQ(NATION_TBL[0].c2, *reinterpret_cast<const int16_t*>(
            fetch_result.columnar_records.cols[2].data.data()));

        EXPECT_EQ(NATION_TBL[0].c3.size(), *reinterpret_cast<const int32_t*>(
            fetch_result.columnar_records.cols[3].data.data()));
        EXPECT_EQ(NATION_TBL[0].c3, string(
            fetch_result.columnar_records.cols[3].data.data() + sizeof(int32_t),
            NATION_TBL[0].c3.size()));
      }
      worker_records += fetch_result.num_records;
    } while (!fetch_result.done);

    EXPECT_EQ(worker_records, 25);
    worker->CloseTask(exec_result.handle);
  } catch (const TRecordServiceException& e) {
    EXPECT_TRUE(false) << "Error: " << e.message << " " << e.detail;
  } catch (const TException& e) {
    EXPECT_TRUE(false) << "Error: " << e.what();
  }
}

vector<string> FetchAllStrings(RecordServiceWorkerClient* worker, const string& task) {
  TExecTaskResult exec_result;
  TExecTaskParams exec_params;
  exec_params.task = task;
  worker->ExecTask(exec_result, exec_params);

  TFetchResult fetch_result;
  TFetchParams fetch_params;
  fetch_params.handle = exec_result.handle;

  vector<string> results;
  do {
    worker->Fetch(fetch_result, fetch_params);
    EXPECT_EQ(fetch_result.columnar_records.cols.size(), 1);
    const char* data = fetch_result.columnar_records.cols[0].data.data();
    for (int i = 0; i < fetch_result.num_records; ++i) {
      int len = *reinterpret_cast<const int*>(data);
      data += sizeof(int);
      results.push_back(string(data, len));
      data += len;
    }
  } while (!fetch_result.done);
  worker->CloseTask(exec_result.handle);
  return results;
}

TEST(ClientTest, NationFile) {
  try {
    shared_ptr<RecordServicePlannerClient> planner = CreatePlannerConnection();
    shared_ptr<RecordServiceWorkerClient> worker = CreateWorkerConnection();

    TPlanRequestResult plan_result;
    TPlanRequestParams plan_params;
    plan_params.request_type = TRequestType::Path;
    plan_params.__isset.path = true;
    plan_params.path.path = "/test-warehouse/tpch.nation/";
    planner->PlanRequest(plan_result, plan_params);

    EXPECT_EQ(plan_result.schema.cols.size(), 1);
    EXPECT_EQ(plan_result.schema.cols[0].type.type_id, TTypeId::STRING);
    EXPECT_EQ(plan_result.schema.cols[0].name, "record");

    EXPECT_EQ(plan_result.tasks.size(), 1);
    vector<string> data = FetchAllStrings(worker.get(), plan_result.tasks[0].task);

    // Verify a few records
    EXPECT_EQ(data[5], "5|ETHIOPIA|0|ven packages wake quickly. regu");
    EXPECT_EQ(data[21], "21|VIETNAM|2|hely enticingly express accounts. even, final ");
  } catch (const TRecordServiceException& e) {
    EXPECT_TRUE(false) << "Error: " << e.message;
  } catch (const TException& e) {
    EXPECT_TRUE(false) << "Error: " << e.what();
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
