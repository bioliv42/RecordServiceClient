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
#include "gen-cpp/Types_types.h"

#include "util/stopwatch.h"
#include "util/time.h"

using namespace boost;
using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

using namespace recordservice;

const char* HOST = "localhost";
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

TEST(ClientTest, Nation) {
  shared_ptr<TTransport> planner_socket(
      new TSocket("localhost", RECORD_SERVICE_PLANNER_PORT));
  shared_ptr<TTransport> planner_transport(new TBufferedTransport(planner_socket));
  shared_ptr<TProtocol> planner_protocol(new TBinaryProtocol(planner_transport));

  RecordServicePlannerClient planner(planner_protocol);
  TPlanRequestResult plan_result;

  planner_transport->open();
  TPlanRequestParams plan_params;
  plan_params.sql_stmt = "select * from tpch.nation";
  planner.PlanRequest(plan_result, plan_params);


  EXPECT_EQ(plan_result.schema.cols.size(), 4);
  EXPECT_EQ(plan_result.schema.cols[0].type.type_id, TTypeId::SMALLINT);
  EXPECT_EQ(plan_result.schema.cols[1].type.type_id, TTypeId::STRING);
  EXPECT_EQ(plan_result.schema.cols[2].type.type_id, TTypeId::SMALLINT);
  EXPECT_EQ(plan_result.schema.cols[3].type.type_id, TTypeId::STRING);

  EXPECT_EQ(plan_result.tasks.size(), 1);
  shared_ptr<TTransport> worker_socket(
      new TSocket(plan_result.tasks[0].hosts[0], RECORD_SERVICE_WORKER_PORT));
  shared_ptr<TTransport> worker_transport(new TBufferedTransport(worker_socket));
  shared_ptr<TProtocol> worker_protocol(new TBinaryProtocol(worker_transport));

  RecordServiceWorkerClient worker(worker_protocol);
  int worker_rows = 0;
  worker_transport->open();

  TExecTaskResult exec_result;
  TExecTaskParams exec_params;
  exec_params.task = plan_result.tasks[0].task;
  worker.ExecTask(exec_result, exec_params);

  TFetchResult fetch_result;
  TFetchParams fetch_params;
  fetch_params.handle = exec_result.handle;

  do {
    worker.Fetch(fetch_result, fetch_params);
    if (worker_rows == 0) {
      // Verify the first row. TODO: verify them all.
      EXPECT_EQ(NATION_TBL[0].c0, *reinterpret_cast<const int16_t*>(
          fetch_result.columnar_row_batch.cols[0].data.data()));

      EXPECT_EQ(NATION_TBL[0].c1.size(), *reinterpret_cast<const int32_t*>(
          fetch_result.columnar_row_batch.cols[1].data.data()));
      EXPECT_EQ(NATION_TBL[0].c1, string(
          fetch_result.columnar_row_batch.cols[1].data.data() + sizeof(int32_t),
          NATION_TBL[0].c1.size()));

      EXPECT_EQ(NATION_TBL[0].c2, *reinterpret_cast<const int16_t*>(
          fetch_result.columnar_row_batch.cols[2].data.data()));

      EXPECT_EQ(NATION_TBL[0].c3.size(), *reinterpret_cast<const int32_t*>(
          fetch_result.columnar_row_batch.cols[3].data.data()));
      EXPECT_EQ(NATION_TBL[0].c3, string(
          fetch_result.columnar_row_batch.cols[3].data.data() + sizeof(int32_t),
          NATION_TBL[0].c3.size()));
    }
    worker_rows += fetch_result.num_rows;
  } while (!fetch_result.done);

  EXPECT_EQ(worker_rows, 25);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
