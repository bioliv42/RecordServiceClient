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

#include <stdio.h>
#include <exception>

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>

#include <gflags/gflags.h>

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

DEFINE_string(planner_host, "localhost", "The host running the planner service.");
const int RECORD_SERVICE_PLANNER_PORT = 40000;

void PlanRequest(const char* request) {
  printf("Planning request: %s\n", request);

  shared_ptr<TTransport> planner_socket(
      new TSocket(FLAGS_planner_host, RECORD_SERVICE_PLANNER_PORT));
  shared_ptr<TTransport> planner_transport(new TBufferedTransport(planner_socket));
  shared_ptr<TProtocol> planner_protocol(new TBinaryProtocol(planner_transport));

  RecordServicePlannerClient planner(planner_protocol);
  TPlanRequestResult plan_result;
  try {
    planner_transport->open();
    TPlanRequestParams plan_params;
    plan_params.request_type = TRequestType::Sql;
    plan_params.__set_sql_stmt(request);
    planner.PlanRequest(plan_result, plan_params);
  } catch (const TRecordServiceException& e) {
    printf("Failed with exception:\n%s\n", e.message.c_str());
    return;
  } catch (const std::exception& e) {
    printf("Failed with exception:\n%s\n", e.what());
    return;
  }

  int64_t total_task_size = 0;
  size_t largest_task_size = 0;
  printf("Done planning. Generated %ld tasks.\n", plan_result.tasks.size());
  for (int i = 0; i < plan_result.tasks.size(); ++i) {
    total_task_size += plan_result.tasks[i].task.size();
    largest_task_size = std::max(largest_task_size, plan_result.tasks[i].task.size());
  }
  printf("Total task size: %ld\n", total_task_size);
  printf("Largest task size: %ld\n", largest_task_size);

  if (plan_result.warnings.size() > 0) {
    printf("Plan generated %ld warnings.", plan_result.warnings.size());
    for (int i = 0; i < plan_result.warnings.size(); ++i) {
      printf("%s: %d\n", plan_result.warnings[i].message.c_str(),
          plan_result.warnings[i].count);
    }
  }
}

// Utility application that plans a request. Does not run the request.
int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  if (argc < 2) {
    printf("usage: plan-request <sql stmt>\n");
    return 1;
  }
  PlanRequest(argv[1]);
  return 0;
}
