// Confidential Cloudera Information: Covered by NDA.
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

#ifndef RECORD_SERVICE_TEST_COMMON_H
#define RECORD_SERVICE_TEST_COMMON_H

#include <vector>
#include <string>

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>

#include "gen-cpp/RecordServicePlanner.h"
#include "gen-cpp/RecordServiceWorker.h"

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

namespace recordservice {

inline boost::shared_ptr<RecordServicePlannerClient> CreatePlannerConnection(
    const char* planner_host, int planner_port) {
  boost::shared_ptr<TTransport> planner_socket(
      new TSocket(planner_host, planner_port));
  boost::shared_ptr<TTransport> planner_transport(new TBufferedTransport(planner_socket));
  boost::shared_ptr<TProtocol> planner_protocol(new TBinaryProtocol(planner_transport));
  boost::shared_ptr<RecordServicePlannerClient> client(
      new RecordServicePlannerClient(planner_protocol));
  planner_transport->open();
  return client;
}

inline boost::shared_ptr<RecordServiceWorkerClient> CreateWorkerConnection(
    const char* worker_host, int worker_port) {
  boost::shared_ptr<TTransport> worker_socket(
      new TSocket(worker_host, worker_port));
  boost::shared_ptr<TTransport> worker_transport(new TBufferedTransport(worker_socket));
  boost::shared_ptr<TProtocol> worker_protocol(new TBinaryProtocol(worker_transport));
  worker_transport->open();
  boost::shared_ptr<RecordServiceWorkerClient> worker(
      new RecordServiceWorkerClient(worker_protocol));
  return worker;
}

inline std::vector<std::string> FetchAllStrings(
    RecordServiceWorkerClient* worker, const std::string& task) {
  TExecTaskResult exec_result;
  TExecTaskParams exec_params;
  exec_params.task = task;
  worker->ExecTask(exec_result, exec_params);

  TFetchResult fetch_result;
  TFetchParams fetch_params;
  fetch_params.handle = exec_result.handle;

  std::vector<std::string> results;
  do {
    worker->Fetch(fetch_result, fetch_params);
    EXPECT_EQ(fetch_result.columnar_records.cols.size(), 1);
    const char* data = fetch_result.columnar_records.cols[0].data.data();
    for (int i = 0; i < fetch_result.num_records; ++i) {
      int len = *reinterpret_cast<const int*>(data);
      data += sizeof(int);
      results.push_back(std::string(data, len));
      data += len;
    }
  } while (!fetch_result.done);
  worker->CloseTask(exec_result.handle);
  return results;
}


}

#endif
