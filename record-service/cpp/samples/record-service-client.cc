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

// If true, runs the aggregation portion of "select sum(l_orderkey)" logic.
#define QUERY_1 1

// Comparator for THostPorts. Thrift declares this (in gen-cpp/Types_types.h) but
// never defines it.
bool impala::TNetworkAddress::operator<(const impala::TNetworkAddress& that) const {
  if (this->hostname < that.hostname) {
    return true;
  } else if ((this->hostname == that.hostname) && (this->port < that.port)) {
    return true;
  }
  return false;
}

string PrintResultSchema(const TSchema& schema) {
  stringstream ss;
  ss << "[";
  for (int i = 0; i < schema.cols.size(); ++i) {
    if (i != 0) ss << ", ";
    switch (schema.cols[i].type_id) {
      case TTypeId::BOOLEAN:
        ss << "BOOLEAN";
        break;
      case TTypeId::TINYINT:
        ss << "SMALLINT";
        break;
      case TTypeId::SMALLINT:
        ss << "SMALLINT";
        break;
      case TTypeId::INT:
        ss << "INT";
        break;
      case TTypeId::BIGINT:
        ss << "BIGINT";
        break;
      case TTypeId::FLOAT:
        ss << "FLOAT";
        break;
      case TTypeId::DOUBLE:
        ss << "DOUBLE";
        break;
      case TTypeId::STRING:
        ss << "STRING";
        break;
      case TTypeId::TIMESTAMP:
        ss << "TIMESTAMP";
        break;
      case TTypeId::DECIMAL:
        ss << "DECIMAL(" << schema.cols[i].precision << ", "
           << schema.cols[i].scale << ")";
        break;
      default:
        ss << "Unknown";
    }
  }
  ss << "]" << endl;
  return ss.str();
}


void ExecRequestDistributed(const char* request, TRowBatchFormat::type format) {
  printf("Planning request: %s\n", request);

  shared_ptr<TTransport> planner_socket(
      new TSocket("localhost", RECORD_SERVICE_PLANNER_PORT));
  shared_ptr<TTransport> planner_transport(new TBufferedTransport(planner_socket));
  shared_ptr<TProtocol> planner_protocol(new TBinaryProtocol(planner_transport));

  RecordServicePlannerClient planner(planner_protocol);
  TPlanRequestResult plan_result;
  try {
    planner_transport->open();
    TPlanRequestParams plan_params;
    plan_params.sql_stmt = request;
    planner.PlanRequest(plan_result, plan_params);
  } catch (const RecordServiceException& e) {
    printf("Failed with exception:\n%s\n", e.message.c_str());
    return;
  } catch (const std::exception& e) {
    printf("Failed with exception:\n%s\n", e.what());
    return;
  }

  printf("Done planning. Generated %ld tasks.\n", plan_result.tasks.size());
  printf("Tasks:\n");
  for (int i = 0; i < plan_result.tasks.size(); ++i) {
    // TODO: generally, this task is not printable.
    printf("  %d: %s\n", i + 1, plan_result.tasks[i].task.c_str());
    for (int j = 0; j < plan_result.tasks[i].hosts.size(); ++j) {
      printf("     %s\n", plan_result.tasks[i].hosts[j].c_str());
    }
  }

  printf("Result Types: %s\n", PrintResultSchema(plan_result.schema).c_str());

  printf("\nExecuting tasks...\n");
  int64_t total_rows = 0;
  int64_t start_time = impala::ms_since_epoch();
  TStats total_stats;
  impala::MonotonicStopWatch task_time;

#if QUERY_1
  int64_t q1_result = 0;
#endif

  for (int i = 0; i < plan_result.tasks.size(); ++i) {
    const TTask& task = plan_result.tasks[i];
    // Run each task on the first host it reported
    // TODO: since to worker port and interface
    shared_ptr<TTransport> socket(new TSocket(task.hosts[0], RECORD_SERVICE_WORKER_PORT));
    shared_ptr<TTransport> transport(new TBufferedTransport(socket));
    shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));

    RecordServiceWorkerClient worker(protocol);
    int worker_rows = 0;
    try {
      transport->open();

      TExecTaskResult exec_result;
      TExecTaskParams exec_params;
      exec_params.task = task.task;
      exec_params.__set_row_batch_format(format);
      worker.ExecTask(exec_result, exec_params);

      TFetchResult fetch_result;
      TFetchParams fetch_params;
      fetch_params.handle = exec_result.handle;

      do {
        worker.Fetch(fetch_result, fetch_params);
        task_time.Start();
        worker_rows += fetch_result.num_rows;

#if QUERY_1
        const int64_t* values = NULL;
        const uint8_t* nulls = NULL;
        if (fetch_result.row_batch_format == TRowBatchFormat::ColumnarThrift) {
          values = &fetch_result.row_batch.cols[0].long_vals[0];
          nulls = (const uint8_t*) fetch_result.row_batch.cols[0].is_null.data();
        } else if (fetch_result.row_batch_format == TRowBatchFormat::Parquet) {
          values = (const int64_t*)fetch_result.parquet_row_batch.cols[0].data.data();
          nulls = (const uint8_t*)fetch_result.parquet_row_batch.cols[0].is_null.data();
        } else {
          printf("Unknown row batch format.\n");
          return;
        }

        int idx = 0;
        for (int n = 0; n < fetch_result.num_rows; ++n) {
          if (nulls[n]) continue;
          q1_result += values[idx++];
        }
#endif
        task_time.Stop();
      } while (!fetch_result.done);

      TStats stats;
      worker.GetTaskStats(stats, exec_result.handle);

      total_stats.serialize_time_ms += stats.serialize_time_ms;
      total_stats.client_time_ms += stats.client_time_ms;
      total_stats.bytes_read += stats.bytes_read;
      total_stats.bytes_read_local += stats.bytes_read_local;

      worker.CloseTask(exec_result.handle);

      printf("Worker %d returned %d rows\n", i + 1, worker_rows);
      total_rows += worker_rows;
    } catch (const RecordServiceException& e) {
      printf("Failed with exception:\n%s\n", e.message.c_str());
      return;
    } catch (const std::exception& e) {
      printf("Failed with exception:\n%s\n", e.what());
      return;
    }
  }

  int64_t task_time_ms = task_time.ElapsedTime() / 1000000;
  int64_t end_time = impala::ms_since_epoch();
  double duration_ms = end_time - start_time;
  printf("Fetched %ld rows in %fms.\n", total_rows, duration_ms);
  if (duration_ms != 0) {
    printf("Millions of Rows / second: %f\n", total_rows / 1000 / duration_ms);
    printf("  SerializeTime: %ld ms (%0.2f%%)\n",
        total_stats.serialize_time_ms, total_stats.serialize_time_ms / duration_ms * 100);
    printf("  TotalTaskTime: %ld ms (%0.2f%%)\n",
        total_stats.client_time_ms, total_stats.client_time_ms / duration_ms * 100);
    printf("  TaskProcessTime: %ld ms (%0.2f%%)\n",
        task_time_ms, task_time_ms / duration_ms * 100);
  }
  printf("  BytesRead: %0.2f mb\n", total_stats.bytes_read / (1024. * 1024.));
  printf("  BytesReadLocal: %0.2f mb\n", total_stats.bytes_read_local / (1024. * 1024.));

#if QUERY_1
  printf("Query 1 Result: %ld\n", q1_result);
#endif
}

int main(int argc, char** argv) {
  if (argc < 2) {
    printf("usage: record-service-client <sql stmt>\n");
    return 1;
  }
  ExecRequestDistributed(argv[1], TRowBatchFormat::Parquet);
  return 0;
}
