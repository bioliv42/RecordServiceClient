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

const char* PLANNER_HOST = "localhost";
const int RECORD_SERVICE_PLANNER_PORT = 40000;

// If true, runs the aggregation portion of "select sum(l_orderkey)" logic.
#define QUERY_1 1

// "select min(l_comment")
#define QUERY_2 0

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
    switch (schema.cols[i].type.type_id) {
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
      case TTypeId::TIMESTAMP_NANOS:
        ss << "TIMESTAMP";
        break;
      case TTypeId::DECIMAL:
        ss << "DECIMAL(" << schema.cols[i].type.precision << ","
           << schema.cols[i].type.scale << ")";
        break;
      default:
        ss << "Unknown";
    }
    ss << " " << schema.cols[i].name;
  }
  ss << "]" << endl;
  return ss.str();
}

// Returns comparison of x <> string(y_data, y_len)
int CompareString(const string& x, const char* y_data, int y_len) {
  int len = min((int)x.size(), y_len);
  int ret = strncmp(x.c_str(), y_data, len);
  if (ret != 0) return ret;
  return x.size() - y_len;
}

const TNetworkAddress& GetHost(const TPlanRequestResult& plan, int task_id) {
  if (plan.tasks[task_id].local_hosts.empty()) {
    return plan.hosts[0];
  }
  return plan.tasks[task_id].local_hosts[0];
}

void ExecRequestDistributed(const char* request, TRowBatchFormat::type format) {
  printf("Planning request: %s\n", request);

  shared_ptr<TTransport> planner_socket(
      new TSocket(PLANNER_HOST, RECORD_SERVICE_PLANNER_PORT));
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

  printf("Done planning. Generated %ld tasks.\n", plan_result.tasks.size());
  printf("Tasks:\n");
  for (int i = 0; i < plan_result.tasks.size(); ++i) {
    printf("  %d\n", i + 1);
    for (int j = 0; j < plan_result.tasks[i].local_hosts.size(); ++j) {
      printf("     %s\n", plan_result.tasks[i].local_hosts[j].hostname.c_str());
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
#elif QUERY_2
  bool min_string_found = false;
  string min_string;
#endif

  for (int i = 0; i < plan_result.tasks.size(); ++i) {
    const TTask& task = plan_result.tasks[i];
    // Run each task on the first host it reported
    const TNetworkAddress& host = GetHost(plan_result, i);
    shared_ptr<TTransport> socket(new TSocket(host.hostname, host.port));
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
        if (fetch_result.row_batch_format == TRowBatchFormat::Columnar) {
          values = (const int64_t*)fetch_result.columnar_row_batch.cols[0].data.data();
          nulls = (const uint8_t*)fetch_result.columnar_row_batch.cols[0].is_null.data();
        } else {
          printf("Unknown row batch format.\n");
          return;
        }

        int idx = 0;
        for (int n = 0; n < fetch_result.num_rows; ++n) {
          if (nulls[n]) continue;
          q1_result += values[idx++];
        }
#elif QUERY_2
        const char* data = NULL;
        const uint8_t* nulls = NULL;
        if (fetch_result.row_batch_format == TRowBatchFormat::Columnar) {
          data = fetch_result.columnar_row_batch.cols[0].data.data();
          nulls = (const uint8_t*)fetch_result.columnar_row_batch.cols[0].is_null.data();
        } else {
          printf("Unknown row batch format.\n");
          return;
        }
        for (int n = 0; n < fetch_result.num_rows; ++n) {
          if (nulls[n]) continue;
          int32_t str_len = *(const int32_t*)data;
          data += sizeof(int32_t);

          if (!min_string_found || CompareString(min_string, data, str_len) > 0) {
            min_string = string(data, str_len);
            min_string_found = true;
          }

          data += str_len;
        }
#endif
        task_time.Stop();
      } while (!fetch_result.done);

      TStats stats;
      worker.GetTaskStats(stats, exec_result.handle);

      total_stats.serialize_time_ms += stats.serialize_time_ms;
      total_stats.client_time_ms += stats.client_time_ms;
      total_stats.decompress_time_ms += stats.decompress_time_ms;
      total_stats.bytes_read += stats.bytes_read;
      total_stats.bytes_read_local += stats.bytes_read_local;
      total_stats.hdfs_throughput += stats.hdfs_throughput;
      total_stats.num_rows_read += stats.num_rows_read;
      total_stats.num_rows_returned += stats.num_rows_returned;

      worker.CloseTask(exec_result.handle);

      printf("Worker %d returned %d rows\n", i + 1, worker_rows);
      total_rows += worker_rows;
    } catch (const TRecordServiceException& e) {
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
    printf("  DecompressTime: %ld ms (%0.2f%%)\n",
        total_stats.decompress_time_ms,
        total_stats.decompress_time_ms / duration_ms * 100);
    printf("  TaskProcessTime: %ld ms (%0.2f%%)\n",
        task_time_ms, task_time_ms / duration_ms * 100);
  }
  printf("  BytesRead: %0.2f mb\n", total_stats.bytes_read / (1024. * 1024.));
  printf("  BytesReadLocal: %0.2f mb\n", total_stats.bytes_read_local / (1024. * 1024.));
  printf("  Avg Hdfs Throughput: %0.2f mb/s\n",
      total_stats.hdfs_throughput / (1024 * 1024) / plan_result.tasks.size());
  printf("  Rows filtered: %ld\n",
      total_stats.num_rows_read - total_stats.num_rows_returned);

#if QUERY_1
  printf("Query 1 Result: %ld\n", q1_result);
#elif QUERY_2
  printf("Query 2 Result: %s\n", min_string.c_str());
#endif
}

int main(int argc, char** argv) {
  if (argc < 2) {
    printf("usage: record-service-client <sql stmt>\n");
    return 1;
  }
  ExecRequestDistributed(argv[1], TRowBatchFormat::Columnar);
  return 0;
}
