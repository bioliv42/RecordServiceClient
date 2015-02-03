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

namespace cpp recordservice
namespace java com.cloudera.recordservice.thrift

struct TUniqueId {
  1: required i64 hi
  2: required i64 lo
}

enum TTypeId {
  BOOLEAN,
  TINYINT,
  SMALLINT,
  INT,
  BIGINT,
  FLOAT,
  DOUBLE,
  STRING,
  TIMESTAMP,
  DECIMAL,

  // TODO: other types
}

// TODO: how to extend this for complex types.
struct TType {
  1: required TTypeId type_id

  // TODO:
  // bool nullable?

  // Only set if id == DECIMAL
  2: optional i32 precision
  3: optional i32 scale
}

struct TColumnDesc {
  1: required TType type
  2: required string name
}

struct TSchema {
  1: required list<TColumnDesc> cols
}

enum TRowBatchFormat {
  ColumnarThrift,
  Parquet,
}

// A union over all possible return types for a column of data
struct TColumnData {
  // One element in the list for every row in the column indicating if there is
  // a value in the vals list or a null.
  1: required binary is_null

  // Only one is set, only non-null values are set.
  2: optional list<bool> bool_vals;
  3: optional list<byte> byte_vals;
  4: optional list<i16> short_vals;
  5: optional list<i32> int_vals;
  6: optional list<i64> long_vals;

  // Used for float/double
  7: optional list<double> double_vals;

  // Used for string/char/vchar
  8: optional list<string> string_vals;

  // Used for decimal, timestamp
  9: optional list<binary> binary_vals;
}

struct TColumnarRowBatch {
  // Each TColumnData contains the data for an entire column. Size is equal to the
  // number of projected columns.
  1: required list<TColumnData> cols
}

// Serialized columnar data. Instead of using a list of types, this is a custom
// serialization. Thrift sees this as a byte buffer so we have minimal serialization
// cost.
// The serialization is identical to parquet's plain encoding.
struct TParquetColumnData {
  // One byte for each value.
  // TODO: turn this into a bitmap.
  1: required binary is_null

  // Serialized data excluding NULLs.
  // TODO: add detailed spec here but this is just the parquet plain encoding.
  // Each value is serialized in little endian back to back.
  2: required binary data

  // Is this useful?
  //3: required i32 num_non_null
}

struct TParquetRowBatch {
  1: required list<TParquetColumnData> cols
}

struct TPlanRequestParams {
  1: required string sql_stmt
}

struct TTask {
  // The list of hosts where this task can run locally.
  1: required list<string> hosts

  // An opaque blob that is produced by the RecordServicePlanner and passed to
  // the RecordServiceWorker.
  2: required binary task
}

struct TPlanRequestResult {
  1: required list<TTask> tasks
  2: required TSchema schema
}

struct TExecTaskParams {
  // This is produced by the RecordServicePlanner and must be passed to the worker
  // unmodified.
  1: required binary task

  // Number of rows that should be returned per fetch. If unset, service picks default.
  2: optional i32 fetch_size

  // The format of the row batch to return. Only the corresponding field is set
  // in TFetchResult. If unset, the service picks the default.
  // TODO: needs to be versioned so old clients can talk to new servers.
  3: optional TRowBatchFormat row_batch_format
}

struct TExecTaskResult {
  1: required TUniqueId handle
  // TODO: do we need the schema here as well?
  //2: required TSchema schema
}

struct TFetchParams {
  1: required TUniqueId handle
}

struct TFetchResult {
  1: required bool done
  2: required i32 num_rows
  3: required TRowBatchFormat row_batch_format;

  // RowBatchFormat.ColumnarThrift
  4: optional TColumnarRowBatch row_batch

  // RowBatchFormat.Parquet
  5: optional TParquetRowBatch parquet_row_batch
}

struct TStats {
  // [0 - 100]
  1: required double completion_percentage

  // The number of rows read before filtering.
  2: required i64 num_rows_read

  // The number of rows returned to the client.
  3: required i64 num_rows_returned

  // Time spent in the record service serializing returned results.
  4: required i64 serialize_time_ms

  // Time spent in the client, as measured by the server. This includes
  // time in the data exchange as well as time the client spent working.
  5: required i64 client_time_ms

  //
  // HDFS specific counters
  //

  // Time spent in decompression.
  6: optional i64 decompress_time_ms

  // Bytes read from HDFS
  7: optional i64 bytes_read

  // Bytes read from the local data node.
  8: optional i64 bytes_read_local

  // Throughput of reading the raw bytes from HDFS, in bytes per second
  9: optional double hdfs_throughput
}

enum TErrorCode {
  // The request is invalid or unsupported by the Planner service.
  INVALID_REQUEST,

  // The handle is invalid or closed.
  INVALID_HANDLE,

  // Service is busy and not unable to process the request. Try later.
  SERVICE_BUSY,

  // The service ran out of memory processing the task.
  OUT_OF_MEMORY,

  // The task was cancelled.
  CANCELLED,

  // Internal error in the service.
  INTERNAL_ERROR,
}

exception TRecordServiceException {
  1: required TErrorCode code

  // The error message, intended for the client of the RecordService.
  2: required string message

  // The detailed error, intended for troubleshooting of the RecordService.
  3: optional string detail
}

// This service is responsible for planning requests.
// TODO: version this
service RecordServicePlanner {
  // Plans the request. This generates the tasks and the list of machines
  // that each task can run on.
  TPlanRequestResult PlanRequest(1:TPlanRequestParams params)
      throws(1:TRecordServiceException ex);

  // Cancel at this level? Would be nice to avoid and rely on the frameworks to
  // deal with that. e.g. Mapreduce cancels all the running map tasks which in
  // turn cancel on the worker service.
}

// This service is responsible for executing tasks generated by the RecordServicePlanner
service RecordServiceWorker {
  // Begin execution of the task in params. This is asynchronous.
  TExecTaskResult ExecTask(1:TExecTaskParams params)
      throws(1:TRecordServiceException ex);

  // Returns the next batch of rows
  // TODO: returning the row batch is not good here. Put this in a wrapper object.
  TFetchResult Fetch(1:TFetchParams params)
      throws(1:TRecordServiceException ex);

  // Closes the task specified by handle. If the task is still running, it is
  // cancelled. The handle is no longer valid after this call.
  void CloseTask(1:TUniqueId handle);

  // Returns stats for the task specified by handle. This can be called for tasks that
  // are not yet closed (including tasks in flight).
  TStats GetTaskStats(1:TUniqueId handle)
      throws(1:TRecordServiceException ex);
}
