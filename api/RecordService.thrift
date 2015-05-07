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

//
// This file contains the service definition for the RecordService which consists
// of two thrift services: RecordServicePlanner and RecordServiceWorker.
//

// Version used to negotiate feature support between the server and client. Both
// clients and servers need to maintain backwards compatibility.
enum TProtocolVersion {
  V1,
}

// 128-bit GUID.
struct TUniqueId {
  1: required i64 hi
  2: required i64 lo
}

// Network address which specifies the machine, typically used to schedule where
// tasks run.
struct TNetworkAddress {
  1: required string hostname
  2: required i32 port
}

// Types support by the RecordService.
enum TTypeId {
  // Boolean, true or false
  BOOLEAN,

  // 1-byte (signed) integer.
  TINYINT,

  // 2-byte (signed) integer.
  SMALLINT,

  // 4-byte (signed) integer.
  INT,

  // 8-byte (signed) integer.
  BIGINT,

  // ieee floating point number
  FLOAT,

  // ieee double precision floating point number
  DOUBLE,

  // Variable length byte array.
  STRING,

  // Variable length byte array with a restricted maximum length.
  VARCHAR,

  // Fixed length byte array.
  CHAR,

  // Fixed point decimal value.
  DECIMAL,

  // Timestamp with nanosecond precision.
  TIMESTAMP_NANOS,
}

// Type specification, containing the enum and any additional parameters the type
// may need.
// TODO: how to extend this for complex types.
struct TType {
  1: required TTypeId type_id

  // Only set if id == DECIMAL
  2: optional i32 precision
  3: optional i32 scale

  // Only set if id == VARCHAR or CHAR
  4: optional i32 len
}

// A description for a column in the schema.
struct TColumnDesc {
  1: required TType type
  2: required string name
}

// Representation of a RecordServiceSchema.
// TODO: extend for nested types.
struct TSchema {
  1: required list<TColumnDesc> cols
}

// Record serialization formats.
enum TRecordFormat {
  Columnar,
}

// Serialized columnar data. Instead of using a list of types, this is a custom
// serialization. Thrift sees this as a byte buffer so we have minimal serialization
// cost.
// The serialization is identical to parquet's plain encoding with these exceptions:
//   - TimestampNanos: Parquet encodes as 8 byte nanos in day and 4 byte Julian day.
//     RecordService encoding is 8 byte millis since epoch and 4 byte nanos.
struct TColumnData {
  // One byte for each value.
  // TODO: turn this into a bitmap.
  1: required binary is_null

  // Serialized data excluding NULLs.
  // TODO: add detailed spec here but this is just the parquet plain encoding.
  // Each value is serialized in little endian back to back.
  2: required binary data

  // Is this useful?
  //3: required i32 num_non_null

  // TODO: this is useful if reading remotely.
  // 4: optional CompressionCodec
}

// List of column data for the Columnar record format. This is 1:1 with the
// schema. i.e. cols[0] is the data for schema.cols[0].
struct TColumnarRecords {
  1: required list<TColumnData> cols
}

// The type of request specified by the client. Clients can specify read
// requests in multiple ways.
enum TRequestType {
  Sql,
  Path,
}

struct TPathRequest {
  // The URI to read.
  1: required string path

  // Optional query (for predicate push down). The query must be valid SQL with
  // __PATH__ used instead of the table.
  // TODO: revisit.
  2: optional string query

  // The file format of the file at this path.
  // TODO: is this a good idea? How hard should we have the service try to figure
  // it out? What do you do if the path is a directory with different file formats or
  // different schemas?
  //3: optional TFileFormat file_format

  // If the application knows the schema from somewhere else, they can specify it
  // here.
  //4: optional TSchema schema
}

// Log messages return by the RPCs. Non-continuable errors are returned via
// exceptions in the RPCs, these messages contain either warnings or additional
// diagnostics.
struct TLogMessage {
  1: required string message

  // The number of times similar messages have occurred. It is up to the service to
  // decide what counts as a duplicate.
  2: required i32 count = 1
}

struct TPlanRequestParams {
  // The version of the client
  1: required TProtocolVersion client_version = TProtocolVersion.V1

  2: required TRequestType request_type

  // TODO: things like abort on error, sampling, etc.
  //3: required TRequestOptions request_options

  // Only one of the below is set depending on request type
  4: optional string sql_stmt
  5: optional TPathRequest path
}

struct TTask {
  // The list of hosts where this task can run locally.
  1: required list<TNetworkAddress> local_hosts

  // An opaque blob that is produced by the RecordServicePlanner and passed to
  // the RecordServiceWorker.
  2: required binary task
}

struct TPlanRequestResult {
  1: required list<TTask> tasks
  2: required TSchema schema

  // The list of all hosts running workers.
  3: required list<TNetworkAddress> hosts

  // List of warnings generated during planning the request. These do not have
  // any impact on correctness.
  4: required list<TLogMessage> warnings
}

struct TGetSchemaResult {
  1: required TSchema schema
  2: required list<TLogMessage> warnings
}

struct TExecTaskParams {
  // This is produced by the RecordServicePlanner and must be passed to the worker
  // unmodified.
  1: required binary task

  // Maximum number of records that can be returned per fetch. The server can return
  // fewer. If unset, service picks default.
  2: optional i32 fetch_size

  // The format of the records to return. Only the corresponding field is set
  // in TFetchResult. If unset, the service picks the default.
  3: optional TRecordFormat record_format

  // The memory limit for this task in bytes. If unset, the service manages it
  // on its own.
  4: optional i64 mem_limit
}

struct TExecTaskResult {
  1: required TUniqueId handle

  // Schema of the records returned from Fetch().
  2: required TSchema schema
}

struct TFetchParams {
  1: required TUniqueId handle
}

struct TFetchResult {
  // If true, all records for this task have been returned. It is still valid to
  // continue to fetch, but they will return 0 records.
  1: required bool done

  // The approximate completion progress [0, 1]
  2: required double task_progress

  // The number of records in this batch.
  3: required i32 num_records

  // The encoding format.
  4: required TRecordFormat record_format

  // TRecordFormat.Columnar
  5: optional TColumnarRecords columnar_records
}

struct TStats {
  // [0 - 1]
  1: required double task_progress

  // The number of records read before filtering.
  2: required i64 num_records_read

  // The number of records returned to the client.
  3: required i64 num_records_returned

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

struct TTaskStatus {
  1: required TStats stats

  // Errors due to invalid data
  2: required list<TLogMessage> data_errors

  // Warnings encountered when running the task. These should have no impact
  // on correctness.
  3: required list<TLogMessage> warnings
}

// FIXME: we need a lot more testing for these.
enum TErrorCode {
  // The request is invalid or unsupported by the Planner service.
  INVALID_REQUEST,

  // The handle is invalid or closed.
  INVALID_HANDLE,

  // The task is malformed.
  INVALID_TASK,

  // Service is busy and not unable to process the request. Try later.
  SERVICE_BUSY,

  // The service ran out of memory processing the task.
  OUT_OF_MEMORY,

  // The task was cancelled.
  CANCELLED,

  // Internal error in the service.
  INTERNAL_ERROR,

  // The server closed this connection and any active requests due to a timeout.
  // Clients will need to reconnect.
  CONNECTION_TIMED_OUT,
}

exception TRecordServiceException {
  1: required TErrorCode code

  // The error message, intended for the client of the RecordService.
  2: required string message

  // The detailed error, intended for troubleshooting of the RecordService.
  3: optional string detail
}

// This service is responsible for planning requests.
service RecordServicePlanner {
  // Returns the version of the server.
  TProtocolVersion GetProtocolVersion()

  // Plans the request. This generates the tasks and the list of machines
  // that each task can run on.
  TPlanRequestResult PlanRequest(1:TPlanRequestParams params)
      throws(1:TRecordServiceException ex);

  // Returns the schema for a plan request.
  TGetSchemaResult GetSchema(1:TPlanRequestParams params)
      throws(1:TRecordServiceException ex);
}

// This service is responsible for executing tasks generated by the RecordServicePlanner
service RecordServiceWorker {
  // Returns the version of the server.
  TProtocolVersion GetProtocolVersion()

  // Begin execution of the task in params. This is asynchronous.
  TExecTaskResult ExecTask(1:TExecTaskParams params)
      throws(1:TRecordServiceException ex);

  // Returns the next batch of records
  TFetchResult Fetch(1:TFetchParams params)
      throws(1:TRecordServiceException ex);

  // Closes the task specified by handle. If the task is still running, it is
  // cancelled. The handle is no longer valid after this call.
  void CloseTask(1:TUniqueId handle);

  // Returns status for the task specified by handle. This can be called for tasks that
  // are not yet closed (including tasks in flight).
  TTaskStatus GetTaskStatus(1:TUniqueId handle)
      throws(1:TRecordServiceException ex);
}

