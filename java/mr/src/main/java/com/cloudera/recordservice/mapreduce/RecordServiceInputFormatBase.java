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

package com.cloudera.recordservice.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.recordservice.core.RecordServicePlannerClient;
import com.cloudera.recordservice.core.Request;
import com.cloudera.recordservice.mr.RecordReaderCore;
import com.cloudera.recordservice.mr.Schema;
import com.cloudera.recordservice.mr.TaskInfo;
import com.cloudera.recordservice.thrift.TPlanRequestResult;
import com.cloudera.recordservice.thrift.TRecordServiceException;
import com.cloudera.recordservice.thrift.TStats;
import com.cloudera.recordservice.thrift.TTask;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * The base RecordService input format that handles functionality common to
 * all RecordService InputFormats.
 *
 * TODO: this input format should subsume the functionality of all the
 * input formats we want to work with. This means respecting the configs
 * from all of them (or the ones from them we care about). These include:
 *   - FileInputFormat
 *   - HCat
 *   - ?
 * TODO: clean this up. Come up with a nicer way to deal with mapred and mapreduce.
 */
public abstract class RecordServiceInputFormatBase<K, V> extends InputFormat<K, V> {
  /**
   * Config keys specific to the record service.
   */
  // The query to record from.
  public final static String QUERY_NAME_CONF = "recordservice.query";

  // The fully qualified table name to read.
  public final static String TBL_NAME_CONF = "recordservice.table.name";

  // The subset of columns to read.
  public final static String COL_NAMES_CONF = "recordservice.col.names";

  // Host/Port of the planner service.
  public final static String PLANNER_HOST = "recordservice.planner.host";
  public final static String PLANNER_PORT = "recordservice.planner.port";

  // Kerberos principal.
  public final static String KERBEROS_PRINCIPAL = "recordservice.kerberos.principal";

  // Name of record service counters group.
  public final static String COUNTERS_GROUP_NAME = "Record Service Counters";

  private final static Logger LOG =
      LoggerFactory.getLogger(RecordServiceInputFormatBase.class);

  // Encapsulates results of a plan request, returning the splits and the schema.
  public static class SplitsInfo {
    public List<InputSplit> splits;
    public Schema schema;

    public SplitsInfo(List<InputSplit> splits, Schema schema) {
      this.splits = splits;
      this.schema = schema;
    }
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException,
      InterruptedException {
    return getSplits(context.getConfiguration()).splits;
  }

  /**
   * Looks inside the jobConf to construct the RecordService request. The
   * request can either be a sql statement, table or path.
   */
  public static SplitsInfo getSplits(Configuration jobConf) throws IOException {
    LOG.debug("Generating input splits.");

    String tblName = jobConf.get(TBL_NAME_CONF);
    String inputDir = jobConf.get(FileInputFormat.INPUT_DIR);
    String sqlQuery = jobConf.get(QUERY_NAME_CONF);

    int numSet = 0;
    if (tblName != null) ++numSet;
    if (inputDir != null) ++numSet;
    if (sqlQuery != null) ++numSet;

    if (numSet == 0) {
      throw new IllegalArgumentException("No input specified. Specify either '" +
          TBL_NAME_CONF + "', '" + QUERY_NAME_CONF + "' or '" +
          FileInputFormat.INPUT_DIR + "'");
    }
    if (numSet > 1) {
      throw new IllegalArgumentException("More than one input specified. Can " +
          "only specify one of '" +
          TBL_NAME_CONF + "'=" + tblName + ", '" +
          FileInputFormat.INPUT_DIR + "'=" + inputDir + ", '" +
          QUERY_NAME_CONF + "'=" + sqlQuery);
    }

    String[] colNames = jobConf.getStrings(COL_NAMES_CONF);
    if (colNames == null) colNames = new String[0];

    if (tblName == null && colNames.length > 0) {
      // TODO: support this.
      throw new IllegalArgumentException(
          "Column projections can only be specified with table inputs.");
    }

    Request request = null;
    if (tblName != null) {
      if (colNames.length == 0) {
        // If length of colNames = 0, return all possible columns
        request = Request.createTableScanRequest(tblName);
      } else {
        request = Request.createProjectionRequest(tblName, Lists.newArrayList(colNames));
      }
    } else if (inputDir != null) {
      // TODO: inputDir is a comma separate list of paths. The service needs to
      // handle that.
      if (inputDir.contains(",")) {
        throw new IllegalArgumentException(
            "Only reading a single directory is currently supported.");
      }
      request = Request.createPathRequest(inputDir);
    } else if (sqlQuery != null) {
      request = Request.createSqlRequest(sqlQuery);
    } else {
      Preconditions.checkState(false);
    }

    TPlanRequestResult result = null;
    try {
      result = RecordServicePlannerClient.planRequest(
          jobConf.get(PLANNER_HOST, "localhost"),
          jobConf.getInt(PLANNER_PORT, 40000),
          request,
          jobConf.get(KERBEROS_PRINCIPAL));
    } catch (Exception e) {
      throw new IOException(e);
    }
    Schema schema = new Schema(result.getSchema());
    List<InputSplit> splits = new ArrayList<InputSplit>();
    for (TTask tTask : result.getTasks()) {
      splits.add(new RecordServiceInputSplit(schema, new TaskInfo(tTask)));
    }
    LOG.debug(String.format("Generated %d splits.", splits.size()));
    return new SplitsInfo(splits, schema);
  }

  /**
   * Sets the input configuration to read 'cols' from 'db.tbl'. If the tbl is fully
   * qualified, db should be null.
   * If cols is empty, all cols in the table are read.
   */
  public static void setInputTable(Configuration config, String db, String tbl,
      String... cols) {
    if (db != null) tbl = db + "." + tbl;
    config.set(TBL_NAME_CONF, tbl);
    if (cols != null && cols.length > 0) config.setStrings(COL_NAMES_CONF, cols);
  }

  /**
   * Sets the input configuration to read results from 'query'.
   */
  public static void setInputQuery(Configuration config, String query) {
    config.set(QUERY_NAME_CONF, query);
  }

  /**
   * Populates RecordService counters in ctx from counters.
   */
  public static void setCounters(TaskAttemptContext ctx, TStats counters) {
    if (ctx == null) return;
    ctx.getCounter(COUNTERS_GROUP_NAME, "Records Read").setValue(
        counters.num_records_read);
    ctx.getCounter(COUNTERS_GROUP_NAME, "Records Returned").setValue(
        counters.num_records_returned);
    ctx.getCounter(COUNTERS_GROUP_NAME, "Record Serialization Time(ms)").setValue(
        counters.serialize_time_ms);
    ctx.getCounter(COUNTERS_GROUP_NAME, "Client Time(ms)").setValue(
        counters.client_time_ms);

    if (counters.isSetBytes_read()) {
      ctx.getCounter(COUNTERS_GROUP_NAME, "Bytes Read").setValue(
          counters.bytes_read);
    }
    if (counters.isSetDecompress_time_ms()) {
      ctx.getCounter(COUNTERS_GROUP_NAME, "Decompression Time(ms)").setValue(
          counters.decompress_time_ms);
    }
    if (counters.isSetBytes_read_local()) {
      ctx.getCounter(COUNTERS_GROUP_NAME, "Bytes Read Local").setValue(
          counters.bytes_read_local);
    }
    if (counters.isSetHdfs_throughput()) {
      ctx.getCounter(COUNTERS_GROUP_NAME, "HDFS Throughput(MB/s)").setValue(
          (long)(counters.hdfs_throughput / (1024 * 1024)));
    }
  }

  /**
   * Populates RecordService counters in ctx from counters.
   */
  public static void setCounters(Reporter ctx, TStats counters) {
    if (ctx == null) return;
    ctx.getCounter(COUNTERS_GROUP_NAME, "Records Read").setValue(
        counters.num_records_read);
    ctx.getCounter(COUNTERS_GROUP_NAME, "Records Returned").setValue(
        counters.num_records_returned);
    ctx.getCounter(COUNTERS_GROUP_NAME, "Record Serialization Time(ms)").setValue(
        counters.serialize_time_ms);
    ctx.getCounter(COUNTERS_GROUP_NAME, "Client Time(ms)").setValue(
        counters.client_time_ms);

    if (counters.isSetBytes_read()) {
      ctx.getCounter(COUNTERS_GROUP_NAME, "Bytes Read").setValue(
          counters.bytes_read);
    }
    if (counters.isSetDecompress_time_ms()) {
      ctx.getCounter(COUNTERS_GROUP_NAME, "Decompression Time(ms)").setValue(
          counters.decompress_time_ms);
    }
    if (counters.isSetBytes_read_local()) {
      ctx.getCounter(COUNTERS_GROUP_NAME, "Bytes Read Local").setValue(
          counters.bytes_read_local);
    }
    if (counters.isSetHdfs_throughput()) {
      ctx.getCounter(COUNTERS_GROUP_NAME, "HDFS Throughput(MB/s)").setValue(
          (long)(counters.hdfs_throughput / (1024 * 1024)));
    }
  }

  /**
   * Base class of RecordService based record readers.
   */
  protected abstract static class RecordReaderBase<K,V> extends RecordReader<K, V> {
    private static final Logger LOG =
        LoggerFactory.getLogger(RecordReaderBase.class);
    protected TaskAttemptContext context_;
    protected RecordReaderCore reader_;

    /**
     * Initializes the RecordReader and starts execution of the task.
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
      RecordServiceInputSplit rsSplit = (RecordServiceInputSplit)split;
      try {
        reader_ = new RecordReaderCore(context.getConfiguration(), rsSplit.getTaskInfo());
      } catch (Exception e) {
        throw new IOException("Failed to execute task.", e);
      }
      context_ = context;
    }

    @Override
    public void close() throws IOException {
      if (reader_ != null) {
        try {
          RecordServiceInputFormatBase.setCounters(
              context_, reader_.records().getStatus().stats);
        } catch (TRecordServiceException e) {
          LOG.debug("Could not populate counters: " + e);
        }
        reader_.close();
        reader_ = null;
      }
    }

    @Override
    public float getProgress() throws IOException {
      if (reader_ == null) return 1;
      return reader_.records().progress();
    }
  }
}
