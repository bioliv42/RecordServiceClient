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
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.recordservice.core.PlanRequestResult;
import com.cloudera.recordservice.core.RecordServiceException;
import com.cloudera.recordservice.core.RecordServicePlannerClient;
import com.cloudera.recordservice.core.Request;
import com.cloudera.recordservice.core.Task;
import com.cloudera.recordservice.core.TaskStatus.Stats;
import com.cloudera.recordservice.mr.RecordReaderCore;
import com.cloudera.recordservice.mr.RecordServiceConfig;
import com.cloudera.recordservice.mr.Schema;
import com.cloudera.recordservice.mr.TaskInfo;
import com.cloudera.recordservice.mr.security.DelegationTokenIdentifier;
import com.cloudera.recordservice.mr.security.TokenUtils;

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
  // Name of record service counters group.
  public final static String COUNTERS_GROUP_NAME = "Record Service Counters";

  private final static Logger LOG =
      LoggerFactory.getLogger(RecordServiceInputFormatBase.class);

  // Encapsulates results of a plan request, returning the splits and the schema.
  public static class SplitsInfo {
    public final List<InputSplit> splits;
    public final Schema schema;

    public SplitsInfo(List<InputSplit> splits, Schema schema) {
      this.splits = splits;
      this.schema = schema;
    }
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException,
      InterruptedException {
    return getSplits(context.getConfiguration(), context.getCredentials()).splits;
  }

  /**
   * Looks inside the jobConf to construct the RecordService request. The
   * request can either be a sql statement, table or path.
   *
   * This also handles authentication using credentials. If there is a delegation
   * token in the credentials, that will be used to authenticate the planner
   * connection. Otherwise, if kerberos is enabled, a token will be generated
   * and added to the credentials.
   * TODO: is this behavior sufficient? Do we need to fall back and renew tokens
   * or does the higher level framework (i.e. oozie) do that?
   * TODO: move this to another class. Not intuitive the implementation is here.
   */
  public static SplitsInfo getSplits(Configuration jobConf,
      Credentials credentials) throws IOException {
    LOG.debug("Generating input splits.");

    String tblName = jobConf.get(RecordServiceConfig.TBL_NAME_CONF);
    String inputDir = jobConf.get(FileInputFormat.INPUT_DIR);
    String sqlQuery = jobConf.get(RecordServiceConfig.QUERY_NAME_CONF);

    int numSet = 0;
    if (tblName != null) ++numSet;
    if (inputDir != null) ++numSet;
    if (sqlQuery != null) ++numSet;

    if (numSet == 0) {
      throw new IllegalArgumentException("No input specified. Specify either '" +
          RecordServiceConfig.TBL_NAME_CONF + "', '" +
          RecordServiceConfig.QUERY_NAME_CONF + "' or '" +
          FileInputFormat.INPUT_DIR + "'");
    }
    if (numSet > 1) {
      throw new IllegalArgumentException("More than one input specified. Can " +
          "only specify one of '" +
          RecordServiceConfig.TBL_NAME_CONF + "'=" + tblName + ", '" +
          FileInputFormat.INPUT_DIR + "'=" + inputDir + ", '" +
          RecordServiceConfig.QUERY_NAME_CONF + "'=" + sqlQuery);
    }

    String[] colNames = jobConf.getStrings(RecordServiceConfig.COL_NAMES_CONF);
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
        // TODO: this has slightly different meaning than createProjectionRequest()
        // which treats empty columns as an empty projection. i.e. select * vs count(*)
        // Reconcile this.
        request = Request.createTableScanRequest(tblName);
      } else {
        List<String> projection = new ArrayList<String>();
        for (String c: colNames) {
          projection.add(c);
        }
        request = Request.createProjectionRequest(tblName, projection);
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
      assert false;
    }

    PlanRequestResult result = null;
    RecordServicePlannerClient planner = null;
    try {
      boolean needToGetToken = false;
      RecordServicePlannerClient.Builder builder =
          new RecordServicePlannerClient.Builder();
      // Try to get the delegation token from the credentials. If it is there, use it.
      @SuppressWarnings("unchecked")
      Token<DelegationTokenIdentifier> delegationToken =
          (Token<DelegationTokenIdentifier>) credentials.getToken(
              DelegationTokenIdentifier.DELEGATION_KIND);

      if (delegationToken != null) {
        builder.setDelegationToken(TokenUtils.toDelegationToken(delegationToken));
      } else {
        String kerberosPrincipal =
            jobConf.get(RecordServiceConfig.KERBEROS_PRINCIPAL_CONF);
        if (kerberosPrincipal != null) {
          builder.setKerberosPrincipal(kerberosPrincipal);
          needToGetToken = true;
        }
      }
      planner = builder.connect(
          jobConf.get(RecordServiceConfig.PLANNER_HOST_CONF,
              RecordServiceConfig.DEFAULT_PLANNER_HOST),
          jobConf.getInt(RecordServiceConfig.PLANNER_PORT_CONF,
              RecordServiceConfig.DEFAULT_PLANNER_PORT));
      result = planner.planRequest(request);

      if (needToGetToken) {
        // We need to get a delegation token and populate credentials (for the map tasks)
        // TODO: what to set as renewer?
        delegationToken =
            TokenUtils.fromTDelegationToken(planner.getDelegationToken(""));
        credentials.addToken(DelegationTokenIdentifier.DELEGATION_KIND, delegationToken);
      }
    } catch (Exception e) {
      throw new IOException(e);
    } finally {
      if (planner != null) planner.close();
    }

    Schema schema = new Schema(result.schema);
    List<InputSplit> splits = new ArrayList<InputSplit>();
    for (Task t : result.tasks) {
      splits.add(new RecordServiceInputSplit(schema, new TaskInfo(t)));
    }
    LOG.debug(String.format("Generated %d splits.", splits.size()));
    return new SplitsInfo(splits, schema);
  }

  /**
   * Populates RecordService counters in ctx from counters.
   */
  public static void setCounters(TaskAttemptContext ctx, Stats counters) {
    if (ctx == null) return;
    ctx.getCounter(COUNTERS_GROUP_NAME, "Records Read").setValue(
        counters.numRecordsRead);
    ctx.getCounter(COUNTERS_GROUP_NAME, "Records Returned").setValue(
        counters.numRecordsReturned);
    ctx.getCounter(COUNTERS_GROUP_NAME, "Record Serialization Time(ms)").setValue(
        counters.serializeTimeMs);
    ctx.getCounter(COUNTERS_GROUP_NAME, "Client Time(ms)").setValue(
        counters.clientTimeMs);

    if (counters.hdfsCountersSet) {
      ctx.getCounter(COUNTERS_GROUP_NAME, "Bytes Read").setValue(
          counters.bytesRead);
      ctx.getCounter(COUNTERS_GROUP_NAME, "Decompression Time(ms)").setValue(
          counters.decompressTimeMs);
      ctx.getCounter(COUNTERS_GROUP_NAME, "Bytes Read Local").setValue(
          counters.bytesReadLocal);
      ctx.getCounter(COUNTERS_GROUP_NAME, "HDFS Throughput(MB/s)").setValue(
          (long)(counters.hdfsThroughput / (1024 * 1024)));
    }
  }

  /**
   * Populates RecordService counters in ctx from counters.
   */
  public static void setCounters(Reporter ctx, Stats counters) {
    if (ctx == null) return;
    ctx.getCounter(COUNTERS_GROUP_NAME, "Records Read").setValue(
        counters.numRecordsRead);
    ctx.getCounter(COUNTERS_GROUP_NAME, "Records Returned").setValue(
        counters.numRecordsReturned);
    ctx.getCounter(COUNTERS_GROUP_NAME, "Record Serialization Time(ms)").setValue(
        counters.serializeTimeMs);
    ctx.getCounter(COUNTERS_GROUP_NAME, "Client Time(ms)").setValue(
        counters.clientTimeMs);

    if (counters.hdfsCountersSet) {
      ctx.getCounter(COUNTERS_GROUP_NAME, "Bytes Read").setValue(
          counters.bytesRead);
      ctx.getCounter(COUNTERS_GROUP_NAME, "Decompression Time(ms)").setValue(
          counters.decompressTimeMs);
      ctx.getCounter(COUNTERS_GROUP_NAME, "Bytes Read Local").setValue(
          counters.bytesReadLocal);
      ctx.getCounter(COUNTERS_GROUP_NAME, "HDFS Throughput(MB/s)").setValue(
          (long)(counters.hdfsThroughput / (1024 * 1024)));
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
        reader_ = new RecordReaderCore(context.getConfiguration(),
            context.getCredentials(), rsSplit.getTaskInfo());
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
        } catch (RecordServiceException e) {
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
