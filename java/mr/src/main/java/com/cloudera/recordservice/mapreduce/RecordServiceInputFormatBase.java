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
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.recordservice.core.NetworkAddress;
import com.cloudera.recordservice.core.PlanRequestResult;
import com.cloudera.recordservice.core.RecordServiceException;
import com.cloudera.recordservice.core.RecordServicePlannerClient;
import com.cloudera.recordservice.core.Request;
import com.cloudera.recordservice.core.Task;
import com.cloudera.recordservice.core.TaskStatus.Stats;
import com.cloudera.recordservice.mr.PlanUtil;
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
    Request request = PlanUtil.getRequest(jobConf);
    List<NetworkAddress> plannerHostPorts = RecordServiceConfig.getPlannerHostPort(
        jobConf.get(RecordServiceConfig.PLANNER_HOSTPORTS_CONF,
            RecordServiceConfig.DEFAULT_PLANNER_HOSTPORTS));
    String kerberosPrincipal =
        jobConf.get(RecordServiceConfig.KERBEROS_PRINCIPAL_CONF);
    int timeoutMs = jobConf.getInt(RecordServiceConfig.PLANNER_SOCKET_TIMEOUT_MS_CONF,
        RecordServiceConfig.DEFAULT_PLANNER_SOCKET_TIMEOUT_MS);
    int maxAttempts = jobConf.getInt(RecordServiceConfig.PLANNER_RETRY_ATTEMPTS_CONF,
        RecordServiceConfig.DEFAULT_PLANNER_RETRY_ATTEMPTS);
    int sleepDurationMs = jobConf.getInt(RecordServiceConfig.PLANNER_RETRY_SLEEP_MS_CONF,
        RecordServiceConfig.DEFAULT_PLANNER_RETRY_SLEEP_MS);

    PlanRequestResult result = null;
    RecordServicePlannerClient planner = PlanUtil.getPlanner(plannerHostPorts,
        kerberosPrincipal, credentials, timeoutMs, maxAttempts, sleepDurationMs);
    try {
      result = planner.planRequest(request);
      if (planner.isKerberosAuthenticated()) {
        // We need to get a delegation token and populate credentials (for the map tasks)
        // TODO: what to set as renewer?
        Token<DelegationTokenIdentifier> delegationToken =
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

    // Randomize the order of the splits to mitigate skew.
    Collections.shuffle(splits);
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
      } catch (RecordServiceException e) {
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
