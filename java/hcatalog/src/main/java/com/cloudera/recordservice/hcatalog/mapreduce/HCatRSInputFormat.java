/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.cloudera.recordservice.hcatalog.mapreduce;

import com.cloudera.recordservice.core.NetworkAddress;
import com.cloudera.recordservice.core.RecordServiceException;
import com.cloudera.recordservice.core.RecordServicePlannerClient;
import com.cloudera.recordservice.core.Request;
import com.cloudera.recordservice.hcatalog.common.HCatRSUtil;
import com.cloudera.recordservice.mr.RecordServiceConfig;
import com.cloudera.recordservice.mr.security.DelegationTokenIdentifier;
import com.cloudera.recordservice.mr.security.TokenUtils;
import com.cloudera.recordservice.thrift.TPlanRequestResult;
import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.data.schema.HCatSchema;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * The InputFormat to use to read data from HCatalog.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class HCatRSInputFormat extends HCatRSBaseInputFormat {

  private Configuration conf;
  private InputJobInfo inputJobInfo;
  private static String requestString;

  /**
   * Initializes the input with a null filter.
   * See {@link #setInput(Configuration, String, String, String)}
   */
  public static HCatRSInputFormat setInput(
          Job job, String dbName, String tableName)
    throws IOException {
    return setInput(job.getConfiguration(), dbName, tableName, null);
  }

  /**
   * Initializes the input with a provided filter.
   * See {@link #setInput(Configuration, String, String, String)}
   */
  public static HCatRSInputFormat setInput(
          Job job, String dbName, String tableName, String filter)
    throws IOException {
    Request request = null;
    createRequestString(dbName, tableName);
    // hardcoded until base call works for ease of testing purposes
    request = Request.createTableScanRequest(requestString);
    Credentials credentials = job.getCredentials();
    TPlanRequestResult result = null;
    RecordServicePlannerClient planner = null;
    try {
      RecordServicePlannerClient.Builder builder =
              new RecordServicePlannerClient.Builder();
      String kerberosPrincipal =
              job.getConfiguration().get(RecordServiceConfig.KERBEROS_PRINCIPAL_CONF);
      builder.setKerberosPrincipal(kerberosPrincipal);
      List<NetworkAddress> plannerHostPorts = RecordServiceConfig.getPlannerHostPort(
              job.getConfiguration().get(RecordServiceConfig.PLANNER_HOSTPORTS_CONF,
                      RecordServiceConfig.DEFAULT_PLANNER_HOSTPORTS));
      Exception lastException = null;
      for (int i = 0; i < plannerHostPorts.size(); ++i) {
        NetworkAddress hostPort = plannerHostPorts.get(i);
        try {
          RecordServicePlannerClient tempPlanner = builder.connect(hostPort.hostname, hostPort.port);
          if (tempPlanner != null)
            planner = tempPlanner;
        } catch (RecordServiceException e) {
          // Ignore, try next host. The errors in builder should be sufficient.
          lastException = e;
        } catch (IOException e) {
          // Ignore, try next host. The errors in builder should be sufficient.
          lastException = e;
        }
      }
        // TODO: what to set as renewer?
      Token<DelegationTokenIdentifier> delegationToken =
                TokenUtils.fromTDelegationToken(planner.getDelegationToken(""));
        credentials.addToken(DelegationTokenIdentifier.DELEGATION_KIND, delegationToken);
    } catch (Exception e) {
      throw new IOException(e);
    } finally {
      if (planner != null) planner.close();
    }
    return setInput(job.getConfiguration(), dbName, tableName, filter);
  }

  /**
   * Initializes the input with a null filter.
   * See {@link #setInput(Configuration, String, String, String)}
   */
  public static HCatRSInputFormat setInput(
          Configuration conf, String dbName, String tableName)
    throws IOException {
    return setInput(conf, dbName, tableName, null);
  }

  /**
   * Set inputs to use for the job. This queries the metastore with the given input
   * specification and serializes matching partitions into the job conf for use by MR tasks.
   * @param conf the job configuration
   * @param dbName database name, which if null 'default' is used
   * @param tableName table name
   * @param filter the partition filter to use, can be null for no filter
   * @throws IOException on all errors
   */
  public static HCatRSInputFormat setInput(
          Configuration conf, String dbName, String tableName, String filter)
    throws IOException {

    Preconditions.checkNotNull(conf, "required argument 'conf' is null");
    Preconditions.checkNotNull(tableName, "required argument 'tableName' is null");

    HCatRSInputFormat hCatRSInputFormat = new HCatRSInputFormat();
    hCatRSInputFormat.conf = conf;
    hCatRSInputFormat.inputJobInfo = InputJobInfo.create(dbName, tableName, filter, null);

    try {
      InitializeInput.setInput(conf, hCatRSInputFormat.inputJobInfo);
    } catch (Exception e) {
      throw new IOException(e);
    }



    return hCatRSInputFormat;
  }

  /**
   * @deprecated as of 0.13, slated for removal with 0.15
   * Use {@link #setInput(Configuration, String, String, String)} instead,
   * to specify a partition filter to directly initialize the input with.
   */
  @Deprecated
  public HCatRSInputFormat setFilter(String filter) throws IOException {
    // null filters are supported to simplify client code
    if (filter != null) {
      inputJobInfo = InputJobInfo.create(
        inputJobInfo.getDatabaseName(),
        inputJobInfo.getTableName(),
        filter,
        inputJobInfo.getProperties());
      try {
        InitializeInput.setInput(conf, inputJobInfo);
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
    return this;
  }

  /**
   * Set properties for the input format.
   * @param properties properties for the input specification
   * @return this
   * @throws IOException on all errors
   */
  public HCatRSInputFormat setProperties(Properties properties) throws IOException {
    Preconditions.checkNotNull(properties, "required argument 'properties' is null");
    inputJobInfo = InputJobInfo.create(
      inputJobInfo.getDatabaseName(),
      inputJobInfo.getTableName(),
      inputJobInfo.getFilter(),
      properties);
    try {
      InitializeInput.setInput(conf, inputJobInfo);
    } catch (Exception e) {
      throw new IOException(e);
    }
    return this;
  }

  /**
   * Return partitioning columns for this input, can only be called after setInput is called.
   * @return partitioning columns of the table specified by the job.
   * @throws IOException
   */
  public static HCatSchema getPartitionColumns(Configuration conf) throws IOException {
    InputJobInfo inputInfo = (InputJobInfo) HCatRSUtil.deserialize(
        conf.get(HCatConstants.HCAT_KEY_JOB_INFO));
    Preconditions.checkNotNull(inputInfo,
        "inputJobInfo is null, setInput has not yet been called to save job into conf supplied.");
    return inputInfo.getTableInfo().getPartitionColumns();

  }

  /**
   * Return data columns for this input, can only be called after setInput is called.
   * @return data columns of the table specified by the job.
   * @throws IOException
   */
  public static HCatSchema getDataColumns(Configuration conf) throws IOException {
    InputJobInfo inputInfo = (InputJobInfo) HCatRSUtil.deserialize(
        conf.get(HCatConstants.HCAT_KEY_JOB_INFO));
    Preconditions.checkNotNull(inputInfo,
        "inputJobInfo is null, setInput has not yet been called to save job into conf supplied.");
    return inputInfo.getTableInfo().getDataColumns();
  }

  private static void createRequestString(String databaseName, String tableName){
    String request = "";
    if(databaseName != null){
      request += databaseName + ".";
    }
    request += tableName;
    requestString = request;
  }

  public static String getRequestString(){
    return requestString;
  }
}
