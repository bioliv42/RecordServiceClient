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

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.cloudera.recordservice.hcatalog.common.HCatRSUtil;
import com.cloudera.recordservice.mr.PlanUtil;
import com.cloudera.recordservice.mr.RecordServiceConfig;
import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import com.cloudera.recordservice.core.NetworkAddress;
import com.cloudera.recordservice.core.RecordServiceException;
import com.cloudera.recordservice.core.RecordServicePlannerClient;
import com.cloudera.recordservice.core.Request;
import com.cloudera.recordservice.mr.security.DelegationTokenIdentifier;
import com.cloudera.recordservice.mr.security.TokenUtils;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import com.cloudera.recordservice.mr.RecordServiceConfig.ConfVars;

import java.io.IOException;
import java.util.Properties;

/**
 * The InputFormat to use to read data from HCatalog.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class HCatRSInputFormat extends HCatRSBaseInputFormat {

  private Configuration conf;
  private InputJobInfo inputJobInfo;

  /**
   * Initializes the input with a provided filter.
   * See {@link #setInput(Configuration, String, String, String)}
   */
  public static HCatRSInputFormat setInput(
          Job job, String dbName, String tableName, String filter)
    throws IOException {
    RecordServiceConfig.setInputTable(job.getConfiguration(), dbName, tableName);
    Request request = null;
    Credentials credentials = job.getCredentials();
    RecordServicePlannerClient planner = null;
    try {
      RecordServicePlannerClient.Builder builder =
              new RecordServicePlannerClient.Builder();
      String kerberosPrincipal =
              job.getConfiguration().get(ConfVars.KERBEROS_PRINCIPAL_CONF.name);
      builder.setKerberosPrincipal(kerberosPrincipal);
      List<NetworkAddress> plannerHostPorts = RecordServiceConfig.getPlannerHostPort(
              job.getConfiguration().get(ConfVars.PLANNER_HOSTPORTS_CONF.name,
                      RecordServiceConfig.DEFAULT_PLANNER_HOSTPORTS));
      Exception lastException = null;
      for (int i = 0; i < plannerHostPorts.size(); ++i) {
        NetworkAddress hostPort = plannerHostPorts.get(i);
        try {
          planner = builder.connect(hostPort.hostname, hostPort.port);
        } catch (RecordServiceException e) {
          // Ignore, try next host. The errors in builder should be sufficient.
          lastException = e;
        } catch (IOException e) {
          // Ignore, try next host. The errors in builder should be sufficient.
          lastException = e;
        }
      }
      if(lastException != null){
        throw new IOException(
                "Could not connect to any of the configured planners.", lastException);
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
    job.getConfiguration().setLong("pig.maxCombinedSplitSize", 1);
    job.setInputFormatClass(HCatRSInputFormat.class);
    return setInput(job.getConfiguration(), dbName, tableName, filter);
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

}
