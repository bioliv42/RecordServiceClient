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

package com.cloudera.recordservice.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.recordservice.core.NetworkAddress;
import com.cloudera.recordservice.core.RecordServiceException;
import com.cloudera.recordservice.core.RecordServicePlannerClient;
import com.cloudera.recordservice.core.Request;
import com.cloudera.recordservice.mr.security.DelegationTokenIdentifier;
import com.cloudera.recordservice.mr.security.TokenUtils;

/**
 * Utilities to communicate with the planner.
 */
public class PlanUtil {
  private final static Logger LOG = LoggerFactory.getLogger(PlanUtil.class);

   /**
   * Generates a request from the configs set in jobConf.
   */
  public static Request getRequest(Configuration jobConf) throws IOException {
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
          if (c == null || c.isEmpty()) {
            throw new IllegalArgumentException(
                "Cannot specify projection with null or empty column name.");
          }
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
    return request;
  }

  /**
   * Returns a connected planner client from the jobConf. The caller needs to close
   * the planner.
   */
  @SuppressWarnings("unchecked")
  public static RecordServicePlannerClient getPlanner(
      List<NetworkAddress> plannerHostPorts,
      String kerberosPrincipal,
      Credentials credentials) throws IOException {
    RecordServicePlannerClient.Builder builder =
        new RecordServicePlannerClient.Builder();
    // Try to get the delegation token from the credentials. If it is there, use it.
    Token<DelegationTokenIdentifier> delegationToken = null;
    if (credentials != null) {
      delegationToken = (Token<DelegationTokenIdentifier>) credentials.getToken(
            DelegationTokenIdentifier.DELEGATION_KIND);
    }

    if (delegationToken != null) {
      builder.setDelegationToken(TokenUtils.toDelegationToken(delegationToken));
    } else if (kerberosPrincipal != null) {
      builder.setKerberosPrincipal(kerberosPrincipal);
    }

    // Try all the host ports in order.
    // TODO: we can randomize the list for load balancing but it might be more
    // efficient to be sticky (hotter cache, etc).
    RecordServicePlannerClient planner = null;
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
    if (planner == null) {
      throw new IOException(
          "Could not connect to any of the configured planners.", lastException);
    }
    return planner;
  }
}
