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
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.cloudera.recordservice.client.RecordServicePlannerClient;
import com.cloudera.recordservice.client.Request;
import com.cloudera.recordservice.thrift.TPlanRequestResult;
import com.cloudera.recordservice.thrift.TSchema;
import com.cloudera.recordservice.thrift.TTask;
import com.google.common.base.Joiner;

/**
 * The InputFormat class to be used for Map Reduce applications that
 * need to use the Record Service.
 */
public class RecordServiceInputFormat extends
    InputFormat<WritableComparable<?>, RecordServiceRecord>{

  public final static String DB_NAME_CONF = "recordservice.db.name";
  public final static String TBL_NAME_CONF = "recordservice.table.name";
  public final static String COL_NAMES_CONF = "recordservice.col.names";

  // Optional configuration option for performance tuning that configures
  // the number of max number of records returned when fetching results from
  // the RecordService. If not set, server default will be used.
  // TODO: It would be nice for the server to adjust this automatically based
  // on how fast the client is able to process records.
  public final static String FETCH_SIZE_CONF = "recordservice.fetch.size";

  public final static String PLANNER_HOST = "recordservice.planner.host";
  public final static String PLANNER_PORT = "recordservice.planner.port";

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException,
      InterruptedException {
    Configuration jobConf = context.getConfiguration();
    return getSplits(jobConf);
  }

  public List<InputSplit> getSplits(Configuration jobConf) throws IOException {
    String dbName = jobConf.get(DB_NAME_CONF, "default");
    String tblName = jobConf.get(TBL_NAME_CONF);
    if (tblName == null) {
      throw new IllegalArgumentException(TBL_NAME_CONF + " not specified.");
    }
    // If length of colNames = 0, return all possible columns
    String[] colNames = jobConf.getStrings(COL_NAMES_CONF, new String[0]);

    String query =
        new StringBuilder("SELECT ")
            .append(colNames.length == 0 ? "*" : Joiner.on(',').join(colNames))
            .append(" FROM ")
            .append(dbName).append('.').append(tblName)
            .toString();

    TPlanRequestResult result = null;
    try {
      result = RecordServicePlannerClient.planRequest(
          jobConf.get(PLANNER_HOST, "localhost"),
          jobConf.getInt(PLANNER_PORT, 40000),
          Request.createSqlRequest(query));
    } catch (Exception e) {
      throw new IOException(e);
    }
    TSchema tSchema = result.getSchema();
    List<InputSplit> splits = new ArrayList<InputSplit>();
    for (TTask tTask : result.getTasks()) {
      splits.add(new RecordServiceInputSplit(
          new Schema(tSchema), new TaskInfo(tTask)));
    }
    return splits;
  }

  @Override
  public RecordReader<WritableComparable<?>, RecordServiceRecord>
      createRecordReader(InputSplit split, TaskAttemptContext context)
          throws IOException, InterruptedException {
    RecordServiceRecordReader rReader = new RecordServiceRecordReader();
    rReader.initialize(split, context);
    return rReader;
  }

}
