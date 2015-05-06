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
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.cloudera.recordservice.client.RecordServicePlannerClient;
import com.cloudera.recordservice.client.Request;
import com.cloudera.recordservice.mr.Schema;
import com.cloudera.recordservice.mr.TaskInfo;
import com.cloudera.recordservice.thrift.TPlanRequestResult;
import com.cloudera.recordservice.thrift.TSchema;
import com.cloudera.recordservice.thrift.TTask;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * The base RecordService input format that handles funcationality common to
 * all RecordService InputFormats.
 *
 * TODO: this input format should subsume the functionality of all the
 * input formats we want to work with. This means respecting the configs
 * from all of them (or the ones from them we care about). These include:
 *   - FileInputFormat
 *   - Avro
 *   - HCat
 *   - ?
 */
public abstract class RecordServiceInputFormatBase<K, V> extends InputFormat<K, V> {
  // The fully qualified table name to read.
  public final static String TBL_NAME_CONF = "recordservice.table.name";

  // The subset of columns to read.
  public final static String COL_NAMES_CONF = "recordservice.col.names";


  public final static String PLANNER_HOST = "recordservice.planner.host";
  public final static String PLANNER_PORT = "recordservice.planner.port";

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException,
      InterruptedException {
    Configuration jobConf = context.getConfiguration();
    return getSplits(jobConf);
  }

  /**
   * Looks inside the jobConf to construct the RecordService request. The
   * request can either be a sql statement, table or path.
   */
  public static List<InputSplit> getSplits(Configuration jobConf) throws IOException {
    String tblName = jobConf.get(TBL_NAME_CONF);
    String inputDir = jobConf.get(FileInputFormat.INPUT_DIR);

    if (tblName == null && inputDir == null) {
      throw new IllegalArgumentException("No input specified. Specify either '" +
          TBL_NAME_CONF + "' or '" + FileInputFormat.INPUT_DIR + "'");
    }
    if (tblName != null && inputDir != null) {
      throw new IllegalArgumentException("Cannot specify both '" +
          TBL_NAME_CONF + "' and '" + FileInputFormat.INPUT_DIR + "'");
    }

    String[] colNames = jobConf.getStrings(COL_NAMES_CONF, new String[0]);
    if (inputDir != null && colNames.length > 0) {
      // TODO: support this.
      throw new IllegalArgumentException(
          "Input specified by path and column projections cannot be used together.");
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
    } else {
      Preconditions.checkState(false);
    }

    TPlanRequestResult result = null;
    try {
      result = RecordServicePlannerClient.planRequest(
          jobConf.get(PLANNER_HOST, "localhost"),
          jobConf.getInt(PLANNER_PORT, 40000),
          request);
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
}
