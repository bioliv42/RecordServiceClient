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

import com.cloudera.recordservice.hcatalog.common.HCatRSUtil;
import com.cloudera.recordservice.mapred.RecordServiceInputFormat;
import com.cloudera.recordservice.mr.RecordServiceRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/** The HCat wrapper for the underlying RecordReader,
 * this ensures that the initialize on
 * the underlying record reader is done with the underlying split,
 * not with HCatSplit.
 */
class HCatRecordReader extends RecordReader<WritableComparable, RecordServiceRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(HCatRecordReader.class);

  private InputErrorTracker errorTracker;

  WritableComparable currentKey;
  RecordServiceRecord currentValue;
  RecordServiceRecord currentRecord;

  /** The underlying record reader to delegate to. */
  private RecordServiceInputFormat.RecordServiceRecordReader baseRecordReader;

  /** The storage handler used */
  private final HiveStorageHandler storageHandler;

  private Deserializer deserializer;

  private Map<String, Object> valuesNotInDataCols;

  private HCatSchema outputSchema = null;
  private HCatSchema dataSchema = null;

  /**
   * Instantiates a new hcat record reader.
   */
  public HCatRecordReader(HiveStorageHandler storageHandler,
              Map<String, Object> valuesNotInDataCols) {
    this.storageHandler = storageHandler;
    this.valuesNotInDataCols = valuesNotInDataCols;
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.RecordReader#initialize(
   * org.apache.hadoop.mapreduce.InputSplit,
   * org.apache.hadoop.mapreduce.TaskAttemptContext)
   */
  @Override
  public void initialize(org.apache.hadoop.mapreduce.InputSplit split,
               TaskAttemptContext taskContext) throws IOException, InterruptedException {

    HCatRSSplit hcatRSSplit = InternalUtil.castToHCatRSSplit(split);

    baseRecordReader = createBaseRecordReader(hcatRSSplit, storageHandler, taskContext);
    createDeserializer(hcatRSSplit, storageHandler, taskContext);

    // Pull the output schema out of the TaskAttemptContext
    outputSchema = (HCatSchema) HCatUtil.deserialize(
      taskContext.getConfiguration().get(HCatConstants.HCAT_KEY_OUTPUT_SCHEMA));

    if (outputSchema == null) {
      outputSchema = hcatRSSplit.getTableSchema();
    }

    // Pull the table schema out of the Split info
    // TODO This should be passed in the TaskAttemptContext instead
    dataSchema = hcatRSSplit.getDataSchema();

    errorTracker = new InputErrorTracker(taskContext.getConfiguration());
  }

  private RecordServiceInputFormat.RecordServiceRecordReader createBaseRecordReader(HCatRSSplit hcatRSSplit,
                                     HiveStorageHandler storageHandler, TaskAttemptContext taskContext) throws IOException {

    JobConf jobConf = HCatUtil.getJobConfFromContext(taskContext);

    HCatUtil.copyJobPropertiesToJobConf(hcatRSSplit.getPartitionInfo().getJobProperties(), jobConf);
    HCatRSUtil.copyCredentialsToJobConf(taskContext.getCredentials(), jobConf);
    RecordServiceInputFormat inputFormat =
      HCatRSInputFormat.getMapRedInputFormat(jobConf, storageHandler.getInputFormatClass());
    return (RecordServiceInputFormat.RecordServiceRecordReader) inputFormat.getRecordReader(hcatRSSplit.getBaseSplit(), jobConf,
      InternalUtil.createReporter(taskContext));
  }

  private void createDeserializer(HCatRSSplit hcatRSSplit, HiveStorageHandler storageHandler,
                  TaskAttemptContext taskContext) throws IOException {

    deserializer = ReflectionUtils.newInstance(storageHandler.getSerDeClass(),
      taskContext.getConfiguration());

    try {
      InternalUtil.initializeDeserializer(deserializer, storageHandler.getConf(),
              hcatRSSplit.getPartitionInfo().getTableInfo(),
              hcatRSSplit.getPartitionInfo().getPartitionSchema());
    } catch (SerDeException e) {
      throw new IOException("Failed initializing deserializer "
        + storageHandler.getSerDeClass().getName(), e);
    }
  }

  /* (non-Javadoc)
  * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentKey()
  */
  @Override
  public WritableComparable getCurrentKey()
    throws IOException, InterruptedException {
    return currentKey;
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentValue()
   */
  @Override
  public RecordServiceRecord getCurrentValue() throws IOException, InterruptedException {
    return currentRecord;
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.RecordReader#getProgress()
   */
  @Override
  public float getProgress() {
    try {
      return baseRecordReader.getProgress();
    } catch (IOException e) {
      LOG.warn("Exception in HCatRecord reader", e);
    }
    return 0.0f; // errored
  }

  /**
   * Check if the wrapped RecordReader has another record, and if so convert it into an
   * HCatRecord. We both check for records and convert here so a configurable percent of
   * bad records can be tolerated.
   *
   * @return if there is a next record
   * @throws IOException on error
   * @throws InterruptedException on error
   */
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (currentKey == null) {
      currentKey = baseRecordReader.createKey();
      currentValue = baseRecordReader.createValue();
    }

    while (baseRecordReader.next(currentKey, currentValue)) {

      currentRecord = currentValue;
      return true;
    }

    return false;
  }

  /* (non-Javadoc)
  * @see org.apache.hadoop.mapreduce.RecordReader#close()
  */
  @Override
  public void close() throws IOException {
    baseRecordReader.close();
  }

  /**
   * Tracks number of of errors in input and throws a Runtime exception
   * if the rate of errors crosses a limit.
   * <br/>
   * The intention is to skip over very rare file corruption or incorrect
   * input, but catch programmer errors (incorrect format, or incorrect
   * deserializers etc).
   *
   * This class was largely copied from Elephant-Bird (thanks @rangadi!)
   * https://github.com/kevinweil/elephant-bird/blob/master/core/src/main/java/com/twitter/elephantbird/mapreduce/input/LzoRecordReader.java
   */
  static class InputErrorTracker {
    long numRecords;
    long numErrors;

    double errorThreshold; // max fraction of errors allowed
    long minErrors; // throw error only after this many errors

    InputErrorTracker(Configuration conf) {
      errorThreshold = conf.getFloat(HCatConstants.HCAT_INPUT_BAD_RECORD_THRESHOLD_KEY,
        HCatConstants.HCAT_INPUT_BAD_RECORD_THRESHOLD_DEFAULT);
      minErrors = conf.getLong(HCatConstants.HCAT_INPUT_BAD_RECORD_MIN_KEY,
        HCatConstants.HCAT_INPUT_BAD_RECORD_MIN_DEFAULT);
      numRecords = 0;
      numErrors = 0;
    }

    void incRecords() {
      numRecords++;
    }

    void incErrors(Throwable cause) {
      numErrors++;
      if (numErrors > numRecords) {
        // incorrect use of this class
        throw new RuntimeException("Forgot to invoke incRecords()?");
      }

      if (cause == null) {
        cause = new Exception("Unknown error");
      }

      if (errorThreshold <= 0) { // no errors are tolerated
        throw new RuntimeException("error while reading input records", cause);
      }

      LOG.warn("Error while reading an input record ("
        + numErrors + " out of " + numRecords + " so far ): ", cause);

      double errRate = numErrors / (double) numRecords;

      // will always excuse the first error. We can decide if single
      // error crosses threshold inside close() if we want to.
      if (numErrors >= minErrors && errRate > errorThreshold) {
        LOG.error(numErrors + " out of " + numRecords
          + " crosses configured threshold (" + errorThreshold + ")");
        throw new RuntimeException("error rate while reading input records crossed threshold", cause);
      }
    }
  }
}
