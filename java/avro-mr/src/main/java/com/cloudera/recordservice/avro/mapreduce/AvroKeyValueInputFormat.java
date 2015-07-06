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

package com.cloudera.recordservice.avro.mapreduce;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.recordservice.avro.GenericRecords;
import com.cloudera.recordservice.mapreduce.RecordServiceInputFormatBase;
import com.cloudera.recordservice.mapreduce.RecordServiceInputSplit;
import com.cloudera.recordservice.mr.RecordReaderCore;
import com.cloudera.recordservice.thrift.TRecordServiceException;

/**
 * Input format which provides identical functionality to
 * org.apache.mapreduce.AvroKeyValueInputFormat
 *
 * TODO: this is not implemented.
 */
public class AvroKeyValueInputFormat<K, V> extends
    RecordServiceInputFormatBase<AvroKey<K>, AvroValue<V> > {

  private static final Logger LOG = LoggerFactory.getLogger(AvroKeyValueInputFormat.class);

  @Override
  public RecordReader<AvroKey<K>, AvroValue<V>> createRecordReader(
      InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    Schema keyReaderSchema = AvroJob.getInputKeySchema(context.getConfiguration());
    if (null == keyReaderSchema) {
      LOG.warn("Key reader schema was not set. " +
          "Use AvroJob.setInputKeySchema() if desired.");
      LOG.info("Using a key reader schema equal to the writer schema.");
    }
    Schema valueReaderSchema = AvroJob.getInputValueSchema(context.getConfiguration());
    if (null == valueReaderSchema) {
      LOG.warn("Value reader schema was not set. " +
          "Use AvroJob.setInputValueSchema() if desired.");
      LOG.info("Using a value reader schema equal to the writer schema.");
    }
    return new AvroKeyValueRecordReader<K, V>(keyReaderSchema, valueReaderSchema);
  }

  /**
   * Reads records from the RecordService, returning them as Key,Value pairs.
   * The record returned from the RecordSerive is the union of the key & value schemas.
   *
   * <p>The contents of the 'key' field will be parsed into an AvroKey object.
   * The contents of the 'value' field will be parsed into an AvroValue object.
   * </p>
   *
   * @param <K> The type of the Avro key to read.
   * @param <V> The type of the Avro value to read.
   */
  private static class AvroKeyValueRecordReader<K, V>
      extends RecordReader<AvroKey<K>, AvroValue<V>> {
    RecordReaderCore reader_;

    // The schema of the returned records.
    private final Schema keySchema_;
    private final Schema valueSchema_;

    // Records to return.
    private GenericRecords records_;

    /** The current key the reader is on. */
    private final AvroKey<K> currentKey_;

    /** The current value the reader is on. */
    private final AvroValue<V> currentValue_;

    private TaskAttemptContext context_;

    /**
     * Constructor.
     */
    public AvroKeyValueRecordReader(Schema keySchema, Schema valueSchema) {
      keySchema_ = keySchema;
      valueSchema_ = valueSchema;
      currentKey_ = new AvroKey<K>(null);
      currentValue_ = new AvroValue<V>(null);

      // FIXME
      // This is a bit tricky. We want the RecordService to return the union of the
      // key and value schema and then populate key/value from that.
      throw new RuntimeException("Not implemented");
    }

    /** {@inheritDoc} */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      try {
        if (!records_.hasNext()) return false;
        //mCurrentRecord.datum(records_.next());
        return true;
      } catch (TRecordServiceException e) {
        throw new IOException("Could not fetch record.", e);
      }
    }

    /** {@inheritDoc} */
    @Override
    public AvroKey<K> getCurrentKey() throws IOException, InterruptedException {
      return currentKey_;
    }

    /** {@inheritDoc} */
    @Override
    public AvroValue<V> getCurrentValue() throws IOException, InterruptedException {
      return currentValue_;
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context)
        throws IOException, InterruptedException {
      RecordServiceInputSplit split = (RecordServiceInputSplit)inputSplit;
      Configuration config = context.getConfiguration();

      try {
        reader_ = new RecordReaderCore(config, split.getTaskInfo());
      } catch (Exception e) {
        throw new IOException("Failed to execute task.", e);
      }
      //records_ = new SpecificRecords<T>(avroSchema_, records, ResolveBy.NAME);
      context_ = context;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      if (reader_ == null) return 0;
      return reader_.records().progress();
    }

    @Override
    public void close() throws IOException {
      if (reader_ != null) {
        assert(context_ != null);
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
  }
}