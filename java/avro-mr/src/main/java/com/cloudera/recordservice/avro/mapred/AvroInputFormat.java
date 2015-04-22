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

package com.cloudera.recordservice.avro.mapred;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import com.cloudera.recordservice.avro.SpecificRecords;
import com.cloudera.recordservice.avro.SpecificRecords.ResolveBy;
import com.cloudera.recordservice.mapred.RecordServiceInputFormatBase;
import com.cloudera.recordservice.mapred.RecordServiceInputSplit;
import com.cloudera.recordservice.mr.RecordReaderCore;
import com.cloudera.recordservice.thrift.TRecordServiceException;

/**
 * Input format which provides identical functionality to
 * org.apache.mapred.AvroInputFormat
 *
 * TODO: support projection
 */
public class AvroInputFormat<T> extends
    RecordServiceInputFormatBase<AvroWrapper<T>, NullWritable> {
  public static String INPUT_FORMAT_CLASS_CONF_KEY = "mapred.input.format.class";

  @Override
  public RecordReader<AvroWrapper<T>, NullWritable> getRecordReader(
      InputSplit split, JobConf job, Reporter reporter)
      throws IOException {
    return new AvroRecordReader<T>(job, (RecordServiceInputSplit)split);
  }

  /**
   * Implementation of the reader. This just reads records from the RecordService
   * and returns them as Avro objects of type T.
   */
  private static class AvroRecordReader<T>
      implements RecordReader<AvroWrapper<T>, NullWritable> {
    private RecordReaderCore reader_;
    private SpecificRecords<T> records_;

    public AvroRecordReader(JobConf config, RecordServiceInputSplit split)
        throws IOException {
      try {
        reader_ = new RecordReaderCore(config, split.getBackingSplit().getTaskInfo());
      } catch (Exception e) {
        throw new IOException("Failed to execute task.", e);
      }
      Schema schema = AvroJob.getInputSchema(config);
      assert(schema != null);

      // FIXME: this is only right if T is a specific record. Re-think our avro package
      // object hierarchy to look more like avros?
      records_ = new SpecificRecords<T>(schema, reader_.records(), ResolveBy.NAME);
    }

    public AvroWrapper<T> createKey() {
      return new AvroWrapper<T>(null);
    }

    public NullWritable createValue() { return NullWritable.get(); }

    public boolean next(AvroWrapper<T> wrapper, NullWritable ignore)
        throws IOException {
      try {
        if (!records_.hasNext()) return false;
        wrapper.datum(records_.next());
        return true;
      } catch (TRecordServiceException e) {
        throw new IOException("Could not get next record.", e);
      }
    }

    public float getProgress() throws IOException {
      if (reader_ == null) return 0;
      return reader_.records().progress();
    }

    public long getPos() throws IOException {
      return 0;
    }

    public void close() throws IOException {
      if (reader_ != null) reader_.close();
    }
  }
}