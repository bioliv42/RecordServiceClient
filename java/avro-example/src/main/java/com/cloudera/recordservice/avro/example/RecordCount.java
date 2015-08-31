// Confidential Cloudera Information: Covered by NDA.
// Copyright 2015 Cloudera Inc.
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

package com.cloudera.recordservice.avro.example;

import java.io.IOException;
import java.util.Iterator;

import com.cloudera.recordservice.mr.RecordServiceConfig;
import com.cloudera.recordservice.mr.RecordServiceRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;

/**
 * MapReduce application that just counts the number of records in the results
 * returned by the input query.
 */
public class RecordCount {
  public static class Map extends MapReduceBase
      implements Mapper<NullWritable, RecordServiceRecord, NullWritable, LongWritable> {
    private final static LongWritable ONE = new LongWritable(1);
    private final static NullWritable NULL = NullWritable.get();

    @Override
    public void map(NullWritable key, RecordServiceRecord value,
        OutputCollector<NullWritable, LongWritable> output, Reporter reporter)
        throws IOException {
      output.collect(NULL, ONE);
    }
  }

  public static class Reduce extends MapReduceBase
      implements Reducer<NullWritable, LongWritable, NullWritable, LongWritable> {
    @Override
    public void reduce(NullWritable key, Iterator<LongWritable> values,
        OutputCollector<NullWritable, LongWritable> output, Reporter reporter)
        throws IOException {
      long sum = 0;
      while (values.hasNext()) {
        sum += values.next().get();
      }
      output.collect(key, new LongWritable(sum));
    }
  }

  public static void main(String[] args) throws IOException {
    if (args.length != 2) {
      System.err.println("Usage: RecordCount <input_query> <output_path>");
      System.exit(1);
    }
    String inputQuery = args[0];
    String output = args[1];

    JobConf conf = new JobConf(RecordCount.class);
    conf.setJobName("recordcount");
    RecordServiceConfig.setInputQuery(conf, inputQuery);

    conf.setOutputKeyClass(NullWritable.class);
    conf.setOutputValueClass(LongWritable.class);

    conf.setInt("mapreduce.job.reduces", 1);
    conf.setMapperClass(Map.class);
    conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);

    conf.setInputFormat(com.cloudera.recordservice.mapred.RecordServiceInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    FileSystem fs = FileSystem.get(conf);
    Path outputPath = new Path(output);
    if (fs.exists(outputPath)) fs.delete(outputPath, true);
    FileOutputFormat.setOutputPath(conf, outputPath);

    JobClient.runJob(conf);
  }
}
