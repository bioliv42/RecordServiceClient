// Confidential Cloudera Information: Covered by NDA.
package com.cloudera.recordservice.avro.example;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;

import com.cloudera.recordservice.mr.RecordServiceConfig;

// TODO: rename this package to mr-example.
public class WordCount {
  public static class Map extends MapReduceBase
      implements Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    public void map(LongWritable key, Text value,
         OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);
      while (tokenizer.hasMoreTokens()) {
        word.set(tokenizer.nextToken());
        output.collect(word, one);
      }
    }
  }

  public static class Reduce extends MapReduceBase
      implements Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text key, Iterator<IntWritable> values,
        OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
      int sum = 0;
      while (values.hasNext()) {
        sum += values.next().get();
      }
      output.collect(key, new IntWritable(sum));
    }
  }

  public static void main(String[] args) throws Exception {
    String input = "";
    String outputPath = "";
    if (args.length != 2) {
      System.err.println("Usage: WordCount <input path> <output path>");
      System.exit(-1);
    }
    input = args[0];
    outputPath = args[1];
    input = input.trim();

    JobConf conf = new JobConf(WordCount.class);
    conf.setJobName("wordcount");

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);

    conf.setMapperClass(Map.class);
    conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);

    conf.setInputFormat(com.cloudera.recordservice.mapred.TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    // Set the request type based on the input string.
    //  - starts with "select" - assume it is a query
    //  - starts with "/" - assume it is a path
    //  - otherwise, assume it is a db.table (TODO: validate this).
    // TODO: this seems generally useful, move it to the library.
    if (input.toLowerCase().startsWith("select")) {
      RecordServiceConfig.setInputQuery(conf, input);
    } else if (input.startsWith("/")) {
      FileInputFormat.setInputPaths(conf, new Path(input));
    } else {
      RecordServiceConfig.setInputTable(conf, null, input);
    }
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    JobClient.runJob(conf);
    System.out.println("Done");
  }
}
