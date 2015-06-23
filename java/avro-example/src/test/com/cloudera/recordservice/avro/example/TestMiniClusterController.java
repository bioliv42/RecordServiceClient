package com.cloudera.recordservice.avro.example;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.cloudera.recordservice.avro.example.WordCount.Map;
import com.cloudera.recordservice.avro.example.WordCount.Reduce;

public class TestMiniClusterController {
  public static final int DEFAULT_NODE_NUM = 3;
  static Random rand_ = new Random();
  MiniClusterController miniCluster_;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    org.apache.log4j.BasicConfigurator.configure();
    MiniClusterController.Start(DEFAULT_NODE_NUM);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

  @Before
  public void setUp() throws Exception {
    miniCluster_ = MiniClusterController.instance();
    assert miniCluster_.isClusterStateCorrect() : "Cluster is in incorrect state";
  }

  @After
  public void tearDown() throws Exception {
  }

  public JobConf createWordCountMRJobConf() throws IOException {
    JobConf conf = new JobConf(WordCount.class);
    fillInWordCountMRJobConf(conf);
    return conf;
  }

  public static void setRandomOutputDir(JobConf conf) {
    Integer intSuffix = rand_.nextInt(10000000);
    String suffix = intSuffix.toString();
    String outDir = "/tmp/" + conf.getJobName() + "_" + suffix;
    System.out.println("outdir: " + outDir);
    FileOutputFormat.setOutputPath(conf, new Path(outDir));
  }

  // TODO: Move this and the following test case to a test library
  public static void fillInWordCountMRJobConf(JobConf conf) {
    String input = "select n_comment from tpch.nation";

    conf.setJobName("samplejob-wordcount");

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);

    conf.setMapperClass(Map.class);
    conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);

    conf.setInputFormat(com.cloudera.recordservice.mapred.TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);
    com.cloudera.recordservice.mr.RecordServiceConfig.setInputQuery(conf,
        input);
  }

  /**
   * This method creates a sample MR job and submits that JobConf object to the
   * static MiniClusterController method to be executed.
   */
  @Test
  public void testRunningJobLocally() throws IOException, InterruptedException {
    JobConf sampleJob = createWordCountMRJobConf();
    setRandomOutputDir(sampleJob);
    RunningJob runningJob = miniCluster_.runJobLocally(sampleJob);
    runningJob.waitForCompletion();
    assert runningJob.isSuccessful() : "Job was not completed successfully";
  }

  /**
   * This method gets a JobConf object from the static MiniClusterController
   * method, fills it with a sample MR job and then executes the job.
   */
  @Test
  public void testGetConfigForMiniCluster() throws IOException {
    JobConf sampleJob = miniCluster_.getJobConf(WordCount.class);
    fillInWordCountMRJobConf(sampleJob);
    setRandomOutputDir(sampleJob);
    RunningJob runningJob = JobClient.runJob(sampleJob);
    runningJob.waitForCompletion();
    assert runningJob.isSuccessful() : "Job was not completed successfully";
  }

  /**
   * This method kills a node in a cluster and then checks to make sure that the
   * cluster state is correct.
   */
  @Test
  public void testClusterHealth() throws IOException {
    assert miniCluster_.isClusterStateCorrect() : "Cluster is in incorrect state";
    miniCluster_.killRandomNode();
    assert miniCluster_.isClusterStateCorrect() : "Cluster is in incorrect state";
  }
}
