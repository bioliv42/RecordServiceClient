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

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.cloudera.recordservice.avro.example.JobQueue.RSMRJob;

/**
 * This class submits batches of jobs to the mini cluster RecordService using
 * the JobQueue object.
 */
public class MiniClusterLoadTest {
  public static final int DEFAULT_CLUSTER_NODE_NUM = 3;
  public static final int DEFAULT_WORKER_THREAD_NUM = 3;
  MiniClusterController miniCluster_;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    org.apache.log4j.BasicConfigurator.configure();
    MiniClusterController.Start(DEFAULT_CLUSTER_NODE_NUM);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

  @Before
  public void setUp() throws Exception {
    miniCluster_ = MiniClusterController.instance();
    assertTrue(miniCluster_.isClusterStateCorrect());
  }

  @After
  public void tearDown() throws Exception {
  }

  /**
   * This test submits a simple batch of MR jobs to the mini cluster, executes
   * them, and checks to ensure that all of the jobs complete.
   */
  @Test
  public <T> void testBasicBatchExecution() throws IOException, InterruptedException {
    JobQueue jobQ = new JobQueue(DEFAULT_WORKER_THREAD_NUM);
    ArrayList<RSMRJob> jobList = new ArrayList<RSMRJob>();
    for (int i = 0; i < 5; ++i) {
      jobList.add(jobQ.new RSMRJob(TestMiniClusterController.createWordCountMRJobConf()));
    }
    jobQ.addJobsToQueue((Collection<? extends Callable<T>>) jobList);
    assertTrue(jobQ.checkCompleted());
  }

}
