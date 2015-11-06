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

package com.cloudera.recordservice.tests;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;

/**
 * This class controls the cluster during tests, both the minicluster and a
 * real cluster. To control the minicluster commands are run through the
 * MiniClusterController class but using this class as an api allows the tests
 * to be cluster agnostic.
 */
public class ClusterController {
  public static final int DEFAULT_NUM_NODES = 3;

  public final boolean USE_MINI_CLUSTER;

  public MiniClusterController miniCluster_;
  public List clusterList_;
  public List activeNodes_;
  public List availableNodes_;

  /**
   * If a miniCluster is being used, this class simply instantiates a
   * MiniClusterController.
   *
   * TODO: Future work involves doing the necessary steps to ensure that a
   * cluster is healthy and ready for RecordService jobs to be executed.
   */
  public ClusterController(boolean miniCluster) {
    USE_MINI_CLUSTER = miniCluster;
    try {
      if (USE_MINI_CLUSTER) {
        MiniClusterController.Start(DEFAULT_NUM_NODES);
        miniCluster_ = MiniClusterController.instance();
      } else {
        throw new NotImplementedException();
      }
    } catch (InterruptedException e) {
      System.out.println("Error starting mini cluster");
      e.printStackTrace();
      System.exit(1);
    }

  }

  /**
   * This method runs the given job as specified in the JobConf on the cluster
   */
  public RunningJob runJob(JobConf mrJob) throws IOException {
    if (USE_MINI_CLUSTER) {
      return miniCluster_.runJobLocally(mrJob);
    } else {
      return JobClient.runJob(mrJob);
    }
  }

  /**
   * This method adds a node to the cluster. In the case of the minicluster this
   * is a very straightforward procedure, a recordserviced is brought up.
   *
   * TODO: Future work is required to make this method work on a real cluster.
   * In that case this method would add a node that was already in the cluster
   * but had previously been disabled. If there were no such node, this method
   * would not do anything.
   */
  public void addNode() {
    if (USE_MINI_CLUSTER) {
      miniCluster_.addRecordServiced();
    } else {
      throw new NotImplementedException();
    }
  }

  /**
   * This class represents a node in a cluster. It contains basic information
   * such as hostname and open ports.
   */
  public static class ClusterNode {
    public String hostname_;
    public int workerPort_;
    public int plannerPort_;
    public int webserverPort_;

    public ClusterNode(String hostname) {
      hostname_ = hostname;
    }

    public ClusterNode(String hostname, int workerPort, int plannerPort, int webserverPort) {
      hostname_ = hostname;
      workerPort_ = workerPort;
      plannerPort_ = plannerPort;
      webserverPort_ = webserverPort;
    }
  }
}
