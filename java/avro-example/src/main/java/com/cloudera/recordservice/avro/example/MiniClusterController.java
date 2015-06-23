package com.cloudera.recordservice.avro.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;

import com.cloudera.recordservice.mr.RecordServiceConfig;

/**
 * A class that starts a minicluster locally and runs a map reduce job on the
 * cluster through JNI calls.
 *
 * This class is a singleton. It first needs to be instantiated, and then the
 * instance method returns the class instance.
 *
 * Usage: MiniClusterController.Start(num_nodes);
 * MiniClusterController miniCluster = MiniClusterController.instance();
 */
public class MiniClusterController {
  public static native void StartMiniCluster(int numNodes);

  public native void KillNodeByPid(int pid);
  public native int[] GetRunningMiniNodePids();
  public native int GetSpecificNodePid(int plannerPort);
  public native int GetStatestorePid();
  public native int GetCatalogPid();

  public static final int DEFAULT_NUM_NODES = 3;
  public static final int BASE_PORT = 30000;
  // Each mini node in the mini cluster has 6 exposed ports which are all initialized in
  // the same order offset from that node's base port; .e.g a node with a base port of
  // 30000 has a planner port of 30000 + PLANNER_PORT_OFFSET or, in this case, 30005
  public static final int PLANNER_PORT_OFFSET = 5;
  public static final String MINICLUSTER_LIBRARY = "libExternalMiniCluster.so";
  public static final String BUILT_RS_CODE_LOCATION = "/cpp/build/release/recordservice/";

  public int catalogPid_ = -1;
  public int statestorePid_ = -1;
  public List<MiniClusterNode> clusterList_;
  public Thread clusterThread_;

  private static MiniClusterController miniCluster_;

  /**
   * This method starts a minicluster with the specified number of nodes. This
   * method calls via JNI the cpp StartMiniCluster method which sleeps
   * indefinitely after starting the cluster, so this method will not return
   * unless the cluster is stopped.
   */
  private static void startMiniCluster(int numNodes) {
    String rsHome = System.getenv("RECORD_SERVICE_HOME");
    String path = rsHome + BUILT_RS_CODE_LOCATION;
    System.load(path + MINICLUSTER_LIBRARY);
    System.out.println("Number of nodes: " + numNodes);
    MiniClusterController.StartMiniCluster(numNodes);
  }

  /**
   * Every node in the local minicluster runs in its own process. This method returns a
   * hashset of all the process ids of running nodes in the mini cluster.
   */
  public HashSet<Integer> getRunningMiniNodePids() {
    HashSet<Integer> pidSet = new HashSet<Integer>();
    Collections.addAll(pidSet, ArrayUtils.toObject(GetRunningMiniNodePids()));
    return pidSet;
  }

  /**
   * This method kills the given node
   */
  public void killNode(MiniClusterNode node) {
    if (node != null) {
      KillNodeByPid(node.pid_);
    }
    clusterList_.remove(node);
  }

  /**
   * Given a planner port, this method returns the process id of the node on the local
   * minicluster
   */
  public int getSpecificNodePid(int plannerPort) {
    return GetSpecificNodePid(plannerPort);
  }

  /**
   * This method returns a randomly selected, live MiniClusterNode object
   */
  public MiniClusterNode getRandomNode() {
    // If the cluster is empty, there are no nodes to return
    if (clusterList_ == null) {
      return null;
    }
    Random r = new Random();
    return clusterList_.get(r.nextInt(clusterList_.size()));
  }

  /**
   * This method kills a randomly selected live node
   */
  public void killRandomNode() {
    killNode(getRandomNode());
  }

  /**
   * This method returns the process id of the statestore
   */
  public int getStatestorePid() {
    return GetStatestorePid();
  }

  /**
   * This method returns the process id of the catalog
   */
  public int getCatalogPid() {
    return GetCatalogPid();
  }

  /**
   * This method checks if there is a running statestore
   */
  public boolean isStatestoreAlive() {
    return getStatestorePid() > 1;
  }

  /**
   * This method checks if there is a running catalog
   */
  public boolean isCatalogAlive() {
    return getCatalogPid() > 1;
  }

  /**
   * This method returns a JobConf object that allows a map reduce job to be run
   * on the minicluster
   */
  public JobConf getJobConf(Class mrClass) {
    if (clusterList_.size() == 0) {
      System.err.println("Cannot run MR job because the cluster has no active nodes");
      return null;
    }
    JobConf conf = new JobConf(mrClass);
    conf.set(RecordServiceConfig.PLANNER_HOST_CONF, "localhost");
    conf.setInt(RecordServiceConfig.PLANNER_PORT_CONF, getRandomNode().plannerPort_);
    return conf;
  }

  /**
   * This method takes JobConf and executes it
   */
  public RunningJob runJobLocally(JobConf mrJob) throws IOException {
    if (clusterList_.size() == 0) {
      System.err.println("Cannot run MR job because the cluster has no active nodes");
      return null;
    }
    mrJob.set(RecordServiceConfig.PLANNER_HOST_CONF, "localhost");
    mrJob.setInt(RecordServiceConfig.PLANNER_PORT_CONF, getRandomNode().plannerPort_);
    System.out.println("Running Job");
    return JobClient.runJob(mrJob);
  }

  /**
   * This class represents a node in the minicluster
   */
  public class MiniClusterNode {
    public int pid_;
    public int beeswaxPort_;
    public int hs2Port_;
    public int bePort_;
    public int webserverPort_;
    public int statestoreSubscriberPort_;
    public int plannerPort_;
    public int workerPort_;

    public MiniClusterNode(int basePort, int pid) {
      pid_ = pid;
      // TODO: Do this in a more flexible way. If this is changed in the backend this
      // will break.
      beeswaxPort_ = basePort;
      hs2Port_ = basePort + 1;
      bePort_ = basePort + 2;
      webserverPort_ = basePort + 3;
      statestoreSubscriberPort_ = basePort + 4;
      plannerPort_ = basePort + 5;
      workerPort_ = basePort + 6;
    }
  }

  /**
   * This class is used to start a minicluster within its own thread
   */
  public static class ClusterRunner implements Runnable {
    private int numNodes_;

    public ClusterRunner(int numNodes) {
      numNodes_ = numNodes;
    }

    /**
     * This method is executed when a Thread, given an instance of this class,
     * calls its start method
     */
    @Override
    public void run() {
      MiniClusterController.startMiniCluster(numNodes_);
    }
  }

  /**
   * This method starts a minicluster in a new thread
   */
  private void start(int numNodes) throws InterruptedException {
    ClusterRunner cr = new ClusterRunner(numNodes);
    clusterThread_ = new Thread(cr);
    clusterThread_.start();
    System.out.println("Sleeping...");
    // The cluster takes some time to start up
    Thread.sleep(5000);
    populateFields();
  }

  /**
   * This method populates the class fields of the minicluster
   */
  private void populateFields() {
    miniCluster_ = this;
    HashSet<Integer> set = getRunningMiniNodePids();
    populateNodeList(set.size());
    catalogPid_ = getCatalogPid();
    statestorePid_ = getStatestorePid();
  }

  private void printExcessNodes(HashSet<Integer> pidSet) {
    System.err.println("Nodes with the following pids: ");
    for (Integer pid : pidSet) System.err.println(pid);
    System.err
        .println("were found but are not being tracked by the MiniClusterController");
  }

  /**
   * This method checks the current state of the MiniClusterController object
   * against the actual state of the system.
   */
  public boolean isClusterStateCorrect() {
    HashSet<Integer> pidSet = getRunningMiniNodePids();
    // Check the cluster list
    if (pidSet.size() > 0 && (clusterList_ == null || clusterList_.size() <= 0)) {
      printExcessNodes(pidSet);
      return false;
    } else {
      for (MiniClusterNode node : clusterList_) {
        if (!pidSet.contains(node.pid_)) {
          System.err.println("Node with pid = " + node.pid_
              + " was expected but not found");
          return false;
        }
        // Two nodes cannot share the same process ID
        pidSet.remove(node.pid_);
      }
      if (pidSet.size() > 0) {
        printExcessNodes(pidSet);
        return false;
      }
    }
    // Check the catalog and statestore
    int sPid = getStatestorePid();
    int cPid = getCatalogPid();
    if (sPid != statestorePid_) {
      System.err.println("Statestore pid does not match MiniClusterController value");
      return false;
    }
    if (cPid != catalogPid_) {
      System.err.println("Catalog pid does not match MiniClusterController value");
      return false;
    }
    return true;
  }

  /**
   * This method populates the node list with the live nodes found running
   */
  private void populateNodeList(int numNodes) {
    clusterList_ = new ArrayList<MiniClusterNode>(numNodes);
    for (int i = 0; i < numNodes; ++i) {
      int offset = i * 7;
      int pid = getSpecificNodePid(BASE_PORT + offset + PLANNER_PORT_OFFSET);
      if (pid == -1) {
        continue;
      }
      MiniClusterNode newNode = new MiniClusterNode(BASE_PORT + offset, pid);
      clusterList_.add(newNode);
    }
  }

  /**
   * This method instantiates the minicluster. It should only be called once. If
   * the minicluster has already been instantiated, nothing is executed.
   */
  public static void Start(int num_nodes) throws InterruptedException {
    new MiniClusterController(num_nodes);
  }

  /**
   * This method returns the instantiated instance of MiniClusterController. If
   * the MiniClusterController has not been instantiated, it returns null.
   */
  public static MiniClusterController instance() {
    return miniCluster_;
  }

  /**
   * This constructor returns a MiniClusterController object. Note that method
   * does not start a minicluster. To start a cluster, miniCluster.start()
   * should be called.
   */
  private MiniClusterController(int numNodes) throws InterruptedException {
    start(numNodes);
    populateFields();
  }

  public static void main(String[] args) throws InterruptedException, IOException, NumberFormatException {
    org.apache.log4j.BasicConfigurator.configure();
    int numNodes = DEFAULT_NUM_NODES;
    if (args.length == 1) {
      numNodes = Integer.parseInt(args[0]);
    }
    MiniClusterController.Start(numNodes);
    MiniClusterController miniCluster = MiniClusterController.instance();
    miniCluster.getRunningMiniNodePids();
    System.out.println(miniCluster.isClusterStateCorrect());
  }
}