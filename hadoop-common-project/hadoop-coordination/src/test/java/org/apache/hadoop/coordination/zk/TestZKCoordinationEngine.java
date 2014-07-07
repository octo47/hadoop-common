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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.coordination.zk;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.primitives.Longs;

/**
 * Test for {@link ZKCoordinationEngine}.
 */
public class TestZKCoordinationEngine {
  private static final Log LOG =
    LogFactory.getLog(ZKCoordinationEngine.class);

  private Configuration conf;
  private String proposerNodeId = "node1";
  private MiniZooKeeperCluster zkCluster;

  @Before
  public void beforeTest() throws Exception {
    conf = new Configuration();
    conf.set(ZKConfigKeys.CE_ZK_NODE_ID_KEY, proposerNodeId);
  }

  @After
  public void after() throws Exception {
    // Shutdown ZK minicluster after each test
    if (zkCluster != null) {
      zkCluster.shutdown();
    }

    // Coordination Engine deliberately doesn't provide API
    // to rewind current GSN backwards or reset it, but just for tests we
    // need it as Coordination Engine has some singleton state.
    Class<?> clazz = ZKCoordinationEngine.class;
    Field field = clazz.getDeclaredField("currentGSN");
    field.setAccessible(true);
    field.set(null, ZKCoordinationEngine.INVALID_GSN);
  }

  /**
   * Submits single proposal and verifies that agreement was reached.
   */
  @Test
  public void testSimpleProposals() throws IOException, KeeperException,
      InterruptedException {

    zkCluster = new MiniZooKeeperCluster();
    zkCluster.setDefaultClientPort(3000);
    zkCluster.startup(
        new File(System.getProperty("test.build.dir", "target/test-dir"),
            "testSimpleProposals"), 1);

    ZKCoordinationEngine cEngine = new ZKCoordinationEngine(conf);
    SampleLearner myLearner = new SampleLearner();
    cEngine.registerHandler(new SampleHandler(myLearner));
    cEngine.start();

    SampleProposal scp = new SampleProposal(proposerNodeId);
    scp.setCurrentUser();
    cEngine.submitProposal(scp, true);

    while (cEngine.getGlobalSequenceNumber() == 0) {
      LOG.info("Waiting for coordination engine to learn agreement");
      Thread.sleep(100);
    }

    assertEquals("Coordination Engine GSN hasn't been updated properly",
      1, cEngine.getGlobalSequenceNumber());

    // check state in ZK
    ZooKeeper zk = new ZooKeeper(
      ZKConfigKeys.CE_ZK_QUORUM_DEFAULT,
      ZKConfigKeys.CE_ZK_SESSION_TIMEOUT_DEFAULT, cEngine);

    String nodeGlobalSeqNumZNodePath =
      ZKCoordinationEngine.getGlobalSequenceNumberZNodePath(
        cEngine.getLocalNodeId());
    byte[] data = zk.getData(nodeGlobalSeqNumZNodePath, cEngine, null);
    long zooKeeperglobalSeqNum = Longs.fromByteArray(data);
    long cEngineGlobalSeqNum = cEngine.getGlobalSequenceNumber();
    assertEquals("GSN stored in ZooKeeper did not match in-memory" +
      " CoordinationEngine GSN.", zooKeeperglobalSeqNum, cEngineGlobalSeqNum);

    List<String> agreementZnodes = zk.getChildren("/", null);
    int numAgreementZnodes = 0;

    for (String s : agreementZnodes) {
      if (s.startsWith(ZKConfigKeys.CE_ZK_AGREEMENT_ZNODE_PATH_DEFAULT.
          substring(1))) {
        numAgreementZnodes++;
      }
    }
    assertEquals("Total number of agreements in ZooKeeper is wrong:",
      1, numAgreementZnodes);
  }

  /**
   * Runs number of threads submitting proposals, validates they get processed
   * by Coordination Engine.
   */
  @Test
  public void testMultipleWriters() throws IOException, KeeperException,
      InterruptedException {

    zkCluster = new MiniZooKeeperCluster();
    zkCluster.setDefaultClientPort(3000);
    zkCluster.startup(
        new File(System.getProperty("test.build.dir", "target/test-dir"),
            "testMultipleWriters"), 1);


    ZKCoordinationEngine cEngine = new ZKCoordinationEngine(conf);
    SampleLearner myLearner = new SampleLearner();
    cEngine.registerHandler(new SampleHandler(myLearner));
    cEngine.start();

    Thread.sleep(200);

    final int totalAgreements = 1000;
    final int totalClients = 64;
    int approxMkdirsPerClient = totalAgreements / totalClients;
    int extrasToMake = (totalAgreements - (approxMkdirsPerClient * totalClients));
    AgreementsThread[] clients = new AgreementsThread[totalClients];
    for(int i = 0; i < clients.length; i++) {
      if(extrasToMake > 0)
        clients[i] = new AgreementsThread(cEngine, approxMkdirsPerClient + 1);
      else
        clients[i] = new AgreementsThread(cEngine, approxMkdirsPerClient);
      extrasToMake--;
      clients[i].start();
    }
    long startingAgreements = cEngine.getGlobalSequenceNumber();
    while(true) {
      boolean doneInitiazling = true;
      for (AgreementsThread client : clients) {
        doneInitiazling = doneInitiazling && client.initialized();
      }
      if(doneInitiazling)
        break;
    }
    AgreementsThread.launch();
    for(AgreementsThread client : clients) {
      client.join();
    }

    while(cEngine.getGlobalSequenceNumber() <
          startingAgreements + totalAgreements) {
      LOG.info("Waiting execution of agreements at GSN = " +
          cEngine.getGlobalSequenceNumber());
      Thread.sleep(200);
    }
    assertTrue("Failed to see all expected agreements.",
        cEngine.getGlobalSequenceNumber() >=
        startingAgreements + totalAgreements);

    assertTrue(cEngine.getGlobalSequenceNumber() > 0);
    ZooKeeper zk = new ZooKeeper(
      ZKConfigKeys.CE_ZK_QUORUM_DEFAULT,
      ZKConfigKeys.CE_ZK_SESSION_TIMEOUT_DEFAULT, cEngine);
    String nodeGlobalSeqNumZNodePath =
      ZKCoordinationEngine.getGlobalSequenceNumberZNodePath(
        cEngine.getLocalNodeId());
    byte[] data = zk.getData(nodeGlobalSeqNumZNodePath, cEngine, null);
    long zooKeeperglobalSeqNum = Longs.fromByteArray(data);
    long cEngineGlobalSeqNum = cEngine.getGlobalSequenceNumber();
    assertEquals("ZooKeeper GSN did not match CoordinationEngine GSN.",
      zooKeeperglobalSeqNum, cEngineGlobalSeqNum);

    List<String> agreementNodes =  zk.getChildren("/", null);
    int numAgreementNodes = 0;

    for (String s : agreementNodes) {
      if (s.startsWith(ZKConfigKeys.CE_ZK_AGREEMENT_ZNODE_PATH_DEFAULT.
          substring(1))) {
        numAgreementNodes++;
      }
    }
    assertEquals("Total number of agreements in ZooKeeper is wrong:",
      totalAgreements, numAgreementNodes);
    assertEquals("Bad learner state.", totalAgreements, myLearner.getState());
  }

  /**
   * Helper thread submitting proposals to Coordination Engine.
   */
  private static class AgreementsThread extends Thread {
    private String proposerNodeId = "node1";
    private int operations;
    private boolean initialized;
    private static volatile boolean launched;
    private static ZKCoordinationEngine ce;

    public AgreementsThread(ZKCoordinationEngine cEngine, int operations) {
      this.operations = operations;
      this.initialized = false;
      launched = false;
      ce = cEngine;
    }

    public boolean initialized() {
      return initialized;
    }

    @Override
    public void run() {
      initialized = true;
      while(!launched);
      for(int i = 0; i < operations; i++) {
        try {
          SampleProposal scp =
            new SampleProposal(proposerNodeId);
          scp.setCurrentUser();
          ce.submitProposal(scp, true);
        } catch (IOException e) {
          fail(String.valueOf(e));
        }
      }
    }

    public static void launch() {
      launched = true;
    }
  }
}
