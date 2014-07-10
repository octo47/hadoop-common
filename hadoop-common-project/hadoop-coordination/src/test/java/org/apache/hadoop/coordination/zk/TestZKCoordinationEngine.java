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
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.coordination.zk.protobuf.ZkCoordinationProtocol;
import org.apache.hadoop.service.Service;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
  }

  /**
   * Submits single proposal and verifies that agreement was reached.
   */
  @Test(timeout = 2000)
  public void testSimpleProposals() throws IOException, KeeperException,
      InterruptedException {

    zkCluster = new MiniZooKeeperCluster();
    zkCluster.setDefaultClientPort(3000);
    zkCluster.startup(
        new File(System.getProperty("test.build.dir", "target/test-dir"),
            "testSimpleProposals"), 1);

    ZKCoordinationEngine cEngine = new ZKCoordinationEngine("ce");
    cEngine.init(conf);
    SampleLearner myLearner = new SampleLearner();
    cEngine.registerHandler(new SampleHandler(myLearner));
    cEngine.start();
    Assert.assertEquals(Service.STATE.STARTED, cEngine.getServiceState());

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

    String nodeGlobalSeqNumZNodePath = cEngine.getZkGsnZNode();

    awaitLearner(cEngine, zk, nodeGlobalSeqNumZNodePath);

    checkAgreemetsCountStoredInZk(cEngine, zk, 1);

    cEngine.stop();
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


    ZKCoordinationEngine cEngine = new ZKCoordinationEngine("ce");
    cEngine.init(conf);
    SampleLearner myLearner = new SampleLearner();
    cEngine.registerHandler(new SampleHandler(myLearner));
    cEngine.start();

    Thread.sleep(200);

    final int totalAgreements = 100000;
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
    String nodeGlobalSeqNumZNodePath = cEngine.getZkGsnZNode();

    awaitLearner(cEngine, zk, nodeGlobalSeqNumZNodePath);

    checkAgreemetsCountStoredInZk(cEngine, zk, totalAgreements);

    assertEquals("Bad learner state.", totalAgreements, myLearner.getState());

    cEngine.stop();
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

  private void checkAgreemetsCountStoredInZk(ZKCoordinationEngine cEngine, ZooKeeper zk, int expected)
          throws KeeperException, InterruptedException {
    List<String> agreementZnodes = zk.getChildren(cEngine.getZkAgreementsPath(), null);
    int numAgreementZnodes = 0;

    for (String s : agreementZnodes) {
      if (s.startsWith(ZKConfigKeys.CE_ZK_AGREEMENTS_ZNODE_PREFIX_PATH.substring(1))) {
        numAgreementZnodes++;
      }
    }
    assertEquals("Total number of agreements in ZooKeeper is wrong:",
            expected, numAgreementZnodes);
  }

  private void awaitLearner(ZKCoordinationEngine cEngine, ZooKeeper zk, String nodeGlobalSeqNumZNodePath) throws KeeperException, InterruptedException, com.google.protobuf.InvalidProtocolBufferException {
    ZkCoordinationProtocol.ZkGsnState state;
    long cEngineGlobalSeqNum;
    do {
      byte[] data = zk.getData(nodeGlobalSeqNumZNodePath, cEngine, null);
      state = ZkCoordinationProtocol.ZkGsnState.parseFrom(data);
      cEngineGlobalSeqNum = cEngine.getGlobalSequenceNumber();
    } while (state.getGsn() != cEngineGlobalSeqNum);
  }

}
