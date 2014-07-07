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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.coordination.Agreement;
import org.apache.hadoop.coordination.AgreementHandler;
import org.apache.hadoop.coordination.CoordinationEngine;
import org.apache.hadoop.coordination.NoQuorumException;
import org.apache.hadoop.coordination.Proposal;
import org.apache.hadoop.coordination.ProposalNotAcceptedException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.google.common.primitives.Longs;

/**
 * ZooKeeper-based implementation of {@link CoordinationEngine}.
 */
public class ZKCoordinationEngine
implements CoordinationEngine, Configurable, Watcher {
  public static final Log LOG = LogFactory.getLog(ZKCoordinationEngine.class);

  /**
   * Used as invalid GSN (normal GSNs should start with 0
   * and increment monotonically with each new agreement.
   */
  public static final long INVALID_GSN = -1L;

  protected Serializable localNodeId;
  protected ZooKeeper zooKeeper;
  protected Configuration conf;
  protected AgreementHandler<?> handler;

  /**
   * True if this instance of Coordination Engine is running.
   */
  protected volatile boolean isRunning;

  /**
   * True if this instance of Coordination Engine is executing agreements.
   */
  protected volatile boolean isLearning;

  protected static volatile long currentGSN = INVALID_GSN;

  public ZKCoordinationEngine(Configuration config) {
    initialize(config);
  }

  /**
   * Used to access random host in ZooKeeper quorum.
   */
  private static final ThreadLocal<Random> RANDOM = new ThreadLocal<Random>() {
    @Override
    protected Random initialValue() {
      return new Random();
    }
  };

  @Override
  public void initialize(Configuration config) {
    this.conf = config;
    this.handler = null;
    this.isRunning = false;
    this.isLearning = false;
    this.localNodeId = conf.get(ZKConfigKeys.CE_ZK_NODE_ID_KEY, null);
  }

  @Override
  public void registerHandler(AgreementHandler<?> handler) {
    this.handler = handler;
  }

  /**
   * Connects to ZooKeeper and initializes GSN.
   *
   * {@inheritDoc}
   */
  @Override
  public void start() throws IOException {
    if(isRunning) {
      return;
    }

    try {
      zooKeeper = new ZooKeeper(getRandomZooKeeperHost(),
        getZooKeeperSessionTimeout(), this);

      isLearning = true;
      if(localNodeId == null) {
        throw new HadoopIllegalArgumentException("Please define a value for: "
          + ZKConfigKeys.CE_ZK_NODE_ID_KEY);
      }

      currentGSN = getGlobalSequenceNumber();
      LOG.info("Set current GSN to: " + currentGSN);
    } catch (Exception e) {
      throw new IOException("Cannot start ZKCoordinationEngine", e);
    }
    isRunning = true;
    LOG.info("Started ZKCoordinationEngine.");
  }

  private String getRandomZooKeeperHost() {
    String hostValue = conf.get(ZKConfigKeys.CE_ZK_QUORUM_KEY,
      ZKConfigKeys.CE_ZK_QUORUM_DEFAULT);
    String hosts[] = hostValue.split(",");
    if(hosts.length == 1)
      return hosts[0];
    int randomIndex = RANDOM.get().nextInt(hosts.length);
    return hosts[randomIndex];
  }

  private int getZooKeeperSessionTimeout() {
    return conf.getInt(ZKConfigKeys.CE_ZK_SESSION_TIMEOUT_KEY,
      ZKConfigKeys.CE_ZK_SESSION_TIMEOUT_DEFAULT);
  }

  @Override
  public void stop() {
    if(!isRunning) {
      return;
    }

    try {
      if(zooKeeper != null) {
        zooKeeper.close();
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    isRunning = false;
    isLearning = false;
    LOG.info("Stopped ZKCoordinationEngine.");
  }

  @Override
  public List<Serializable> getMembershipNodeIds() {
    // TODO: Figure out nodes in membership from Configuration.
    return Collections.singletonList(getLocalNodeId());
  }

  @Override
  public Serializable getLocalNodeId() {
    return localNodeId;
  }

  @Override
  public ProposalReturnCode submitProposal(Proposal proposal,
                                           boolean checkQuorum)
      throws ProposalNotAcceptedException {
    // Check for quorum.
    if(checkQuorum) {
      try {
        checkQuorum();
      } catch (NoQuorumException e) {
        return ProposalReturnCode.NO_QUORUM;
      }
    }
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos;
    try {
      // Serialize proposal.
      oos = new ObjectOutputStream(baos);
      oos.writeObject(proposal);
      byte[] serializedProposal = baos.toByteArray();
      // Write proposal to znode

      zooKeeper.create(getAgreementZNodePath(), serializedProposal,
        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
    } catch (Exception e) {
      throw new ProposalNotAcceptedException("Cannot create ZooKeeper", e);
    }
    return ProposalReturnCode.OK;
  }

  @Override
  public long getGlobalSequenceNumber() {
    if(currentGSN == INVALID_GSN) {
      return getCurrentGSN(getLocalNodeId());
    }
    return currentGSN;
  }

  @Override
  public boolean canRecoverAgreements() {
    return true;
  }

  @Override
  public boolean canPropose() {
    return zooKeeper!= null && zooKeeper.getState().isConnected();
  }

  @Override
  public void pauseLearning() {
    if(!isLearning) {
      return;
    }

    isLearning = false;
  }

  @Override
  public void resumeLearning() {
    if(isLearning) {
      return;
    }

    isLearning = true;
  }

  @Override
  public void checkQuorum() throws NoQuorumException {
    if(zooKeeper == null || !zooKeeper.getState().isConnected()) {
      throw new NoQuorumException("No connection to ZooKeeper");
    }
  }

  @Override
  public void process(WatchedEvent watchedEvent) {
    LOG.info("Got watched event: " + watchedEvent);
    try {
      processImpl(watchedEvent);
    } catch (Exception e) {
      LOG.error("Failed to process event", e);
    }
  }

  private synchronized long getCurrentGSN(Serializable nodeId) {
    long globalSequenceNumber = 0L;
    createOrGetGlobalSequenceNumber(nodeId, globalSequenceNumber);
    return globalSequenceNumber;
  }

  private long createOrGetGlobalSequenceNumber(Serializable nodeId,
                                               long globalSequenceNumber) {
    String gsnZnode = getGlobalSequenceNumberZNodePath(nodeId);
    String engineZnode = ZKConfigKeys.CE_ZK_ENGINE_ZNODE_DEFAULT;

    while(true) {
      try {
        if(zooKeeper.exists(engineZnode, false) == null) {
          zooKeeper.create(engineZnode, new byte[0],
            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        if(zooKeeper.exists(gsnZnode, false) == null) {
          zooKeeper.create(gsnZnode, Longs.toByteArray(globalSequenceNumber),
            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
          return globalSequenceNumber;
        } else {
          return Longs.fromByteArray(zooKeeper.getData(gsnZnode, this, null));
        }
      } catch (Exception e) {
        if(LOG.isDebugEnabled())
          LOG.debug("Failed to update GSN.", e);
      }
    }
  }

  static String getGlobalSequenceNumberZNodePath(Serializable nodeId) {
    return ZKConfigKeys.CE_ZK_ENGINE_ZNODE_DEFAULT + "/" + nodeId +
      ZKConfigKeys.CE_ZK_GSN_ZNODE_SUFFIX_DEFAULT;
  }

  static String getAgreementZNodePath() {
    return ZKConfigKeys.CE_ZK_AGREEMENT_ZNODE_PATH_DEFAULT;
  }

  private synchronized String getExpectedAgreementZNodePath() {
    String agreementZNodePath = getAgreementZNodePath();
    int digits = String.valueOf(currentGSN + 1).length();
    int zeroes = 10 - digits;
    StringBuilder expectedPathBuilder = new StringBuilder();
    expectedPathBuilder.append(agreementZNodePath);
    for(int i = 0; i < zeroes; i++)
      expectedPathBuilder.append(0);
    expectedPathBuilder.append(currentGSN + 1);
    return expectedPathBuilder.toString();
  }

  private void updateCurrentGSN() {
    synchronized (zooKeeper) {
      long newGSN = currentGSN + 1;
      String gsnZnode = getGlobalSequenceNumberZNodePath(getLocalNodeId());
      try {
        if(zooKeeper.exists(gsnZnode, false) == null) {
          createOrGetGlobalSequenceNumber(getLocalNodeId(), newGSN);
        }
        zooKeeper.setData(gsnZnode, Longs.toByteArray(newGSN), -1);
      } catch (Exception e) {
        LOG.error("Failed to update current GSN to: " + newGSN);
        throw new RuntimeException(e);
      }
      currentGSN = newGSN;
    }
  }

  private void processImpl(WatchedEvent event) throws Exception {
    if(!isLearning) {
      return;
    }
    String path = event.getPath();

    if (event.getType() == Event.EventType.None) {
      // We are are being told that the state of the
      // connection has changed
      switch (event.getState()) {
        case SyncConnected:
          // In this particular example we don't need to do anything
          // here - watches are automatically re-registered with
          // server and any watches triggered while the client was
          // disconnected will be delivered (in order of course)
          LOG.info("Coordination Engine is connected to ZK, engine is running"
            + " and learning agreements");
          isRunning = true;
          break;
        case Expired:
          // It's all over
          LOG.info("CoordinationEngine should shutdown.");
          stop();
          break;
        case Disconnected:
          // client got disconnected from ZooKeeper ensemble and will try to
          // reconnect automatically; until that, Coordination Engine may
          // neither submit new proposals, nor learn agreements
          isRunning = false;
          LOG.warn("Coordination Engine got disconnected from ZooKeeper,"
            + " agreements processing is paused");
          return;
        default:
          LOG.error("Unexpected event state: " + event);
          break;
      }
    } else if(event.getType() == Event.EventType.NodeDataChanged) {
      if(path.startsWith(ZKConfigKeys.CE_ZK_ENGINE_ZNODE_DEFAULT)) {
        LOG.info("Detected GSN node updated: " + path);
      }
    } else if(event.getType() == Event.EventType.NodeCreated) {
      LOG.info("Detected node created: " + path);
      if(path.startsWith(ZKConfigKeys.CE_ZK_AGREEMENT_ZNODE_PATH_DEFAULT)) {
        executeAgreement(path);
      }
    }
    loopLearningUntilCaughtUp();
  }

  private void loopLearningUntilCaughtUp() throws IOException {
    Stat stat = null;
    do {
      if(!isLearning) return;
      String nextProposal = getExpectedAgreementZNodePath();
      try {
        stat = zooKeeper.exists(nextProposal, this);
      } catch(Exception e) {
        throw new IOException("Cannot obtain stat for: " + nextProposal, e);
      }
      if(stat == null) {
        LOG.info("Registered for: " + nextProposal);
        return;
      }
      executeAgreement(nextProposal);
    } while(isLearning);
  }

  void executeAgreement(String path) throws IOException {
    Object obj = null;
    try {
      byte[] data = zooKeeper.getData(path, this, null);
      ObjectInputStream ois = new ObjectInputStream(
                                new ByteArrayInputStream(data));
      obj = ois.readObject();
    } catch(Exception e) {
      throw new IOException("Cannot obtain agreement data: ", e);
    }
    LOG.info("Event object: " + obj);
    if(!(obj instanceof Agreement<?,?>))
      throw new IOException("Expecting Agreement type, but got " + obj);
    Agreement<?, ?> agreement = (Agreement<?, ?>) obj;

    updateCurrentGSN();
    handler.executeAgreement(agreement);
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
