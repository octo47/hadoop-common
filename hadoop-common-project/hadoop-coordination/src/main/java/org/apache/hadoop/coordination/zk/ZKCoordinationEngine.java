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
import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.coordination.Agreement;
import org.apache.hadoop.coordination.AgreementHandler;
import org.apache.hadoop.coordination.CoordinationEngine;
import org.apache.hadoop.coordination.NoQuorumException;
import org.apache.hadoop.coordination.Proposal;
import org.apache.hadoop.coordination.ProposalNotAcceptedException;
import org.apache.hadoop.coordination.QuorumInitializationException;
import org.apache.hadoop.coordination.zk.protobuf.ZkCoordinationProtocol;
import org.apache.hadoop.service.AbstractService;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;

/**
 * ZooKeeper-based implementation of {@link CoordinationEngine}.
 */
public class ZKCoordinationEngine extends AbstractService
        implements CoordinationEngine, Watcher {
  private static final Log LOG = LogFactory.getLog(ZKCoordinationEngine.class);


  public static final byte[] EMPTY_BYTES = new byte[0];

  public static final int ZK_POLL_INTERVAL_MS = 100;

  /**
   * Used as invalid GSN (normal GSNs should start with 0
   * and increment monotonically with each new agreement.
   */
  public static final long INVALID_GSN = -1L;

  /**
   * ZK generates sequential nodes using cversion of ZNode.
   * That gives as a clue of how many changes are pending
   */
  public static final int INVALID_SEQ = -1;

  /**
   * True if this instance of Coordination Engine is executing agreements.
   */
  private volatile boolean isLearning;

  private String localNodeId;
  private String instanceId;
  private String zkRootPath;
  private String zkAgreementsPath;
  private String zkGsnPath;
  private String zkGsnZNode;
  private int zookeeperSessionTimeout;
  private AgreementHandler<?> handler;
  private String zkConnectString;
  private int zkBucketDigits;
  private int zkBatchSize;
  private boolean zkBatchCommit;

  private final Semaphore learnerCanProceed = new Semaphore(0);
  private Thread learnerThread;
  private ZkAgreementsStorage storage;

  private ZkConnection zooKeeper;

  private volatile ZNode gsnNodeStat;
  private volatile ZkCoordinationProtocol.ZkGsnState currentGSN =
          ZkCoordinationProtocol.ZkGsnState.newBuilder()
                  .setGsn(INVALID_GSN)
                  .setSeq(INVALID_SEQ)
                  .setBucket(0)
                  .build();


  public ZKCoordinationEngine(String name) {
    super(name);
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    this.handler = null;
    this.isLearning = false;
    this.zookeeperSessionTimeout = conf.getInt(ZKConfigKeys.CE_ZK_SESSION_TIMEOUT_KEY,
            ZKConfigKeys.CE_ZK_SESSION_TIMEOUT_DEFAULT);

    this.localNodeId = conf.get(ZKConfigKeys.CE_ZK_NODE_ID_KEY, null);
    if (this.localNodeId == null) {
      throw new HadoopIllegalArgumentException("Please define a value for: "
              + ZKConfigKeys.CE_ZK_NODE_ID_KEY);
    }
    this.zkConnectString = conf.get(ZKConfigKeys.CE_ZK_QUORUM_KEY,
            ZKConfigKeys.CE_ZK_QUORUM_DEFAULT);
    this.instanceId = ManagementFactory.getRuntimeMXBean().getName();
    this.zkBatchSize = conf.getInt(ZKConfigKeys.CE_ZK_BATCH_SIZE_KEY,
            ZKConfigKeys.CE_ZK_BATCH_SIZE_DEFAULT);
    this.zkBatchCommit = conf.getBoolean(ZKConfigKeys.CE_ZK_BATCH_COMMIT_KEY,
            ZKConfigKeys.CE_ZK_BATCH_COMMIT_DEFAULT);
    this.zkRootPath = ensureNoEndingSlash(conf.get(ZKConfigKeys.CE_ZK_QUORUM_PATH_KEY,
            ZKConfigKeys.CE_ZK_QUORUM_PATH_DEFAULT));
    this.zkAgreementsPath = ensureNoEndingSlash(zkRootPath +
            ZKConfigKeys.CE_ZK_AGREEMENTS_ZNODE_PATH);
    this.zkGsnPath = ensureNoEndingSlash(zkRootPath +
            ZKConfigKeys.CE_ZK_GSN_ZNODE_PATH);
    this.zkGsnZNode = zkGsnPath + "/" + localNodeId;

    this.zkBucketDigits = conf.getInt(ZKConfigKeys.CE_ZK_BUCKET_DIGITS_KEY,
            ZKConfigKeys.CE_ZK_BUCKET_DIGITS_DEFAULT);

    LOG.info("CE parameters: batch=" + zkBatchSize);
  }

  @Override
  public void registerHandler(AgreementHandler<?> handler) {
    this.handler = handler;
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
    try {
      zooKeeper = new ZkConnection(
              zkConnectString,
              getZooKeeperSessionTimeout());

      initStorage();

      zooKeeper.addWatcher(this);
      LOG.info("Сurrent GSN to: " + currentGSN + ", zk session 0x" +
              Long.toHexString(zooKeeper.getSessionId()));
      this.storage = new ZkAgreementsStorage(zooKeeper, zkAgreementsPath,
              zkBucketDigits);
      this.storage.start();
    } catch (Exception e) {
      serviceStop();
      throw new IOException("Cannot start ZKCoordinationEngine", e);
    }

    resumeLearning();
    LOG.info("Started ZKCoordinationEngine.");
    learnerCanProceed.release();
  }

  @Override
  protected void serviceStop() throws Exception {
    if (storage != null) {
      storage.stop();
      storage = null;
    }
    pauseLearning();
    stopZk();
    isLearning = false;
    super.serviceStop();
    LOG.info("Stopped ZKCoordinationEngine.");
  }


  private void initStorage() throws IOException, InterruptedException {
    try {
      if (!zooKeeper.exists(zkRootPath).isExists()) {
        zooKeeper.create(zkRootPath, EMPTY_BYTES,
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }
      if (!zooKeeper.exists(zkAgreementsPath).isExists()) {
        zooKeeper.create(zkAgreementsPath, EMPTY_BYTES,
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }
      if (!zooKeeper.exists(zkGsnPath).isExists()) {
        zooKeeper.create(zkGsnPath, EMPTY_BYTES,
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }
      // holding ephemeral lock for given nodeId
      zooKeeper.create(zkGsnPath + ".alive", instanceId.getBytes(),
              ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
      createOrGetGlobalSequenceNumber();
    } catch (KeeperException e) {
      throw new QuorumInitializationException("Can't init zk storage", e);
    }
  }


  public int getZooKeeperSessionTimeout() {
    return zookeeperSessionTimeout;
  }

  public String getZkRootPath() {
    return zkRootPath;
  }

  public String getZkAgreementsPath() {
    return zkAgreementsPath;
  }

  public String getZkGsnPath() {
    return zkGsnPath;
  }

  public String getZkGsnZNode() {
    return zkGsnZNode;
  }

  private synchronized void stopZk() {
    if (zooKeeper != null) {
      zooKeeper.close();
      zooKeeper = null;
    }
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
          throws ProposalNotAcceptedException, NoQuorumException {
    // Check for quorum.
    if (checkQuorum) {
      checkQuorum();
    }

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos;
    try {
      // Serialize proposal.
      oos = new ObjectOutputStream(baos);
      oos.writeObject(proposal);
      byte[] serializedProposal = baos.toByteArray();

      storage.writeProposal(serializedProposal);

      return ProposalReturnCode.OK;

    } catch (Exception e) {
      throw new ProposalNotAcceptedException("Cannot accept proposal", e);
    }
  }

  @Override
  public long getGlobalSequenceNumber() {
    return currentGSN.getGsn();
  }

  @Override
  public boolean canRecoverAgreements() {
    return storage.canRecoverAgreements(currentGSN.getBucket());
  }

  @Override
  public boolean canPropose() {
    return zooKeeper.isAlive();
  }

  @Override
  public void pauseLearning() {
    if (!isLearning) {
      return;
    }

    isLearning = false;
    if (learnerThread != null) {
      learnerThread.interrupt();
      try {
        learnerThread.join();
      } catch (InterruptedException e) {
      }
      learnerThread = null;
    }
  }

  @Override
  public void resumeLearning() {
    if (isLearning) {
      return;
    }
    isLearning = true;
    learnerThread = new Thread(createLearnerThread());
    learnerThread.setName(getName() + "-learner");
    learnerThread.start();
  }

  @Override
  public void checkQuorum() throws NoQuorumException {
    if (zooKeeper == null) {
      throw new IllegalStateException("Not initialized CE");
    }
    if (!zooKeeper.isAlive()) {
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

  private synchronized void createOrGetGlobalSequenceNumber()
          throws IOException, InterruptedException {

    final ZNode data;
    try {
      data = zooKeeper.getData(zkGsnZNode);
      if (!data.isExists()) {
        zooKeeper.create(zkGsnZNode, currentGSN.toByteArray(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        gsnNodeStat = zooKeeper.exists(zkGsnZNode);
      } else {
        gsnNodeStat = data;
        currentGSN = ZkCoordinationProtocol.ZkGsnState.parseFrom(data.getData());
      }
    } catch (KeeperException e) {
      throw new IOException("Failed", e);
    }
  }

  private void processImpl(WatchedEvent event) throws Exception {
    if (!isLearning) {
      return;
    }
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
          break;
        case Expired:
          // It's all over
          LOG.info("CoordinationEngine should shutdown.");
          noteFailure(new NoQuorumException("ZK session was lost"));
          stop();
          break;
        case Disconnected:
          // client got disconnected from ZooKeeper ensemble and will try to
          // reconnect automatically; until that, Coordination Engine may
          // neither submit new proposals, nor learn agreements
          LOG.warn("Coordination Engine got disconnected from ZooKeeper,"
                  + " agreements processing is paused");
          return;
        default:
          LOG.error("Unexpected event state: " + event);
          break;
      }
    }
    learnerCanProceed.release();
  }


  private Runnable createLearnerThread() {
    return new AgreementsRunnable();
  }

  /**
   * Class is responsible for applying agreements.
   * It waits for pings from other parts of the CE
   * and check ZK storage for available agreements,
   * need to be applied.
   */
  class AgreementsRunnable implements Runnable {
    @Override
    public void run() {
      while (isLearning) {
        try {
          learnerCanProceed.tryAcquire(ZK_POLL_INTERVAL_MS, TimeUnit.MILLISECONDS);
          learnerCanProceed.drainPermits();
          executeAgreements();
        } catch (InterruptedException e) {
          if (LOG.isDebugEnabled())
            LOG.debug("AgreementExecutor interrupted", e);
          Thread.interrupted();
        } catch (Exception e) {
          LOG.error("AgreementExecutor got exception", e);
          ZKCoordinationEngine.this.stop();
          return;
        }
      }
    }

    // TODO: here should not be syncronized
    synchronized void executeAgreements() throws IOException, InterruptedException {
      do {
        if (!isLearning)
          return;
        try {
          storage.iterateAgreements(
                  currentGSN.getBucket(),
                  currentGSN.getSeq(),
                  zkBatchSize,
                  new ZkAgreementsStorage.AgreementCallback() {
                    @Override
                    public void apply(long bucket, int seq, byte[] data) throws IOException, InterruptedException {
                      try {
                        ObjectInputStream ois = new ObjectInputStream(
                                new ByteArrayInputStream(data));
                        Object obj = ois.readObject();
                        if (!(obj instanceof Agreement<?, ?>))
                          throw new IOException("Expecting Agreement type, but got " + obj);
                        Agreement<?, ?> agreement = (Agreement<?, ?>) obj;

                        applyAgreement(bucket, seq, agreement);
                        if (!zkBatchCommit)
                          updateCurrentGSN();
                      } catch (InterruptedException e) {
                        throw e;
                      } catch (Exception e) {
                        throw new IOException("Cannot obtain agreement data: ", e);
                      }
                    }
                  });
          if (zkBatchCommit)
            updateCurrentGSN();

          if (!storage.watchNextAgreement(currentGSN.getBucket(), currentGSN.getSeq()))
            break;
        } catch (KeeperException e) {
          throw new IOException("Agreements path missed");
        }
        if (LOG.isTraceEnabled())
          LOG.trace("Agreement iteration processing done, GSN is " + currentGSN.toString());

      } while (isLearning);
    }

    private synchronized void applyAgreement(long bucket, int seq, Agreement<?, ?> agreement)
            throws IOException, KeeperException, InterruptedException {
      currentGSN = ZkCoordinationProtocol.ZkGsnState.newBuilder()
              .setGsn(currentGSN.getGsn() + 1)
              .setBucket(bucket)
              .setSeq(seq)
              .build();
      if (LOG.isTraceEnabled())
        LOG.trace("Applying agreement, set GSN to " + currentGSN.toString());
      handler.executeAgreement(agreement);
    }

  }

  private void updateCurrentGSN()
          throws InterruptedException, IOException, KeeperException {
    if (LOG.isTraceEnabled())
      LOG.trace("Saving agreement, set GSN to " + currentGSN.toString());
    gsnNodeStat = zooKeeper.setData(zkGsnZNode,
            currentGSN.toByteArray(), gsnNodeStat.getStat().getVersion());
  }

  private static String ensureNoEndingSlash(String path) {
    String p = path.trim();
    if (p.endsWith("/"))
      return p.substring(0, p.length() - 1);
    else
      return path;
  }

}
