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
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.coordination.Agreement;
import org.apache.hadoop.coordination.CoordinationEngine;
import org.apache.hadoop.coordination.NoQuorumException;
import org.apache.hadoop.coordination.Proposal;
import org.apache.hadoop.coordination.ProposalSubmissionException;
import org.apache.hadoop.coordination.zk.protobuf.ZkCoordinationProtocol;
import org.apache.hadoop.service.AbstractService;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;

/**
 * ZooKeeper-based implementation of {@link CoordinationEngine} that delivers
 * agreements to the learner of type {@code L} via the {@link AgreementHandler}.
 *
 * Implementation uses zookeeper sequential nodes. For each agreement
 * new znode allocated. To prevent unconditional growth of children znodes
 * grouped into buckets. Each agreement has to be written to
 * /ce/agreemetns/<bucketid>/a-<agreementseq>. Result of the write operation
 * is checked for assigned sequential number. If it exceeds maximum number of
 * agreements per bucket, agreement resubmitted to next bucket.
 *
 * Background thread cleans buckets, older then given threshold (maximum
 * number of buckets).
 *
 * Learning done in batches, CE knows in advance (looking at cversion of znode)
 * how much agreements in given bucket and request several read requests at once.
 * Upon completion of agreement processing CE stores final GSN (Global Sequence
 * Number) alongside bucket and sequence seen. This information is used on
 * recovery.
 */
@InterfaceAudience.Private
public class ZKCoordinationEngine extends AbstractService
        implements CoordinationEngine, Watcher {

  private static final Log LOG = LogFactory.getLog(ZKCoordinationEngine.class);

  public static final byte[] EMPTY_BYTES = new byte[0];

  public static final int ZK_POLL_INTERVAL_MS = 100;

  /**
   * ZK generates sequential nodes using cversion of ZNode.
   * That gives as a clue of how many changes are pending
   */
  public static final int INVALID_SEQ = -1;

  /**
   * True if this instance of Coordination Engine is executing agreements.
   */
  private volatile boolean isLearning;

  /** CE node identification */
  private String localNodeId;
  /** root zpath where CE stores state */
  private String zkRootPath;
  /** znode where buckets are created */
  private String zkAgreementsPath;
  /** znode where gsn states are created */
  private String zkGsnPath;
  /** znode where current CE instance stores ZkGsnState structure */
  private String zkGsnZNode;

  private int zookeeperSessionTimeout;
  private String zkConnectString;

  /** Configurable parameter for number of agreements per bucket calculation */
  private int zkBucketDigits;
  /** Agreements processing batch size */
  private int zkBatchSize;
  /** Cleanup thread will try to remove extra buckets over this limit */
  private int zkMaxBuckets;
  /**
   * If true, tells CE to commit GSN after processing of all agreements in batch,
   * significally reduces write load of zk ensemble
   */
  private boolean zkBatchCommit;

  private final Semaphore learnerCanProceed = new Semaphore(0);
  private Thread learnerThread;
  private ZkAgreementsStorage agreementsStorage;

  private ZkConnection zooKeeper;

  private final List<ZKAgreementHandler<?, ? extends Agreement<?, ?>>>
          handlers = Lists.newArrayList();

  private volatile ZNode gsnNodeStat;
  private volatile ZkCoordinationProtocol.ZkGsnState currentGSN =
          ZkCoordinationProtocol.ZkGsnState.newBuilder()
                  .setGsn(INVALID_GSN)
                  .setSeq(INVALID_SEQ)
                  .setBucket(0)
                  .build();

  public ZKCoordinationEngine(String name) {
    this(name, null);
  }

  public ZKCoordinationEngine(String name, String localNodeId) {
    super(name);
    this.localNodeId = localNodeId;
  }

  @Override // AbstractService
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    this.isLearning = false;
    this.zookeeperSessionTimeout = conf.getInt(ZKConfigKeys.CE_ZK_SESSION_TIMEOUT_KEY,
            ZKConfigKeys.CE_ZK_SESSION_TIMEOUT_DEFAULT);

    if (localNodeId == null)
      this.localNodeId = conf.get(ZKConfigKeys.CE_ZK_NODE_ID_KEY, null);
    if (this.localNodeId == null) {
      throw new HadoopIllegalArgumentException("Please define a value for: "
              + ZKConfigKeys.CE_ZK_NODE_ID_KEY);
    }
    this.zkConnectString = conf.get(ZKConfigKeys.CE_ZK_QUORUM_KEY,
            ZKConfigKeys.CE_ZK_QUORUM_DEFAULT);
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
    this.zkMaxBuckets = conf.getInt(ZKConfigKeys.CE_ZK_MAX_BUCKETS_KEY,
            ZKConfigKeys.CE_ZK_MAX_BUCKETS_DEFAULT);

    LOG.info("CE parameters: batch=" + zkBatchSize);
  }

  @Override // AbstractService
  protected void serviceStart() throws Exception {
    try {
      zooKeeper = new ZkConnection(
              zkConnectString,
              getZooKeeperSessionTimeout());

      initZkPaths();

      zooKeeper.addWatcher(this);
      LOG.info("Сurrent GSN to: " + currentGSN + ", zk session 0x" +
              Long.toHexString(zooKeeper.getSessionId()));
      this.agreementsStorage = new ZkAgreementsStorage(zooKeeper, zkAgreementsPath,
              zkBucketDigits, zkMaxBuckets);
      this.agreementsStorage.start();
    } catch (Exception e) {
      serviceStop();
      throw new IOException("Cannot start ZKCoordinationEngine", e);
    }

    LOG.info("Started ZKCoordinationEngine.");
    learnerCanProceed.release();
  }

  @Override // AbstractService
  protected void serviceStop() throws Exception {
    if (agreementsStorage != null) {
      agreementsStorage.stop();
      agreementsStorage = null;
    }
    pauseLearning();
    stopZk();
    isLearning = false;
    super.serviceStop();
    LOG.info("Stopped ZKCoordinationEngine.");
  }

  private void initZkPaths() throws IOException, InterruptedException {
    try {
      if (!zooKeeper.exists(zkRootPath).isExists()) {
        zooKeeper.create(zkRootPath, EMPTY_BYTES,
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, true);
      }
      if (!zooKeeper.exists(zkAgreementsPath).isExists()) {
        zooKeeper.create(zkAgreementsPath, EMPTY_BYTES,
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, true);
      }
      if (!zooKeeper.exists(zkGsnPath).isExists()) {
        zooKeeper.create(zkGsnPath, EMPTY_BYTES,
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, true);
      }
      // holding ephemeral lock for given nodeId
      zooKeeper.create(zkGsnZNode + ".alive", localNodeId.getBytes(),
              ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
      createOrGetGlobalSequenceNumber();
    } catch (KeeperException e) {
      throw new IOException("Can't init zk storage", e);
    }
  }

  public int getZooKeeperSessionTimeout() {
    return zookeeperSessionTimeout;
  }

  public String getZkAgreementsPath() {
    return zkAgreementsPath;
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
  public void initialize(Configuration config) {
    this.init(config);
  }

  /**
   * Register handlers for this instance of CE.
   */
  public <L, A extends Agreement<L, R>, R> void addHandler(ZKAgreementHandler<L, A> handler) {
    synchronized (handlers) {
      handlers.add(handler);
    }
  }

  @Override
  public List<Serializable> getMembershipNodeIds() {
    // TODO: implement me
    throw new UnsupportedOperationException("Not yet");
  }

  @Override
  public long lastSeenGlobalSequenceNumber() {
    return currentGSN.getGsn();
  }

  @Override
  public boolean canRecoverAgreements() {
    return agreementsStorage.canRecoverAgreements(
            currentGSN.getBucket()
    );
  }

  @Override
  public boolean canPropose() {
    return zooKeeper.isAlive();
  }


  @Override // CoordinationEngine
  public Serializable getLocalNodeId() {
    return localNodeId;
  }

  @Override // CoordinationEngine
  public void submitProposal(Proposal proposal,
                            boolean checkQuorum)
          throws ProposalSubmissionException {
    // Check for quorum.
    if (checkQuorum && !hasQuorum()) {
      throw new NoQuorumException("The zookeeper engine does not have quorum");
    }

    try {
      byte[] serializedProposal = serialize(proposal);
      agreementsStorage.writeProposal(serializedProposal);
    } catch (Exception e) {
      throw new ProposalSubmissionException("Cannot accept proposal", e);
    }
  }

  public byte[] serialize(Object obj) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(bytes);
    oos.writeObject(obj);
    oos.close();
    return bytes.toByteArray();
  }

  public Object deserialize(byte[] data)
      throws IOException, ClassNotFoundException {
    ByteArrayInputStream bytes = new ByteArrayInputStream(data);
    ObjectInputStream ois = new ObjectInputStream(bytes);
    Object obj = ois.readObject();
    ois.close();
    return obj;
  }

  public long getGlobalSequenceNumber() {
    return currentGSN.getGsn();
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
  public void resumeLearning(long fromGSN) {
    if (fromGSN != INVALID_GSN)
      throw new IllegalArgumentException("Don't know how to start from given GSN, yet");

    if (isLearning) {
      return;
    }

    AgreementsRunnable runnable = new AgreementsRunnable();
    learnerThread = new Thread(runnable);
    learnerThread.setDaemon(true);
    learnerThread.setName(getName() + "-learner");
    learnerThread.start();
    isLearning = true;
  }

  public boolean isLearning() {
    return isLearning;
  }

  @Override
  public void checkQuorum() throws NoQuorumException {
    try {
      zooKeeper.sync(zkAgreementsPath);
    } catch (Exception e) {
      throw new NoQuorumException("Unable to sync() zk connection:", e);
    }
  }

  public boolean hasQuorum() {
    boolean hasQuorum = false;
    if (zooKeeper != null && zooKeeper.isAlive()) {
      hasQuorum = true;
    }
    return hasQuorum;
  }

  @Override // Watcher
  public void process(WatchedEvent watchedEvent) {
    try {
      processImpl(watchedEvent);
    } catch (Exception e) {
      LOG.error("Failed to process event", e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Method for upserting current instance state.
   */
  private synchronized void createOrGetGlobalSequenceNumber()
          throws IOException, InterruptedException {
    final ZNode data;
    try {
      data = zooKeeper.getData(zkGsnZNode);
      if (!data.isExists()) {
        LOG.info("GSN state not found for " + localNodeId + ", creating new");
        zooKeeper.create(zkGsnZNode, currentGSN.toByteArray(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        gsnNodeStat = zooKeeper.exists(zkGsnZNode);
      } else {
        gsnNodeStat = data;
        currentGSN = ZkCoordinationProtocol.ZkGsnState.parseFrom(data.getData());
        LOG.info("GSN loaded for " + localNodeId + ": " + currentGSN.toString());
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

  /**
   * Class is responsible for applying agreements. It waits for pings from other
   * parts of the CE and check ZK storage for available agreements, need to be
   * applied.
   */
  class AgreementsRunnable implements Runnable {

    @Override // Runnable
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

    // TODO: here should not be synchronized
    @SuppressWarnings("unchecked")
    synchronized void executeAgreements() throws IOException, InterruptedException {
      do {
        if (!isLearning)
          return;
        try {
          agreementsStorage.iterateAgreements(
                  currentGSN.getBucket(),
                  currentGSN.getSeq(),
                  zkBatchSize,
                  new ZkAgreementsStorage.AgreementCallback() {
                    @Override
                    public void apply(long bucket, int seq, byte[] data) throws IOException, InterruptedException {
                      try {
                        Object obj = deserialize(data);
                        if (!(obj instanceof Agreement))
                          throw new IOException("Expecting " + Agreement.class.getName()
                                  + " instead got " + obj.getClass());
                        Agreement<?, ?> agreed = (Agreement<?, ?>) obj;
                        applyAgreed(bucket, seq, agreed);
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

          if (!agreementsStorage.watchNextAgreement(currentGSN.getBucket(), currentGSN.getSeq()))
            break;
        } catch (KeeperException e) {
          throw new IOException("Agreements path missed");
        }
        if (LOG.isTraceEnabled())
          LOG.trace("Agreement iteration processing done, GSN is " + currentGSN.toString());

      } while (isLearning);
    }

    @SuppressWarnings("unchecked")
    private synchronized void applyAgreed(long bucket, int seq, Agreement<?, ?> agreed)
    throws IOException, KeeperException, InterruptedException {
      try {
        currentGSN = ZkCoordinationProtocol.ZkGsnState.newBuilder()
            .setGsn(currentGSN.getGsn() + 1)
            .setBucket(bucket)
            .setSeq(seq)
            .build();
        if (LOG.isTraceEnabled())
          LOG.trace("Applying agreement, set GSN to " + currentGSN.toString());
        synchronized (handlers) {
          for (ZKAgreementHandler handler : handlers) {
            if (handler.handles(agreed))
              handler.executeAgreement(agreed);
          }
        }
      } catch (Exception e) {
        throw new IOException(e);
      }
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
    if (p.endsWith("/")) {
      return p.substring(0, p.length() - 1);
    } else {
      return path;
    }
  }
}
