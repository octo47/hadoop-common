package org.apache.hadoop.coordination.zk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Futures;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.coordination.zk.protobuf.ZkCoordinationProtocol;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;

/**
 * @author Andrey Stepachev
 */
public class ZkAgreementsStorage {

  private String zkBucketStatePath;

  public interface AgreementCallback {
    public void apply(long bucket, int seq, byte[] data) throws IOException;
  }

  public static final Log LOG = LogFactory.getLog(ZkAgreementsStorage.class);

  public static final byte[] EMPTY_BYTES = new byte[0];

  private final ZkConnection zooKeeper;
  private final String zkAgreementsPath;
  private final String zkAgreementsZNodeNamePrefix;

  private int zkBucketDigits;
  private int zkBucketAgreements;

  private AtomicLong currentBucket = new AtomicLong(-1);
  private Lock resolverLock = new ReentrantLock(false);

  private ArrayList<ACL> defaultAcl;

  public ZkAgreementsStorage(ZkConnection zooKeeper,
                             String zkAgreementsPath,
                             int zkBucketDigits) {
    this.zooKeeper = zooKeeper;
    this.zkAgreementsPath = zkAgreementsPath;
    this.zkBucketDigits = zkBucketDigits;
    this.zkBucketAgreements = ipow(10, this.zkBucketDigits);
    this.zkAgreementsZNodeNamePrefix = ZKConfigKeys.CE_ZK_AGREEMENTS_ZNODE_PREFIX_PATH;
    this.zkBucketStatePath = zkAgreementsPath + ZKConfigKeys.ZK_BUCKETS_STATE_PATH;
    this.defaultAcl = ZooDefs.Ids.OPEN_ACL_UNSAFE;
  }

  public void start() throws InterruptedException, IOException, KeeperException {
    nextBucket(0);
    final ZkCoordinationProtocol.ZkBucketsState state = getOrCreateState(0);
    LOG.info("Initialized agreements storage at " + zkAgreementsPath + " state:" + state);
  }

  public synchronized void stop() {
  }

  public long getCurrentBucket() {
    return currentBucket.get();
  }


  String getZkAgreementBucketPath(long bucket) {
    return zkAgreementsPath + "/" + String.format("%013d", bucket);
  }

  String getZkAgreementPathTemplate(long bucket) {
    return getZkAgreementBucketPath(bucket) + zkAgreementsZNodeNamePrefix;
  }

  String getExpectedAgreementZNodePath(long bucket, int cversion) {
    return getZkAgreementPathTemplate(bucket) +
            String.format(Locale.ENGLISH, "%010d", cversion);
  }

  String getZkBucketStatePath() {
    return zkBucketStatePath;
  }

  public String writeProposal(final byte[] serialisedProposal)
          throws IOException, KeeperException, TimeoutException, InterruptedException {
    do {
      long currentBucket = this.currentBucket.get();
      final ZNode bucketPathZNode = zooKeeper
              .exists(getZkAgreementBucketPath(currentBucket));
      if (!bucketPathZNode.isExists() || bucketPathZNode.getStat().getCversion() >= zkBucketAgreements)
        nextBucket(currentBucket);
      else {
        final String agreementPath = zooKeeper.create(getZkAgreementPathTemplate(currentBucket), serialisedProposal,
                defaultAcl, CreateMode.PERSISTENT_SEQUENTIAL);
        if (isInBucket(agreementPath))
          return agreementPath;
        else
          nextBucket(currentBucket);
      }
    } while (true);
  }


  private long nextBucket(long startingBucket) throws InterruptedException, IOException, KeeperException {
    long bucket = currentBucket.get();
    if (bucket > startingBucket)
      return bucket;
    resolverLock.lock();
    try {
      // double check if other thread already resolved next bucket
      bucket = currentBucket.get();
      if (bucket > startingBucket)
        return bucket;
      ZNode bucketZNode;
      // advance to the next bucket
      bucket++;
      do {
        final ZkCoordinationProtocol.ZkBucketsState bucketState =
                getOrCreateState(bucket);

        if (bucket < bucketState.getMaxBucket()) {
          bucket = bucketState.getMaxBucket();
        }
        createBucket(bucket); // just for sure

        final String bucketPath = getZkAgreementBucketPath(bucket);
        bucketZNode = zooKeeper.exists(bucketPath);
        if (bucketZNode.getStat().getCversion() >= zkBucketAgreements)
          bucket++;
        else
          break;
      } while (true);
      saveState(bucket);
      currentBucket.set(bucket);
      LOG.info("Using bucket " + currentBucket.get());
      return bucket;
    } finally {
      resolverLock.unlock();
    }
  }

  private void createBucket(long bucket) throws IOException, KeeperException, InterruptedException {
    zooKeeper.create(getZkAgreementBucketPath(bucket),
            EMPTY_BYTES,
            defaultAcl, CreateMode.PERSISTENT, true);
  }

  private void saveState(long bucket) throws InterruptedException, IOException, KeeperException {
    LOG.info("Saving bucket: " + bucket);
    while (true) {
      final ZNode bucketData = zooKeeper.getData(zkBucketStatePath);
      if (bucketData.isExists()) {
        final ZkCoordinationProtocol.ZkBucketsState savedState =
                ZkCoordinationProtocol.ZkBucketsState.parseFrom(bucketData.getData());
        if (savedState.getMaxBucket() < bucket) {
          try {
            final ZkCoordinationProtocol.ZkBucketsState newState =
                    savedState.toBuilder().setMaxBucket(bucket).build();
            zooKeeper.setData(zkBucketStatePath,
                    newState.toByteArray(),
                    bucketData.getStat().getVersion());
            LOG.info("Saved bucket state: " + newState);
            return;
          } catch (KeeperException ke) {
            if (ke.code() != KeeperException.Code.BADVERSION)
              throw ke;
            LOG.info("Conflict, retry for bucket state: " + savedState);
          }
        } else {
          return;
        }
      } else {
        getOrCreateState(bucket);
      }
    }
  }

  private ZkCoordinationProtocol.ZkBucketsState getOrCreateState(long defaultBucket)
          throws InterruptedException, IOException, KeeperException {
    final ZNode bucketData = zooKeeper.getData(zkBucketStatePath);
    final ZkCoordinationProtocol.ZkBucketsState state;
    if (!bucketData.isExists()) {
      state = ZkCoordinationProtocol.ZkBucketsState.newBuilder()
              .setBucketDigits(zkBucketDigits)
              .setMaxBucket(defaultBucket)
              .build();
      LOG.info("Buckets not initialized yet, initializing at: " + zkBucketStatePath);
      zooKeeper.create(zkBucketStatePath,
              state.toByteArray(),
              defaultAcl, CreateMode.PERSISTENT, true);
    }
    final ZNode stored = zooKeeper.getData(zkBucketStatePath);
    return ZkCoordinationProtocol.ZkBucketsState.parseFrom(stored.getData());
  }

  public void iterateAgreements(long lastBucket, int lastSeq, int batchSize, AgreementCallback cb)
          throws InterruptedException, IOException, KeeperException {
    long bucket = lastBucket;
    // iterate from next seq
    int start = lastSeq + 1;
    if (start < 0) {
      throw new IllegalArgumentException("Wrong lastSeq passed: " + lastSeq);
    }
    // skip to next bucket if sequence exceeds maximum of agreements in current bucket
    if (start >= zkBucketAgreements) { // seq starts from 0
      start = 0;
      bucket += 1;
    }
    ZNode bucketZNode = zooKeeper.getData(getZkAgreementBucketPath(bucket));
    // now we have knowledge of how many agreements are in bucket, that can be
    // obtained from znode CVersion.
    // end is ensured to get minimum of available agreements or specified batch size
    int end = Math.min(bucketZNode.isExists() ? bucketZNode.getStat().getCversion() : 0, start + batchSize);
    // if end is out of bucket, fix it on the end of bucket.
    // agreements out of bucket were resubmitted to next bucket,
    // so we don't need to read them right now.
    if (end > zkBucketAgreements)
      end = zkBucketAgreements;
    if (end - start == 0)
      return;

    if (LOG.isDebugEnabled()) {
      LOG.debug("Iterating agreements [" + start + "; " + end
              + ") in bucket " + bucket);
    }
    final List<Future<ZNode>> futures = new ArrayList<Future<ZNode>>();
    for (int seq = start; seq < end; seq++) {
      futures.add(zooKeeper.getDataAsync(getExpectedAgreementZNodePath(bucket, seq), false));
    }
    int seq = start;
    for (Future<ZNode> future : futures) {
      final ZNode proposal = Futures.get(future,
              zooKeeper.getSessionTimeout(), TimeUnit.MILLISECONDS, IOException.class);
      if (!proposal.isExists()) {
        throw new IOException("No agreement found at expected path " + proposal.getPath()
                + " in " + zkAgreementsPath);
      }
      cb.apply(bucket, seq++, proposal.getData());
    }
  }

  /**
   * Watch for next agreement for current bucket/seq.
   * Watcher should be registered with given ZkConnection.
   * TODO: make watching internal to ZkAgreementsStorage, doesn't depend preregistration.
   *
   * @return true if agreement already here
   */
  public boolean watchNextAgreement(long bucket, int seq) throws IOException {
    long expectedBucket = bucket;
    int expectedSeq = seq + 1;
    if (expectedSeq >= zkBucketAgreements) {
      expectedBucket++;
      expectedSeq = 0;
    }
    String nextProposal = getExpectedAgreementZNodePath(expectedBucket, expectedSeq);
    ZNode stat;
    try {
      stat = zooKeeper.exists(nextProposal, true);
    } catch (Exception e) {
      throw new IOException("Cannot obtain stat for: " + nextProposal, e);
    }
    if (stat.isExists()) {
      LOG.debug("Next agreement exists already: " + nextProposal);
      return true;
    }
    return false;
  }


  /**
   * Ensure znodeName in bucket range
   */
  @VisibleForTesting
  boolean isInBucket(String znodeName) {
    final int len = znodeName.length();
    if (len < 10)
      throw new IllegalArgumentException(
              "Path should not be less then 10 chars: " + znodeName);
    final String bucket = znodeName.substring(len - 10, len - zkBucketDigits);
    for (int i = bucket.length() - 1; i >= 0; i--) {
      if (bucket.charAt(i) != '0')
        return false;
    }
    return true;
  }

  private static int ipow(int base, int exp) {
    int result = 1;
    while (exp != 0) {
      if ((exp & 1) == 1)
        result *= base;
      exp >>= 1;
      base *= base;
    }
    return result;
  }

  public boolean canRecoverAgreements(long bucket) {
    try {
      return zooKeeper.exists(getZkAgreementBucketPath(bucket)) != null;
    } catch (Exception e) {
      LOG.error("Can't recover agreemetns");
      return false;
    }
  }
}
