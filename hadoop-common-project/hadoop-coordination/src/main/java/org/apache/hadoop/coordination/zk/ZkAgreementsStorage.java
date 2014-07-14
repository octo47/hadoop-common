package org.apache.hadoop.coordination.zk;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.coordination.zk.protobuf.ZkCoordinationProtocol;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

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

  private final ExecutorService executor;

  private int zkBucketDigits;
  private int zkBucketAgreements;
  private AtomicLong currentBucket = new AtomicLong(-1);
  private volatile SettableFuture<Long> resolvedBucket = null;
  private ArrayList<ACL> defaultAcl;

  public ZkAgreementsStorage(ZkConnection zooKeeper,
                             String zkAgreementsPath,
                             int zkBucketDigits,
                             ExecutorService executor) {
    this.zooKeeper = zooKeeper;
    this.zkAgreementsPath = zkAgreementsPath;
    this.executor = executor;
    this.zkBucketDigits = zkBucketDigits;
    this.zkBucketAgreements = ipow(10, this.zkBucketDigits);
    this.zkAgreementsZNodeNamePrefix = ZKConfigKeys.CE_ZK_AGREEMENTS_ZNODE_PREFIX_PATH;
    this.zkBucketStatePath = zkAgreementsPath + ZKConfigKeys.ZK_BUCKETS_STATE_PATH;
    this.defaultAcl = ZooDefs.Ids.OPEN_ACL_UNSAFE;
  }

  public void start() throws InterruptedException, IOException, KeeperException {
    findSuitableBucket();
  }

  public synchronized void stop() {
    // TODO: gracefully stop resolvers
    if (resolvedBucket != null) {
      resolvedBucket.cancel(true);
      resolvedBucket = null;
    }
  }

  public long currentBucket() {
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

  public ListenableFuture<String> writeProposal(final byte[] serialisedProposal) {
    final SettableFuture<String> resultFuture =
            SettableFuture.create();
    return writeProposalInner(resultFuture, serialisedProposal);
  }

  private ListenableFuture<String> writeProposalInner(final SettableFuture<String> resultFuture,
                                                      final byte[] serialisedProposal) {
    final ListenableFuture<String> async = zooKeeper.createAsync(
            getZkAgreementPathTemplate(currentBucket.get()), serialisedProposal,
            defaultAcl, CreateMode.PERSISTENT_SEQUENTIAL);
    final FutureCallback<String> callback = new FutureCallback<String>() {
      @Override
      public void onSuccess(@Nonnull String path) {
        try {
          if (isInBucket(path)) {
            resultFuture.set(path);
            if (LOG.isTraceEnabled())
              LOG.trace("Proposal stored in " + path);
          } else {
            if (LOG.isDebugEnabled())
              LOG.debug("Proposal is out of bucket " + path);
            waitForSuitableBucket(new Runnable() {
              @Override
              public void run() {
                writeProposalInner(resultFuture, serialisedProposal);
              }
            });
          }
        } catch (Exception e) {
          resultFuture.setException(e); // just to be sure
        }
      }

      @Override
      public void onFailure(Throwable t) {
        resultFuture.setException(t);
      }
    };
    Futures.addCallback(async, callback, MoreExecutors.sameThreadExecutor());
    return resultFuture;
  }

  private synchronized void waitForSuitableBucket(Runnable runnable) {
    if (resolvedBucket == null) {
      resolvedBucket = SettableFuture.create();
      executor.submit(new Runnable() {
        @Override
        public void run() {
          try {
            findSuitableBucket();
            resolvedBucket.set(currentBucket.get());
          } catch (Exception e) {
            resolvedBucket.setException(e);
          }
          resolvedBucket = null;
        }
      });
    }
    resolvedBucket.addListener(runnable, executor);
  }

  private void findSuitableBucket() throws InterruptedException, IOException, KeeperException {
    Stat bucketZNode;
    long bucket = currentBucket.get() + 1;
    do {
      final ZkCoordinationProtocol.ZkBucketsState bucketState =
              getOrCreateState(bucket);

      if (bucket < bucketState.getMaxBucket()) {
        bucket = bucketState.getMaxBucket();
      }
      createBucket(bucket); // just for sure

      final String bucketPath = getZkAgreementBucketPath(bucket);
      bucketZNode = zooKeeper.exists(bucketPath);
      if (bucketZNode == null) {
        throw new IllegalStateException("Bucket disappeared: " + bucketPath);
      }
      if (bucketZNode.getCversion() >= zkBucketAgreements)
        bucket++;
      else
        break;
    } while (true);
    saveState(bucket);
    currentBucket.set(bucket);
    LOG.info("Using bucket " + currentBucket.get());
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

  private ZkCoordinationProtocol.ZkBucketsState initBucket(long bucket)
          throws IOException, KeeperException, InterruptedException {

    final byte[] stateBytes = ZkCoordinationProtocol.ZkBucketsState.newBuilder()
            .setBucketDigits(zkBucketDigits)
            .setMaxBucket(bucket)
            .build().toByteArray();
    ZNode bucketData = zooKeeper.getData(zkBucketStatePath);
    if (!bucketData.isExists()) {
      LOG.info("Buckets not initialized yet, initializing at: " + zkBucketStatePath);
      zooKeeper.create(zkBucketStatePath,
              stateBytes,
              defaultAcl, CreateMode.PERSISTENT, true);
    } else {
      try {
        zooKeeper.setData(zkBucketStatePath, stateBytes, bucketData.getStat().getVersion());
      } catch (KeeperException ke) {
        if (ke.code() != KeeperException.Code.BADVERSION) {
          throw ke;
        }
        // ok, we have update by someone else
      }
    }
    final ZkCoordinationProtocol.ZkBucketsState bucketState =
            ZkCoordinationProtocol.ZkBucketsState.parseFrom(bucketData.getData());
    if (bucketState.getBucketDigits() != zkBucketDigits) {
      throw new IllegalStateException("Inconsistent number of digits: stored "
              + bucketState.getBucketDigits() + " vs " + zkBucketDigits);
    }


    if (bucketState.getMaxBucket() >= bucket) {
      final String bucketPath = getZkAgreementBucketPath(bucket);
      LOG.info("Creating bucket at: " + bucketPath);
      zooKeeper.create(bucketPath,
              EMPTY_BYTES, defaultAcl, CreateMode.PERSISTENT, true);
      zooKeeper.setData(zkBucketStatePath, stateBytes, bucketData.getStat().getVersion());
    }
    return bucketState;
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
    Stat stat;
    try {
      stat = zooKeeper.exists(nextProposal, true);
    } catch (Exception e) {
      throw new IOException("Cannot obtain stat for: " + nextProposal, e);
    }
    if (stat != null) {
      LOG.debug("Next agreement already exists: " + nextProposal);
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
      exp--;
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
