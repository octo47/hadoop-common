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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.coordination.StaleReplicaException;
import org.apache.hadoop.coordination.zk.protobuf.ZkCoordinationProtocol;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;

import static com.google.common.base.Predicates.and;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;

/**
 * Class encapsulate all operations with agreements, stored in zk.
 * Agreements stored in buckets, extra buckets are removed by background
 * thread.
 * Bucket can be marked as deleted, that means, bucket is under deletion,
 * so some of agreement can be deleted already and this bucket should not
 * be used due of it's inconsistency.
 * BucketCleaner thread uses locks for preventing concurrent node deletions.
 */
@InterfaceAudience.Private
public class ZkAgreementsStorage {

  public static final int DELETE_BATCH_SIZE = 1024;
  private String zkBucketStatePath;

  public interface AgreementCallback {
    public void apply(long bucket, int seq, byte[] data) throws IOException, InterruptedException;
  }

  private static final Log LOG = LogFactory.getLog(ZkAgreementsStorage.class);

  public static final byte[] EMPTY_BYTES = new byte[0];

  private final ZkConnection zooKeeper;
  private final String zkAgreementsPath;
  private final String zkAgreementsZNodeNamePrefix;

  private int zkBucketDigits;
  private int zkBucketAgreements;
  private int zkMaxBuckets;
  private int cleanupInterval = 30; // seconds

  private AtomicLong currentBucket = new AtomicLong(-1);
  private Lock resolverLock = new ReentrantLock(false);

  private ArrayList<ACL> defaultAcl;

  private ScheduledExecutorService scheduler;

  public ZkAgreementsStorage(ZkConnection zooKeeper,
                             String zkAgreementsPath,
                             int zkBucketDigits,
                             int zkMaxBuckets) {
    this.zooKeeper = zooKeeper;
    this.zkAgreementsPath = zkAgreementsPath;
    this.zkBucketDigits = zkBucketDigits;
    this.zkBucketAgreements = ipow(10, this.zkBucketDigits);
    this.zkMaxBuckets = zkMaxBuckets;
    this.zkAgreementsZNodeNamePrefix = ZKConfigKeys.CE_ZK_AGREEMENTS_ZNODE_PREFIX_PATH;
    this.zkBucketStatePath = zkAgreementsPath + ZKConfigKeys.ZK_BUCKETS_STATE_PATH;
    this.defaultAcl = ZooDefs.Ids.OPEN_ACL_UNSAFE;
  }

  public void start() throws InterruptedException, IOException, KeeperException {
    scheduler = Executors
            .newScheduledThreadPool(1, new ThreadFactoryBuilder().setDaemon(true)
                    .setNameFormat("ZkAgreementsStorage-%d").build());
    final ZkCoordinationProtocol.ZkBucketsState state = getOrCreateState(0);
    nextBucket(state.getMaxBucket());
    scheduler.scheduleAtFixedRate(new BucketCleaner(this), cleanupInterval, cleanupInterval,
            TimeUnit.SECONDS);
    LOG.info("Initialized agreements storage at " + zkAgreementsPath + " state:" + state);
  }

  public synchronized void stop() {
    scheduler.shutdownNow();
  }

  public long getCurrentBucket() {
    return currentBucket.get();
  }

  private final static Pattern bucketNamePattern = Pattern.compile("\\d{13}");
  public static final Predicate<String> BUCKET_ZNODE = new Predicate<String>() {
    @Override
    public boolean apply(String znodeName) {
      return bucketNamePattern.matcher(znodeName).matches();
    }
  };
  private final static Pattern bucketLockNamePattern = Pattern.compile("\\d{13}\\.lock");
  public static final Predicate<String> BUCKET_LOCK_ZNODE = new Predicate<String>() {
    @Override
    public boolean apply(String znodeName) {
      return bucketLockNamePattern.matcher(znodeName).matches();
    }
  };

  String getZkAgreementBucketPath(long bucket) {
    return zkAgreementsPath + "/" + String.format("%013d", bucket);
  }

  String getZkAgreementBucketDeletedPath(long bucket) {
    return zkAgreementsPath + "/" + String.format("%013d", bucket) + ".deleted";
  }

  String getZkAgreementBucketLockPath(long bucket) {
    return zkAgreementsPath + "/" + String.format("%013d", bucket) + ".busy";
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
        // create bucket znode
        zooKeeper.create(getZkAgreementBucketPath(bucket),
                EMPTY_BYTES,
                defaultAcl, CreateMode.PERSISTENT, true);
        final ZkCoordinationProtocol.ZkBucketsState bucketState =
                getOrCreateState(bucket);

        if (bucket < bucketState.getMaxBucket()) {
          bucket = bucketState.getMaxBucket();
        }

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
    ZNode deletedBucketZNode = zooKeeper.exists(getZkAgreementBucketDeletedPath(bucket));
    if (!bucketZNode.isExists() || deletedBucketZNode.isExists()) {
      throw new StaleReplicaException("Bucket " + bucket + " marked as deleted or not exists");
    }
    // now we have knowledge of how many agreements are in bucket, that can be
    // obtained from znode CVersion.
    // _end_ is ensured to get minimum of available agreements or specified batch size
    int end = Math.min(bucketZNode.getStat().getCversion(), start + batchSize);
    // if _end_ is out of bucket, fix it on the end of bucket.
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
  public boolean watchNextAgreement(long bucket, int seq) throws IOException, InterruptedException {
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
    } catch (InterruptedException e) {
      throw e;
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

  private static class BucketCleaner implements Runnable {

    private final ZkAgreementsStorage storage;

    private BucketCleaner(ZkAgreementsStorage storage) {
      this.storage = storage;
    }

    @Override
    public void run() {
      try {
        storage.gcBuckets();
      } catch (IOException e) {
        LOG.error(e);
      } catch (KeeperException e) {
        LOG.error(e);
      } catch (InterruptedException e) {
        LOG.error(e);
      }
    }
  }


  private void gcBuckets() throws InterruptedException, IOException, KeeperException {
    List<String> znodes = zooKeeper.getChildren(zkAgreementsPath);
    HashSet<String> locks = Sets.newHashSet();
    for (String lock : Iterables.filter(znodes, BUCKET_LOCK_ZNODE)) {
      locks.add(lock.replace(".lock", "")); // will remove locked buckets from queue
    }
    List<String> buckets = Lists.newArrayList(
            Iterables.filter(znodes,
                    and(BUCKET_ZNODE, not(in(locks)))));
    Collections.sort(buckets);
    int current = buckets.size();
    if (current <= zkMaxBuckets)
      return;
    // assume that after sorting locks will follow bucket znodes
    List<String> toRemove = buckets.subList(0, current - zkMaxBuckets);
    for (String s : toRemove) {
      if (deleteBucket(s))
        return;
    }
  }

  private boolean deleteBucket(String s) throws IOException, KeeperException, InterruptedException {
    long bucketId = Long.parseLong(s);
    String bucketLockPath = getZkAgreementBucketLockPath(bucketId);
    String bucketDeletedPath = getZkAgreementBucketDeletedPath(bucketId);
    zooKeeper.create(bucketDeletedPath,
            EMPTY_BYTES, defaultAcl, CreateMode.PERSISTENT, true);
    ZNode exists = zooKeeper.exists(bucketLockPath);
    if (exists.isExists() && exists.getStat().getEphemeralOwner() != zooKeeper.getSessionId()) {
      return false;
    }
    try {
      zooKeeper.create(bucketLockPath,
              EMPTY_BYTES, defaultAcl, CreateMode.EPHEMERAL, false);
    } catch (KeeperException.NodeExistsException nee) {
      // already locked by other process
      return false;
    }
    try {
      LOG.info("Removing bucket: " + s);
      final String bucketPath = getZkAgreementBucketPath(bucketId);
      List<String> agreements = zooKeeper.getChildren(bucketPath);
      List<List<String>> parts = Lists.partition(agreements, DELETE_BATCH_SIZE);
      for (List<String> part : parts) {
        ListenableFuture<List<Void>> result = Futures.allAsList(
                Lists.transform(part, new Function<String, ListenableFuture<Void>>() {
                  @Override
                  public ListenableFuture<Void> apply(String input) {
                    return zooKeeper.deleteAsync(bucketPath + "/" + input, -1);
                  }
                }));
        try {
          result.get();
        } catch (ExecutionException e) {
          LOG.error("Unable to delete items in bucket " + bucketPath, e);
          return false;
        }
      }
      zooKeeper.delete(bucketPath);
      zooKeeper.delete(bucketDeletedPath);
      LOG.info("Removed bucket: " + s);
    } finally {
      zooKeeper.delete(bucketLockPath);
    }
    return true;
  }
}
