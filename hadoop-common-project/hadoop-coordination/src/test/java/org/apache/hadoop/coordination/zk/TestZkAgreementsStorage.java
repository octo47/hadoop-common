package org.apache.hadoop.coordination.zk;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.coordination.zk.protobuf.ZkCoordinationProtocol;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class TestZkAgreementsStorage {

  private byte[] EMPTY = new byte[0];
  private byte[] PROPOSAL = "prop".getBytes();

  private final String zkAgreementsPath = "/agreements";
  private final String zkStatePath = zkAgreementsPath + ZKConfigKeys.ZK_BUCKETS_STATE_PATH;
  private MiniZooKeeperCluster zkCluster;

  @Before
  public void init() throws IOException, InterruptedException {
    zkCluster = new MiniZooKeeperCluster();
    zkCluster.startup(
            new File(System.getProperty("test.build.dir", "target/test-dir"),
                    "testSimpleProposals"), 1);

  }

  @After
  public void fini() throws IOException {
    zkCluster.shutdown();
  }

  @Test
  public void testInit() throws InterruptedException, IOException, KeeperException {
    final ZkConnection zkConnection = initZk();
    final ZkAgreementsStorage storage = new ZkAgreementsStorage(zkConnection,
            zkAgreementsPath, 1, MoreExecutors.sameThreadExecutor());
    storage.start();

    // check state save
    ArgumentCaptor<byte[]> state = ArgumentCaptor.forClass(byte[].class);
    verify(zkConnection).create(eq(zkStatePath), state.capture(),
            Matchers.<ArrayList<ACL>>any(), eq(CreateMode.PERSISTENT), eq(true));
    final ZkCoordinationProtocol.ZkBucketsState parsedState =
            ZkCoordinationProtocol.ZkBucketsState.parseFrom(state.getValue());
    Assert.assertEquals(1, parsedState.getBucketDigits());
    Assert.assertEquals(0, parsedState.getMaxBucket());

    // check bucket creation
    verify(zkConnection).create(eq(zkAgreementsPath + "/0000000000000"), any(byte[].class),
            Matchers.<ArrayList<ACL>>any(), eq(CreateMode.PERSISTENT), eq(true));

    storage.stop();
  }

  @Test
  public void testWriting()
          throws Exception {
    final ZkConnection zkConnection = initZk();
    final ZkAgreementsStorage storage = new ZkAgreementsStorage(zkConnection,
            zkAgreementsPath, 1, Executors.newSingleThreadExecutor());
    storage.start();

    final int TIMEOUT = 100000;
    for (int i = 0; i < 10; i++) {
      final String path0 = storage.writeProposal(PROPOSAL);
      Assert.assertEquals(storage.getExpectedAgreementZNodePath(0, i), path0);
    }
    verifyBucketState(zkConnection, 0);

    final String pathBucket1 = storage.writeProposal(PROPOSAL);
    Assert.assertEquals(storage.getExpectedAgreementZNodePath(1, 0), pathBucket1);
    verifyUpdateBucketState(zkConnection, 1, 0);

    // check state save
    for (int i = 1; i < 10; i++) {
      final String path0 = storage.writeProposal(PROPOSAL);
      Assert.assertEquals(storage.getExpectedAgreementZNodePath(1, i), path0);
    }

    final String pathBucket2 = storage.writeProposal(PROPOSAL);
    Assert.assertEquals(storage.getExpectedAgreementZNodePath(2, 0), pathBucket2);
    verifyUpdateBucketState(zkConnection, 2, 1);

    storage.stop();
  }

  private void verifyBucketState(ZkConnection zkConnection, int maxBucket)
          throws IOException, KeeperException, InterruptedException {
    ArgumentCaptor<byte[]> state = ArgumentCaptor.forClass(byte[].class);
    verify(zkConnection).create(eq(zkStatePath), state.capture(),
            Matchers.<ArrayList<ACL>>any(), eq(CreateMode.PERSISTENT), eq(true));
    final ZkCoordinationProtocol.ZkBucketsState parsedState =
            ZkCoordinationProtocol.ZkBucketsState.parseFrom(state.getValue());
    Assert.assertEquals(1, parsedState.getBucketDigits());
    Assert.assertEquals(maxBucket, parsedState.getMaxBucket());
  }

  private void verifyUpdateBucketState(ZkConnection zkConnection, int maxBucket, int version)
          throws IOException, KeeperException, InterruptedException {
    ArgumentCaptor<byte[]> state = ArgumentCaptor.forClass(byte[].class);
    verify(zkConnection).setData(eq(zkStatePath), state.capture(), eq(version));
    final ZkCoordinationProtocol.ZkBucketsState parsedState =
            ZkCoordinationProtocol.ZkBucketsState.parseFrom(state.getValue());
    Assert.assertEquals(1, parsedState.getBucketDigits());
    Assert.assertEquals(maxBucket, parsedState.getMaxBucket());
  }

  private ZkConnection initZk() throws IOException, KeeperException, InterruptedException {
    final ZkConnection zkConnection = new ZkConnection(zkCluster.getConnectString(), 600000);
    zkConnection.create(zkAgreementsPath, EMPTY, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    return spy(zkConnection);
  }
}
