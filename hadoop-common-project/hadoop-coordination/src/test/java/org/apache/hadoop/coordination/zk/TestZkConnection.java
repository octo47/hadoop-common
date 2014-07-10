package org.apache.hadoop.coordination.zk;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Throwables;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.coordination.NoQuorumException;
import org.apache.zookeeper.ClientCnxn;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestZkConnection {

  private static final Log LOG = LogFactory.getLog(ZkConnection.class);

  private static final byte[] EMPTY_BYTES = {};

  private MiniZooKeeperCluster zkCluster;

  @After
  public void fini() throws IOException {
    stopMiniZk();
  }

  @Test
  public void testDisconnects() throws IOException, InterruptedException {
    try {
      new ZkConnection("localhost:3000", 1000);
      Assert.fail();
    } catch (NoQuorumException nqe) {
      // OK
    }
    LOG.info("Starting miniZK");
    startMiniZk();
    ZkConnection zkcon = new ZkConnection(zkCluster.getConnectString(), 1000);
    Assert.assertTrue(zkcon.isAlive());
    Assert.assertTrue(zkcon.isConnected());
    LOG.info("Disconnecting miniZK");
    getClientCnxn(zkcon).disconnect();
    try {
      zkcon.exists("/");
      LOG.info("exists succesfully completed");
      Assert.fail();
    } catch (NoQuorumException e) {
      Assert.assertFalse(zkcon.isAlive());
      Assert.assertFalse(zkcon.isConnected());
    } catch (KeeperException e) {
      Assert.fail();
    }
    zkcon.close();
  }

  @Test
  public void testBasicOps() throws IOException, InterruptedException, KeeperException {
    startMiniZk();
    final ZkConnection zkcon = new ZkConnection(zkCluster.getConnectString(), 2000);
    zkcon.delete("/test1");
    zkcon.create("/test1", EMPTY_BYTES, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    try {
      zkcon.create("/test1", EMPTY_BYTES, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      Assert.fail();
    } catch (KeeperException.NodeExistsException nee) {
    } catch (Exception e) {
      Assert.fail();
    }
    Assert.assertEquals(0, zkcon.getData("/test1").getStat().getCversion());

    String path = zkcon.create("/test1/abc-", EMPTY_BYTES,
            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
    Assert.assertEquals("/test1/abc-0000000000", path);
    Assert.assertEquals(1, zkcon.getData("/test1").getStat().getCversion());

    zkcon.delete(path, 0);
    Assert.assertEquals(2, zkcon.getData("/test1").getStat().getCversion());
    zkcon.delete(path, -1);
    zkcon.delete(path);

    zkcon.exists("/test1/abc-0000000002", true);
    final CountDownLatch fired = new CountDownLatch(1);
    zkcon.addWatcher(new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        if (event.getType().equals(Event.EventType.NodeCreated)
                && event.getPath().startsWith("/test1"))
          fired.countDown();
      }
    });

    String outoforder = zkcon.create("/test1/node-added", EMPTY_BYTES,
            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    String path2 = zkcon.create("/test1/abc-", EMPTY_BYTES,
            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
    Assert.assertEquals("/test1/abc-0000000002", path2);
    Assert.assertEquals(4, zkcon.getData("/test1").getStat().getCversion());

    Assert.assertTrue(fired.await(1, TimeUnit.SECONDS));

    zkcon.close();
  }

  private static final Field zkField;
  private static final Field zkClientConnField;

  static {
    try {
      zkField = ZkConnection.class.getDeclaredField("zk");
      zkField.setAccessible(true);
      zkClientConnField = ZooKeeper.class.getDeclaredField("cnxn");
      zkClientConnField.setAccessible(true);
    } catch (NoSuchFieldException e) {
      throw new IllegalStateException("Can't find zk field");
    }
  }

  @SuppressWarnings("unchecked")
  private <T> T getFieldValue(Object obj, Field fld) {
    try {
      return (T) fld.get(obj);
    } catch (IllegalAccessException e) {
      throw Throwables.propagate(e);
    }
  }

  private ZooKeeper getZooKeeper(ZkConnection zkConn) {
    return getFieldValue(zkConn, zkField);
  }

  private ClientCnxn getClientCnxn(ZkConnection zkConn) {
    ZooKeeper zk = getFieldValue(zkConn, zkField);
    return getFieldValue(zk, zkClientConnField);
  }

  private void startMiniZk() throws IOException, InterruptedException {
    startMiniZk(1);
  }

  private void startMiniZk(int numServers) throws IOException, InterruptedException {
    stopMiniZk();
    zkCluster = new MiniZooKeeperCluster();
    zkCluster.startup(
            new File(System.getProperty("test.build.dir", "target/test-dir"),
                    "testSimpleProposals"), numServers);
  }

  private void stopMiniZk() {
    RuntimeException t = null;
    if (zkCluster != null) {
      try {
        zkCluster.shutdown();
      } catch (IOException e) {
        t = Throwables.propagate(e);
      }
      zkCluster = null;
    }
    if (t != null)
      throw t;
  }
}
