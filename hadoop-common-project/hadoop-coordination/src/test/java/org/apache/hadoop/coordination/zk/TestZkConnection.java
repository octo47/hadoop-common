package org.apache.hadoop.coordination.zk;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class TestZkConnection {

  private static final Log LOG = LogFactory.getLog(ZkConnection.class);

  private static final byte[] EMPTY_BYTES = {};

  private MiniZooKeeperCluster zkCluster;
  private ExecutorService executor = Executors.newSingleThreadExecutor();

  @After
  public void fini() throws IOException {
    stopMiniZk();
  }

  @Test
  public void testDisconnects() throws IOException, InterruptedException, KeeperException {

    try {
      new ZkConnection(new MyZkFactory(true));
      Assert.fail();
    } catch (NoQuorumException nqe) {
      // OK
    }

    final MyZkFactory zk = new MyZkFactory();
    ZkConnection zkcon = getZkConnection(zk);
    zk.watcher.process(noteTypeEvent(Watcher.Event.KeeperState.Disconnected));
    zk.watcher.process(noteTypeEvent(Watcher.Event.KeeperState.SyncConnected));
    when(zk.mock.getState()).thenReturn(ZooKeeper.States.CONNECTED);
    Assert.assertTrue(zkcon.isConnected());

    zk.watcher.process(noteTypeEvent(Watcher.Event.KeeperState.Expired));
    verify(zk.mock).close();
    // zk should be closed
    Assert.assertFalse(zkcon.isConnected());
    Assert.assertFalse(zkcon.isAlive());
  }

  private ZkConnection getZkConnection(MyZkFactory zk) throws KeeperException, InterruptedException, IOException {
    final ArgumentCaptor<Watcher> watcherArgument =
            ArgumentCaptor.forClass(Watcher.class);
    when(zk.mock.exists(eq("/"), eq(false))).thenReturn(new Stat());
    when(zk.mock.getSessionId()).thenReturn(123l);
    ZkConnection zkcon = new ZkConnection(zk);
    verify(zk.mock).register(watcherArgument.capture());
    zk.watcher = watcherArgument.getValue(); // reregistered
    return zkcon;
  }

  @Test
  public void testLiveZk() throws IOException, InterruptedException, KeeperException {
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

    // Testing setdata
    String outoforder = zkcon.create("/test1/node-added", EMPTY_BYTES,
            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    final String hello = "Hello";
    zkcon.setData("/test1/node-added", hello.getBytes("UTF-8"), -1);
    final ZNode data = zkcon.getData("/test1/node-added");
    Assert.assertTrue(data.isExists());
    Assert.assertEquals(new String(data.getData(), "UTF-8"), hello);

    // out of order child insertion should bump node version
    String path2 = zkcon.create("/test1/abc-", EMPTY_BYTES,
            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
    Assert.assertEquals("/test1/abc-0000000002", path2);
    Assert.assertEquals(4, zkcon.getData("/test1").getStat().getCversion());

    Assert.assertTrue(fired.await(1, TimeUnit.SECONDS));

    Assert.assertEquals(2, zkcon.getChildren("/test1").size());

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


  private WatchedEvent noteTypeEvent(Watcher.Event.KeeperState type) {
    return new WatchedEvent(
            Watcher.Event.EventType.None,
            type, null);
  }

  class MyZkFactory implements ZkConnection.ZkFactory {

    private final ZooKeeper mock;
    private final boolean failToConnect;
    String quorumString;
    Watcher watcher;
    int sessionTimeout;

    MyZkFactory() {
      this(false);
    }

    MyZkFactory(boolean failToConnect) {
      this.quorumString = "localhost:1234";
      this.sessionTimeout = 5000;
      // , withSettings().verboseLogging()
      this.mock = mock(ZooKeeper.class);
      this.failToConnect = failToConnect;
    }

    public String getQuorumString() {
      return quorumString;
    }

    public int getSessionTimeout() {
      return sessionTimeout;
    }

    @Override
    public ZooKeeper create(Watcher defaultWatcher) throws IOException {
      this.watcher = defaultWatcher;
      if (failToConnect)
        throw new IOException("Connection failed");
      executor.submit(new Runnable() {
        @Override
        public void run() {
          watcher.process(noteTypeEvent(Watcher.Event.KeeperState.SyncConnected));
        }
      });
      return mock;
    }
  }
}
