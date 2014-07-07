package org.apache.hadoop.coordination.zk;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;

import com.google.common.base.Throwables;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.coordination.NoQuorumException;
import org.apache.zookeeper.ClientCnxn;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class ZkConnectionTest {

  private static final Log LOG = LogFactory.getLog(ZkConnection.class);

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
    LOG.info("Disconnecting miniZK");
    getClientCnxn(zkcon).disconnect();
    try {
      zkcon.exists("/");
      LOG.info("exists succesfully completed");
      Assert.fail();
    } catch (NoQuorumException e) {
    } catch (KeeperException e) {
      Assert.fail();
    }
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
