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

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.coordination.NoQuorumException;
import org.apache.hadoop.fs.FileUtil;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.test.ClientBase;

/**
 * This class is copied here from HBase test harness.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MiniZooKeeperCluster {
  private static final Log LOG = LogFactory.getLog(MiniZooKeeperCluster.class);

  private static final int TICK_TIME = 2000;
  private static final int CONNECTION_TIMEOUT = 30000;
  public static final int DEFAULT_ZOOKEPER_MAX_CLIENT_CNXNS = 300;

  private boolean started;

  /**
   * The default port. If zero, we use a random port.
   */
  private int defaultClientPort = 0;
  private String defaultHost = "127.0.0.1";

  private List<NIOServerCnxnFactory> standaloneServerFactoryList;
  private List<ZooKeeperServer> zooKeeperServers;
  private List<Integer> clientPortList;

  private int activeZKServerIndex;
  private String connectString;

  public MiniZooKeeperCluster() {
    this.started = false;
    activeZKServerIndex = -1;
    zooKeeperServers = new ArrayList<ZooKeeperServer>();
    clientPortList = new ArrayList<Integer>();
    standaloneServerFactoryList = new ArrayList<NIOServerCnxnFactory>();
  }

  public void setDefaultClientPort(int clientPort) {
    if (clientPort <= 0) {
      throw new IllegalArgumentException("Invalid default ZK client port: "
              + clientPort);
    }
    this.defaultClientPort = clientPort;
  }

  public String getConnectString() {
    return connectString;
  }

  /**
   * Selects a ZK client port. Returns the default port if specified.
   * Otherwise, returns a random port. The random port is selected from the
   * range between 49152 to 65535. These ports cannot be registered with IANA
   * and are intended for dynamic allocation (see http://bit.ly/dynports).
   */
  private int selectClientPort() {
    if (defaultClientPort > 0) {
      return defaultClientPort;
    }
    return 0xc000 + new Random().nextInt(0x3f00);
  }

  /**
   * @param baseDir             baseDir for ZooKeeper cluster
   * @param numZooKeeperServers number of ZooKeepers desired
   * @return ClientPort server bound to, -1 if there was a
   * binding problem and we couldn't pick another port.
   * @throws IOException
   * @throws InterruptedException
   */
  public int startup(File baseDir, int numZooKeeperServers) throws IOException,
          InterruptedException {
    if (numZooKeeperServers <= 0)
      return -1;

    ClientBase.setupTestEnv();
    shutdown();

    int tentativePort = selectClientPort();

    // running all the ZK servers
    for (int i = 0; i < numZooKeeperServers; i++) {
      File dir = new File(baseDir, "zookeeper_" + i).getAbsoluteFile();
      recreateDir(dir);
      ZooKeeperServer server = new ZooKeeperServer(dir, dir, TICK_TIME);
      NIOServerCnxnFactory standaloneServerFactory;
      while (true) {
        try {
          standaloneServerFactory = new NIOServerCnxnFactory();
          standaloneServerFactory.configure(
                  new InetSocketAddress(tentativePort),
                  DEFAULT_ZOOKEPER_MAX_CLIENT_CNXNS);
        } catch (BindException e) {
          LOG.debug("Failed binding ZK Server to client port: " +
                  tentativePort, e);
          // We're told to use some port but it's occupied, fail
          if (defaultClientPort > 0) return -1;
          // This port is already in use, try to use another.
          tentativePort = selectClientPort();
          continue;
        }
        break;
      }

      // Start up this ZK server
      standaloneServerFactory.startup(server);
      if (!ClientBase.waitForServerUp(defaultHost + ":" + tentativePort,
              CONNECTION_TIMEOUT)) {
        throw new IOException("Waiting for startup of standalone server");
      }

      // We have selected this port as a client port.
      clientPortList.add(tentativePort);
      standaloneServerFactoryList.add(standaloneServerFactory);
      zooKeeperServers.add(server);
      tentativePort++; //for the next server
    }

    // set the first one to be active ZK; Others are backups
    activeZKServerIndex = 0;
    started = true;
    final int clientPort = clientPortList.get(activeZKServerIndex);
    StringBuilder sb = new StringBuilder();
    for (ZooKeeperServer zooKeeperServer : zooKeeperServers) {
      sb.append(zooKeeperServer.getServerCnxnFactory().getLocalAddress().getHostString());
      sb.append(":").append(zooKeeperServer.getClientPort());
      sb.append(",");
    }

    connectString = sb.toString();
    LOG.info("Started MiniZK Cluster and connect 1 ZK server " +
            "on: " + connectString);
    final SettableFuture<Boolean> connected = SettableFuture.create();
    final ZooKeeper zk = new ZooKeeper(connectString, 1000, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        switch (event.getState()) {
          case SyncConnected:
            connected.set(true);
            break;
          case Expired:
            connected.setException(
                    new NoQuorumException("Failed to establish connection to " + connectString));
        }
      }
    });
    try {
      connected.get();
    } catch (ExecutionException e) {
      throw Throwables.propagate(e.getCause());
    } finally {
      zk.close();
    }
    return clientPort;
  }

  private void recreateDir(File dir) throws IOException {
    if (dir.exists()) {
      if (!FileUtil.fullyDelete(dir)) {
        throw new IOException("Could not delete zk base directory: " + dir);
      }
    }
    try {
      dir.mkdirs();
    } catch (SecurityException e) {
      throw new IOException("creating dir: " + dir, e);
    }
  }

  /**
   * @throws IOException
   */
  public void shutdown() throws IOException {
    if (!started) {
      return;
    }

    // shut down all the zk servers
    for (int i = 0; i < standaloneServerFactoryList.size(); i++) {
      shutdownServerByIndex(i);
    }
    for (ZooKeeperServer zkServer : zooKeeperServers) {
      //explicitly close ZKDatabase since ZookeeperServer does not close them
      zkServer.getZKDatabase().close();
    }

    // clear everything
    started = false;
    activeZKServerIndex = 0;
    standaloneServerFactoryList.clear();
    clientPortList.clear();
    zooKeeperServers.clear();

    LOG.info("Shutdown MiniZK cluster with all ZK servers");
  }

  public void shutdownServerByIndex(int i) throws IOException {
    NIOServerCnxnFactory standaloneServerFactory =
            standaloneServerFactoryList.get(i);
    int clientPort = clientPortList.get(i);

    standaloneServerFactory.shutdown();
    if (!ClientBase.waitForServerDown(defaultHost + ":" + clientPort,
            CONNECTION_TIMEOUT)) {
      throw new IOException("Waiting for shutdown of standalone server");
    }
  }
}
