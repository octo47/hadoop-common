package org.apache.hadoop.coordination.zk.bench;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.coordination.NoQuorumException;
import org.apache.hadoop.coordination.ProposalNotAcceptedException;
import org.apache.hadoop.coordination.zk.MiniZooKeeperCluster;
import org.apache.hadoop.coordination.zk.ZKConfigKeys;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class LoadToolTest {
  private MiniZooKeeperCluster zkCluster;
  private Configuration conf;
  private String proposerNodeId = "abc";

  @Before
  public void beforeTest() throws Exception {
    conf = new Configuration();
    conf.set(ZKConfigKeys.CE_ZK_NODE_ID_KEY, proposerNodeId);
    zkCluster = new MiniZooKeeperCluster();
    zkCluster.startup(
            new File(System.getProperty("test.build.dir", "target/test-dir"),
                    "testSimpleProposals"), 1);
  }

  @After
  public void after() throws Exception {
    // Shutdown ZK minicluster after each test
    if (zkCluster != null) {
      zkCluster.shutdown();
    }
  }

  @Test
  public void testLoadTool() throws ProposalNotAcceptedException, NoQuorumException, InterruptedException {
    conf.set(ZKConfigKeys.CE_ZK_QUORUM_KEY, zkCluster.getConnectString());
    conf.setInt(LoadTool.CE_BENCH_ITERATIONS_KEY, 15);
    final LoadTool lt = new LoadTool(conf);
    lt.run();
  }
}
