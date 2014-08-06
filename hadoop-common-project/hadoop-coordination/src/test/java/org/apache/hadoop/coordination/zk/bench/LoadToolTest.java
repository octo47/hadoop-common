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
package org.apache.hadoop.coordination.zk.bench;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.coordination.NoQuorumException;
import org.apache.hadoop.coordination.ProposalNotAcceptedException;
import org.apache.hadoop.coordination.zk.MiniZooKeeperCluster;
import org.apache.hadoop.coordination.zk.ZKConfigKeys;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class LoadToolTest {
  private MiniZooKeeperCluster zkCluster;
  private Configuration conf;

  @Before
  public void beforeTest() throws Exception {
    conf = new Configuration();
    conf.set(ZKConfigKeys.CE_ZK_NODE_ID_KEY, "abc");
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
//    final MetricsSystem metricsSystem = DefaultMetricsSystem.initialize("load");
//    metricsSystem.register("stdout", "Stdout sink", new MetricsStdoutSink());
    final LoadTool lt = new LoadTool(conf);
    lt.run();
  }
}
