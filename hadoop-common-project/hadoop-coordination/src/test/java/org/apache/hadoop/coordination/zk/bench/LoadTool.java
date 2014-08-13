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

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.coordination.Agreement;
import org.apache.hadoop.coordination.AgreementHandler;
import org.apache.hadoop.coordination.Proposal;
import org.apache.hadoop.coordination.ProposalNotAcceptedException;
import org.apache.hadoop.coordination.zk.ZKConfigKeys;
import org.apache.hadoop.coordination.zk.ZKCoordinationEngine;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetUtils;

/**
 * Simple load tool for CE, which uses an instance of the ZKCoordinationEngine
 * to deliver agreements to the {@link LoadGenerator}.
 *
 * The idea is to exploit the idea of global sequencing. Each load thread
 * submits a {@link RegisterProposal} and waits for the corresponding agreement,
 * which is delivered to it via the {@link CoordinationHandler}. Each
 * {@link RegisterProposal} has a global sequence number (GSN) and this value is
 * used to assign a unique id to a generator thread. Also for all threads
 * Learner maintains Random initialized with seed == GSN. That gives a
 * predictable pseudorandom sequence in any JVM.
 *
 * Upon the arrival of a {@link LoadProposal}, the learner (the
 * {@code LoadGenerator}) checks that sequence is the same as on generator side
 * (generator uses Random also initialized with GSN).
 *
 * Ideally, system should behave well even with zk disconnects and restarts.
 */
public class LoadTool {

  private static final Log LOG = LogFactory.getLog(LoadTool.class);

  public static final String CE_BENCH_ITERATIONS_KEY = "ce.bench.iterations";
  public static final String CE_BENCH_SECONDS_KEY = "ce.bench.seconds";
  public static final String CE_BENCH_THREADS_KEY = "ce.bench.threads";

  private final Configuration conf;
  private final ZKCoordinationEngine<LoadLearner> engine;

  private volatile LoadLearner generator;

  LoadTool(Configuration conf) {
    this.conf = conf;
    String nodeId = this.conf.get(ZKConfigKeys.CE_ZK_NODE_ID_KEY, null);
    if (nodeId == null) {
      nodeId = System.getProperty(ZKConfigKeys.CE_ZK_NODE_ID_KEY);
      if (nodeId == null) {
        nodeId = NetUtils.getHostname();
      }
    }
    LOG.info("Starting LoadTool as nodeId: " + nodeId);
    this.engine = new ZKCoordinationEngine<LoadLearner>("engine");
  }

  public void run() throws ProposalNotAcceptedException, InterruptedException {
    generator = new LoadLearner(engine);
    engine.init(conf);
    engine.start();
    engine.deliverAgreements(new CoordinationHandler(generator));
    final int numThreads = conf.getInt(CE_BENCH_THREADS_KEY, 5);
    LOG.info("Starting " + numThreads + " threads");
    LoadToolMetrics metrics = LoadToolMetrics.create(this, 0);
    final ExecutorService service = Executors.newCachedThreadPool();
    for (int i = 0; i < numThreads; i++) {
      service.submit(new ClientThread(generator, metrics,
              conf.getLong(CE_BENCH_SECONDS_KEY, -1) * 1000,
              conf.getLong(CE_BENCH_ITERATIONS_KEY, Long.MAX_VALUE)));
    }
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          service.shutdownNow();
          generator.stop();
        } catch (InterruptedException e) {
          LOG.error(e);
        }
        engine.stop();
      }
    }));
    generator.awaitThreads();
    service.shutdownNow();
  }

  public String getNodeId() {
    return this.conf.get(ZKConfigKeys.CE_ZK_NODE_ID_KEY, "undefined-node-id");
  }

  static class CoordinationHandler implements AgreementHandler<LoadLearner> {

    private LoadLearner loadLearner;

    CoordinationHandler(LoadLearner loadLearner) {
      this.loadLearner = loadLearner;
    }

    @Override
    public LoadLearner getLearner() {
      return loadLearner;
    }

    @Override
    public void process(String proposalIdentity, String ceIdentity, Agreement<LoadLearner, Object> value)
            throws Exception {
      try {
        value.execute(proposalIdentity, ceIdentity, getLearner());
      } catch (IOException e) {
        LOG.error("Failed to apply agreement: " + value, e);
      }
    }
  }

  public static void main(String[] args) throws ProposalNotAcceptedException, InterruptedException {
    Configuration conf = new Configuration();
    conf.addResource(args[0]);
    final MetricsSystem metricsSystem = DefaultMetricsSystem.initialize("load");
    metricsSystem.register("stdout", "Stdout sink", new MetricsStdoutSink());
    final LoadTool loadTool = new LoadTool(conf);
    loadTool.run();
  }
}
