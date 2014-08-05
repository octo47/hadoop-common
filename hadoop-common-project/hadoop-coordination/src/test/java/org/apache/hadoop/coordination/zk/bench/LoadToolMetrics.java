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


import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.hadoop.metrics2.lib.MutableRate;

@Metrics(context = "load")
public class LoadToolMetrics {

  final MetricsRegistry registry = new MetricsRegistry("LoadTool");

  private final int[] QUANTILE_INTERVALS = new int[] {
          1*60, // 1m
          5*60, // 5m
          60*60 // 1h
  };

  @Metric(value = "proposals.count", always = true)
  protected MutableRate proposals;
  protected MutableQuantiles[] proposalsLatency;

  LoadToolMetrics() {
    proposalsLatency = new MutableQuantiles[QUANTILE_INTERVALS.length];
    for (int i = 0; i < proposalsLatency.length; i++) {
      int interval = QUANTILE_INTERVALS[i];
      proposalsLatency[i] = registry.newQuantiles(
              "latency" + interval + "s",
              "Proposal latency", "ops", "latencyMicros", interval);
    }  }

  public static LoadToolMetrics create(LoadTool tool, long threadId) {
    LoadToolMetrics m = new LoadToolMetrics();
    return DefaultMetricsSystem.instance().register(
            tool.getNodeId() + "-" + threadId, "CE Load Generation Tool", m);
  }

  public void addProposal(long us) {
    proposals.add(1);
    for (MutableQuantiles mutableQuantiles : proposalsLatency) {
      mutableQuantiles.add(us);
    }
  }
}
