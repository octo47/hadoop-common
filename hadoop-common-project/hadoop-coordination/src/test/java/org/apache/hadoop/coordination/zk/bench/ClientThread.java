/*
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

import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.coordination.NoQuorumException;
import org.apache.hadoop.coordination.ProposalNotAcceptedException;

public class ClientThread implements Runnable {

  private static final Log LOG = LogFactory.getLog(ClientThread.class);

  private String name;
  private final LoadLearner generator;
  private LoadToolMetrics metrics;
  private final long millisToRun;
  private final long maxIterations;

  private final AtomicBoolean done = new AtomicBoolean(false);
  private volatile Thread thread;
  private volatile Long id;

  public ClientThread(LoadLearner generator,
                      LoadToolMetrics metrics, long millisToRun, long maxIterations) {
    this.generator = generator;
    this.metrics = metrics;
    this.millisToRun = millisToRun;
    this.maxIterations = maxIterations;
  }

  @Override
  public void run() {
    try {
      thread = Thread.currentThread();
      id = generator.register(this).get();
      Random rnd = new Random(id);
      LOG.info("Registered with id = " + id);

      Stopwatch runningTime = new Stopwatch();
      runningTime.start();
      long iteration = 0;
      while (!Thread.currentThread().isInterrupted()
              && !done.get()
              && iteration < maxIterations
              && (millisToRun < 0 || runningTime.elapsedMillis() < millisToRun)) {
        final LoadProposal proposal = new LoadProposal(
                generator.getLocalNodeId(), id, rnd.nextLong(), iteration);
        if (LOG.isTraceEnabled())
          LOG.trace("Proposing " + proposal + " from " + name);
        Stopwatch sw = new Stopwatch();
        sw.start();

        SettableFuture<Long> future = generator.makeProposal(proposal);
        try {
          future.get(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          LOG.warn("Proposal interrupted, waiting unconditionally");
          future.get();
        } catch (TimeoutException e) {
          LOG.warn("Proposal longer then 1 second, waiting unconditionally");
          future.get();
        }
        sw.stop();
        metrics.addProposal(sw.elapsedTime(TimeUnit.MILLISECONDS));
        iteration++;
      }
      LOG.info("Done client " + name);
    } catch (Exception e) {
      LOG.error("Client " + name + " failed", e);
      throw Throwables.propagate(e);
    } finally {
      generator.unregister(this);
    }
  }

  public long getId() {
    return id;
  }

  public void stop() {
    done.set(true);
    if (thread != null)
      thread.interrupt();
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }
}
