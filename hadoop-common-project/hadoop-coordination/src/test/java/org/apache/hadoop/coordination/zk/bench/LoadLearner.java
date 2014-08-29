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

import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.coordination.ProposalSubmissionException;
import org.apache.hadoop.coordination.zk.ZKCoordinationEngine;
import org.apache.hadoop.coordination.zk.ZKSimpleAgreementHandler;

class LoadLearner {

  private static final Log LOG = LogFactory.getLog(LoadLearner.class);


  class State {
    final String threadId;
    final ClientThread thread;
    final SettableFuture<Long> register;

    State(String threadId, ClientThread thread) {
      this.threadId = threadId;
      this.thread = thread;
      this.register = SettableFuture.create();
    }

    public void assignId(long id) {
      register.set(id);
    }

    public void fail(Exception e) {
      register.setException(e);
    }
  }

  private AtomicInteger requestId = new AtomicInteger(0);
  private Lock lock = new ReentrantLock();
  private Condition cond = lock.newCondition();
  private final Map<String, State> threads = Maps.newConcurrentMap();
  private final Map<Long, State> id2Thread = Maps.newConcurrentMap();
  private final Map<Long, Random> state = Maps.newConcurrentMap();
  private final Map<LoadProposal, SettableFuture<Long>> pending = Maps.newConcurrentMap();
  private final ZKCoordinationEngine engine;

  LoadLearner(ZKCoordinationEngine engine) {
    this.engine = engine;
  }

  static String threadId(String nodeId, int requestId) {
    return nodeId + ":" + requestId;
  }

  /**
   * Initiate registration.
   *
   * @param name of the client
   * @return future id
   */
  public ListenableFuture<Long> register(ClientThread ct)
          throws IOException {
    int req = requestId.incrementAndGet();
    String threadId = threadId(getLocalIdentity(), req);
    State state = new State(threadId, ct);
    lock.lock();
    try {
      ct.setName(threadId);
      threads.put(threadId, state);
      engine.submitProposal(new RegisterProposal(
              getLocalIdentity(), req), true);
      return state.register;
    } finally {
      lock.unlock();
    }
  }

  public void unregister(ClientThread lt) {
    lock.lock();
    try {
      threads.remove(lt.getName());
      id2Thread.remove(lt.getId());
      cond.signal();
    } finally {
      lock.unlock();
    }
  }

  public String getLocalIdentity() {
    return engine.getLocalNodeId().toString();
  }

  /**
   * Whence registration arrived, we either register new state for remote
   * generator or start our own. In both case GSN should be the same, so
   * it is safe to assume that Random will be initialized using the same GSN.
   */
  public long handleRegister(String ceIdentity, RegisterProposal registerProposal) {
    Long id = engine.getGlobalSequenceNumber();
    lock.lock();
    try {
      state.put(id, new Random(id));
      // lookup for thread, if found, it is our registration
      // and we should assign id to it
      final State lt = threads.get(
              threadId(ceIdentity, registerProposal.getRequestId()));
      if (lt != null) {
        lt.assignId(id);
        id2Thread.put(id, lt);
      }
    } finally {
      lock.unlock();
    }
    return id;
  }

  public SettableFuture<Long> makeProposal(LoadProposal proposal)
          throws IOException {

    SettableFuture<Long> result = SettableFuture.create();
    pending.put(proposal, result);
    try {
      engine.submitProposal(proposal, false);
    } catch (ProposalSubmissionException pnae) {
      pending.remove(proposal);
      result.setException(pnae);
    }
    return result;
  }

  /**
   * Advance state of thread, compare with expected random sequence.
   */
  public Long handleProposal(LoadProposal proposal) {
    final Long clientId = proposal.getClientId();
    Random random = state.get(clientId);
    if (random == null) {
      return clientId;
    }
    SettableFuture<Long> remove = pending.remove(proposal);
    if (remove != null) {
      remove.set(engine.getGlobalSequenceNumber());
      if (LOG.isTraceEnabled())
        LOG.trace("Complete Proposal " + proposal);
    } else {
      if (proposal.getProposalNodeId().equals(this.getLocalIdentity())) {
        throw new IllegalStateException("Pending map contains no proposals for " + proposal);
      }
    }
    final Long value = proposal.getValue();
    if (!(random.nextLong() == value)) {
      throw new IllegalStateException("Failed at " + clientId +
              " and seq " + value + " on iteration " + proposal.getIteration());
    }
    return clientId;
  }

  public void stop() throws InterruptedException {
    try {
      lock.lock();
      for (State clientThread : threads.values()) {
        clientThread.thread.stop();
      }
    } finally {
      lock.unlock();
    }
  }

  public void awaitThreads() throws InterruptedException {
    lock.lock();
    try {
      while (threads.size() > 0) {
        cond.await(100, TimeUnit.MILLISECONDS);
      }
    } finally {
      lock.unlock();
    }
  }

  public void addHandlers(ZKCoordinationEngine engine) {
    engine.addHandler(new RegisterHandler(this));
    engine.addHandler(new LoadHandler(this));
  }


  static class RegisterHandler extends ZKSimpleAgreementHandler<LoadLearner, RegisterProposal> {
    RegisterHandler(LoadLearner loadLearner) {
      super(RegisterProposal.class, loadLearner);
    }
  }

  static class LoadHandler extends ZKSimpleAgreementHandler<LoadLearner, LoadProposal> {
    LoadHandler(LoadLearner loadLearner) {
      super(LoadProposal.class, loadLearner);
    }
  }

}
