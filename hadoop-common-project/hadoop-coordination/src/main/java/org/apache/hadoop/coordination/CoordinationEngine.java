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
package org.apache.hadoop.coordination;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.conf.Configuration;


/**
 * Base interface defining general purpose CoordinationEngine.
 * <br>
 * A node submits its proposals to the CoordinationEngine.
 * The CoordinationEngine comes with an agreement and makes a call back.
 * Thus all nodes learn about the agreement.<br>
 * CoordinationEngine guarantees that all nodes learn about the same
 * agreements in the same order.
 */
public interface CoordinationEngine {

  static enum ProposalReturnCode { OK, NO_QUORUM }

  /**
   * Initialize the Engine.
   */
  void initialize(Configuration config);

  /**
   * Start the Engine.
   * @throws IOException
   */
  void start() throws IOException;

  /**
   * Stop the Engine.
   */
  void stop();

  /**
   * Obtain ordered collection of node IDs in the membership.
   * This includes all nodes independently on their role and the current
   * liveness status.
   * <p>
   * Provided by CoordinationEngine implementation, the order is
   * to be persisted across node restarts.
   * The order can change when the membership changes.
   *
   * @return ordered collection of all node IDs in the membership.
   */
  List<Serializable> getMembershipNodeIds();

  /**
   * The Id of the current node as it is recognized by the
   * CoordinationEngine implementation.
   * <p>
   * LocalNodeId is passed to the CoordinationEngine with every proposal.
   * The id is retained in the agreement so that the node which originated
   * the proposal could reply to the client.
   */
  Serializable getLocalNodeId();

  /**
   * Submit proposal to coordination engine.
   *
   * @param proposal the {@link Proposal}
   * @param checkQuorum whether to check for quorum or not before submitting
   * that should be processed by the coordination engine.
   *
   * @return an enum value signaling the result of the proposal submission
   * @throws ProposalNotAcceptedException if issue occurs on submit
   */
  ProposalReturnCode submitProposal(final Proposal proposal,
                                    boolean checkQuorum)
    throws ProposalNotAcceptedException;

  /**
   * An agreement in the Global Sequence of events is characterized by
   * the global sequence number with the monotonicity property
   * that agreements with higher GSN are applied later in the sequence.
   * The agreement has the same GSN when it is learned on all nodes.
   * <p/>
   *
   * On boot-up, will retrieve the last seen GSN of this node.
   * @return global sequence number of the agreement
   */
  long getGlobalSequenceNumber();

  /**
   * When node restarts it may need to recover its state from
   * CoordinationEngine by replaying missed agreements.
   * @return true if recovery is possible or false otherwise.
   */
  boolean canRecoverAgreements();

  /**
   * @return true if the coordination engine can propose.
   */
  boolean canPropose();

  /**
   * Pause learning of agreements from CoordinationEngine.
   * Returns immediately if already paused.
   */
  void pauseLearning();

  /**
   * Resume learning of agreements from CoordinationEngine.
   * Returns immediately if already learning.
   */
  void resumeLearning();

  /**
   * Checks if majority of nodes are available online. If not, throws
   * a NoQuorumException, forcing a client fail-over.
   */
  void checkQuorum() throws NoQuorumException;

  /**
   * Register specified {@link AgreementHandler}.
   * Handlers trigger execution of agreements.
   */
  void registerHandler(AgreementHandler<?> handler);
}
