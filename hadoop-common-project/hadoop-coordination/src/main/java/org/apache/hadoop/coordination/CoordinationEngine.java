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
 * <p/>
 * A node submits its proposals to the CoordinationEngine.
 * The CoordinationEngine comes with an agreement and makes a call back.
 * Thus all nodes learn about the agreement.<br/>
 * CoordinationEngine guarantees that all nodes learn about the same
 * agreements in the same order.
 */
public interface CoordinationEngine {

  public static final long INVALID_GSN = -1L;

  /**
   * Initialize CoordinationEngine.
   */
  void initialize(Configuration config);

  /**
   * Start the Engine.
   * Start accepting proposals and learning agreements.
   * @throws IOException
   */
  void start() throws IOException;

  /**
   * Stop the Engine.
   * Stop accepting proposals and learning agreements.
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
   * @throws ProposalSubmissionException if issue occurs on submit
   */
  void submitProposal(final Proposal proposal, boolean checkQuorum)
    throws ProposalSubmissionException;

  /**
   * Get the GSN last seen on this node.
   * @return the GSN of the last agreement successfully executed on the node.
   */
  long lastSeenGlobalSequenceNumber();

  /**
   * When node restarts it may need to recover its state from
   * CoordinationEngine by replaying missed agreements.
   * If it missed too many agreements then it may not be able to learn them
   * as the older ones have been discarded.
   * @return true  if the node can recover by applying agreements or
   *         false if the node is beyond the recovery threshold.
   */
  boolean canRecoverAgreements();

  /**
   * @return true if this node can propose.
   */
  boolean canPropose();

  /**
   * Stop learning agreements. May still be able to propose.
   */
  void pauseLearning();

  /**
   * Resume learning agreements from the specified GSN.<p/>
   * If fromGSN is greater than the last seen GSN,
   * then skip agreements until fromGSN.
   * Otherwise, start learning from the last seen GSN.
   */
  void resumeLearning(long fromGSN);

  /**
   * Checks if a quorum is available.
   * If not, throws NoQuorumException, forcing a client to fail-over.
   */
  void checkQuorum() throws NoQuorumException;
}
