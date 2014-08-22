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

import java.io.Serializable;

/**
 * ConsensusProposal is the base class for {@link Proposal}s.
 * Once agreed upon by {@link CoordinationEngine} it also acts as an agreement.
 */
public abstract class ConsensusProposal<L, R>
                implements Proposal, Agreement<L, R> {
  private static final long serialVersionUID = 1L;

  /** The identity of the node initiating the proposal */
  protected final Serializable proposerNodeId;
  protected long globalSequenceNumber;

  public ConsensusProposal(final Serializable proposerNodeId){
    this.proposerNodeId = proposerNodeId;
    this.globalSequenceNumber = CoordinationEngine.INVALID_GSN;
  }

  /**
   * @return the identity of the node submitting the proposal.
   */
  public Serializable getProposerNodeId() {
    return proposerNodeId;
  }

  @Override // Agreement
  public long getGlobalSequenceNumber() {
    return globalSequenceNumber;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ConsensusProposal)) return false;

    ConsensusProposal that = (ConsensusProposal) o;
    if (!proposerNodeId.equals(that.proposerNodeId)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    return proposerNodeId.hashCode();
  }

  /**
   * @return a String representation of this proposal
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName() + ": proposerId: ");
    sb.append(proposerNodeId);
    return sb.toString();
  }
}
