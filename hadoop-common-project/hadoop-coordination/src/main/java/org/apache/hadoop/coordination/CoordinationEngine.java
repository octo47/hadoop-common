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

import org.apache.hadoop.service.Service;

/**
 * The base interface defining a general purpose {@link CoordinationEngine}.
 * <br>
 * A proposer submits {@link Proposal}s to the CoordinationEngine. The
 * CoordinationEngine coordinates the proposals and, when an agreement is made,
 * delivers the agreement to the Learner of type {@link L}.
 */
public interface CoordinationEngine<L> extends Service {

  /**
   * The identity of the coordination engine instance as it is specified by the
   * {@link CoordinationEngine} implementation.
   * <p/>
   * The identity of the CoordinationEngine is used to to identify which
   * instance of the coordination engine made the proposal which resulted in the
   * agreement.
   */
  String getIdentity();

  /**
   * Submits a proposal to the {@link CoordinationEngine}.
   *
   * @param proposal    the {@link Proposal} to be agreed.
   * @param checkQuorum if true the coordination engine should check to see if a
   *                    to check for quorum or not before submitting
   *                    that should be processed by the coordination engine.
   * @throws ProposalNotAcceptedException if an error occurs on submission.
   */
  void submitProposal(final Proposal proposal,
                      final boolean checkQuorum)
      throws ProposalNotAcceptedException;

  /**
   * Deliver the agreed values to the {@link AgreementHandler<L>}.
   *
   * @param consumer the consumer of the agreed values.
   */
  void startDeliveringAgreements(final AgreementHandler<L> consumer);

  /**
   * @return true if the {@link CoordinationEngine} is delivering agreements
   *         to the {@link AgreementHandler<L>}.
   */
  boolean isDeliveringAgreements();

  /**
   * Stop the agreements being delivered to the {@link AgreementHandler<L>}.
   */
  void stopAgreements();
}
