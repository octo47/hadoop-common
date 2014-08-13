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
 * Interface for proposals submitted to the {@link CoordinationEngine}.
 * <p/>
 * Serialization is specified using readObject() and writeObject(). If custom
 * serialization is not provided Java defaults will be used.
 */
public interface Proposal extends Serializable {

  /**
   * @return the identity of the proposal. A unique identity for each proposal
   *         is necessary to distinguish between two independent proposals with
   *         the same value made by the same proposer.
   */
  String getProposalIdentity();

  /**
   * @return the identity of the {@link CoordinationEngine} instance making the
   *         proposal.
   */
  String getCeIdentity();

  /**
   * @return the value being proposed, serialized as a byte array.
   */
  byte[] getValue();
}
