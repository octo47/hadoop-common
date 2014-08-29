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
package org.apache.hadoop.coordination.zk;

import java.io.IOException;

import org.apache.hadoop.coordination.Agreement;

/**
 * Interface for handling execution of agreements
 * for a particular learner type L and a particular agreement type A.
 * @param L the learner type.
 */
public interface ZKAgreementHandler<L, A extends Agreement<L, ?>> {

  /**
   * Get the learner.
   */
  L getLearner();

  /**
   * Set the learner.
   */
  void setLearner(L learner);

  /**
   * Check whether this handler can execute this agreement or not.
   */
  boolean handles(Agreement<L, ?> agreement);

  /**
   * Execute specified agreement.
   */
  void executeAgreement(Agreement<L, ?> agreement) throws IOException;
}
