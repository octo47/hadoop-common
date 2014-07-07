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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.coordination.Agreement;
import org.apache.hadoop.coordination.AgreementHandler;

/**
 * Handles execution of {@link SampleProposal} agreements.
 */
public class SampleHandler implements AgreementHandler<SampleLearner> {
  private static final Log LOG = LogFactory.getLog(SampleHandler.class);

  private SampleLearner learner;

  public SampleHandler(SampleLearner learner) {
    setLearner(learner);
  }

  @Override
  public SampleLearner getLearner() {
    return learner;
  }

  @Override
  public void setLearner(SampleLearner learner) {
    this.learner = learner;
  }

  @Override
  public void executeAgreement(Agreement<?,?> agreement) {
    SampleProposal agreed = (SampleProposal) agreement;
    try {
      int s = agreed.execute(getLearner());
      LOG.info(learner.getClass().getSimpleName() + " state updated to " + s);
    } catch (IOException e) {
      LOG.error("Failed to apply agreement: " + agreement, e);
    }
  }
}
