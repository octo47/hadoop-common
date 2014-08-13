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

import javax.annotation.concurrent.Immutable;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.coordination.Agreement;
import org.apache.hadoop.coordination.AgreementHandler;
import org.apache.hadoop.coordination.Proposal;

/**
 * Handles agreed {@link SampleProposal}s.
 */
@Immutable
public class SampleHandler implements AgreementHandler<SampleLearner> {
  private static final Log LOG = LogFactory.getLog(SampleHandler.class);

  private final SampleLearner learner;

  public SampleHandler(final SampleLearner learner) {
    this.learner = learner;
  }

  @Override
  public SampleLearner getLearner() {
    return learner;
  }

  @Override
  public void process(String proposalIdentity,
                      String ceIdentity,
                      Agreement<SampleLearner, Object> agreement)
          throws Exception {
    Object o = agreement.execute(proposalIdentity, ceIdentity, learner);
    if (LOG.isTraceEnabled()) {
      LOG.trace(learner.getClass().getSimpleName() + " state updated to " + o);
    }
  }
}
