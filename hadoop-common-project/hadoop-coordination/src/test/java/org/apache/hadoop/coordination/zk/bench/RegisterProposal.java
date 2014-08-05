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
package org.apache.hadoop.coordination.zk.bench;

import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.coordination.ConsensusProposal;

/**
 * Used for loader threads registration.
 */
public class RegisterProposal extends ConsensusProposal<LoadTool.LoadGenerator, Void> implements Serializable {

  private final int requestId;

  public RegisterProposal(Serializable proposerNodeId, int requestId) {
    super(proposerNodeId);
    this.requestId = requestId;
  }

  @Override
  public Void execute(LoadTool.LoadGenerator callBackObject) throws IOException {
    callBackObject.register(getProposerNodeId(), requestId);
    return null;
  }
}
