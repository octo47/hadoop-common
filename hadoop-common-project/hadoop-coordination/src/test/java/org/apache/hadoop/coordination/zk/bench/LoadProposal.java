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
import java.io.ObjectStreamException;
import java.io.Serializable;

import org.apache.hadoop.coordination.ConsensusProposal;

/**
 * Main workhorse of load test, this proposals issued by
 * load generator threads and accounted by Learner.
 */
public class LoadProposal extends ConsensusProposal<LoadTool.LoadGenerator, Long> implements Serializable {

  private Long generatorId;
  private Long value;
  private Long iteration;

  public Long getGeneratorId() {
    return generatorId;
  }

  public Long getValue() {
    return value;
  }

  public Long getIteration() {
    return iteration;
  }

  public LoadProposal(Serializable nodeId, Long generatorId, Long value, Long iteration) {
    super(nodeId);
    this.value = value;
    this.generatorId = generatorId;
    this.iteration = iteration;
  }

  private void writeObject(java.io.ObjectOutputStream out)
          throws IOException {
    out.writeLong(value);
    out.writeLong(generatorId);
    out.writeLong(iteration);
  }

  private void readObject(java.io.ObjectInputStream in)
          throws IOException, ClassNotFoundException {
    value = in.readLong();
    generatorId = in.readLong();
    iteration = in.readLong();
  }

  private void readObjectNoData()
          throws ObjectStreamException {
  }

  @Override
  public Long execute(LoadTool.LoadGenerator loadGenerator) throws IOException {
    return loadGenerator.advance(this);
  }

  @Override
  public String toString() {
    return "LoadProposal{" +
            "generatorId=" + generatorId +
            ", value=" + value +
            ", iteration=" + iteration +
            '}';
  }
}
