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
public class LoadProposal
        extends ConsensusProposal<LoadLearner, Long>
        implements Serializable {

  private static final long serialVersionUID = 1L;

  private Long clientId;
  private Long value;
  private Long iteration;
  private Long creationTime;

  public Long getClientId() {
    return clientId;
  }

  public Long getValue() {
    return value;
  }

  public Long getIteration() {
    return iteration;
  }

  public Long getCreationTime() {
    return creationTime;
  }

  public LoadProposal(Serializable nodeId, Long clientId, Long value, Long iteration) {
    super(nodeId);
    this.value = value;
    this.clientId = clientId;
    this.iteration = iteration;
    this.creationTime = System.currentTimeMillis();
  }

  private void writeObject(java.io.ObjectOutputStream out)
          throws IOException {
    out.writeLong(value);
    out.writeLong(clientId);
    out.writeLong(iteration);
    out.writeLong(creationTime);
  }

  private void readObject(java.io.ObjectInputStream in)
          throws IOException, ClassNotFoundException {
    value = in.readLong();
    clientId = in.readLong();
    iteration = in.readLong();
    creationTime = in.readLong();
  }

  private void readObjectNoData()
          throws ObjectStreamException {
  }

  @Override
  public Long execute(LoadLearner loadLearner) throws IOException {
    return loadLearner.handleProposal(this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    LoadProposal that = (LoadProposal) o;

    if (creationTime != null ? !creationTime.equals(that.creationTime) : that.creationTime != null) return false;
    if (clientId != null ? !clientId.equals(that.clientId) : that.clientId != null) return false;
    if (iteration != null ? !iteration.equals(that.iteration) : that.iteration != null) return false;
    if (value != null ? !value.equals(that.value) : that.value != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (clientId != null ? clientId.hashCode() : 0);
    result = 31 * result + (value != null ? value.hashCode() : 0);
    result = 31 * result + (iteration != null ? iteration.hashCode() : 0);
    result = 31 * result + (creationTime != null ? creationTime.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "LoadProposal{" +
            "clientId=" + clientId +
            ", value=" + value +
            ", iteration=" + iteration +
            ", creationTime=" + creationTime +
            '}';
  }
}
