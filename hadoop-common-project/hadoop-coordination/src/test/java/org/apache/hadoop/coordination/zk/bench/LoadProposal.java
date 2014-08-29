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

import javax.annotation.concurrent.Immutable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamException;
import java.io.Serializable;

import org.apache.hadoop.coordination.Agreement;
import org.apache.hadoop.coordination.Proposal;

/**
 * The main part of the load test, this is a value object issued by the
 * {@link LoadTool.LoadThread} and received by the {@link LoadTool.LoadGenerator}.
 * When applied to the {@link LoadTool.LoadGenerator} it returns a {@link Long}.
 */
@Immutable
public class LoadProposal implements
        Proposal, Agreement<LoadLearner, Long>, Serializable {

  private static final long serialVersionUID = 1L;

  private Long clientId;
  private Long value;
  private Long iteration;
  private Long creationTime;
  private String proposalNodeId;

  public String getProposalNodeId() {
    return proposalNodeId;
  }

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

  public LoadProposal(String proposalNodeId, Long clientId, Long value, Long iteration) {
    this.proposalNodeId = proposalNodeId;
    this.value = value;
    this.clientId = clientId;
    this.iteration = iteration;
    this.creationTime = System.currentTimeMillis();
  }

  private void writeObject(ObjectOutputStream out)
          throws IOException {
    out.writeUTF(proposalNodeId);
    out.writeLong(value);
    out.writeLong(clientId);
    out.writeLong(iteration);
    out.writeLong(creationTime);
  }

  private void readObject(ObjectInputStream in)
          throws IOException, ClassNotFoundException {
    proposalNodeId = in.readUTF();
    value = in.readLong();
    clientId = in.readLong();
    iteration = in.readLong();
    creationTime = in.readLong();
  }

  private void readObjectNoData()
          throws ObjectStreamException {
    throw new IllegalArgumentException("Can't handle empty object");
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    LoadProposal that = (LoadProposal) o;

    if (clientId != null ? !clientId.equals(that.clientId) : that.clientId != null) return false;
    if (creationTime != null ? !creationTime.equals(that.creationTime) : that.creationTime != null)
      return false;
    if (iteration != null ? !iteration.equals(that.iteration) : that.iteration != null)
      return false;
    if (proposalNodeId != null ? !proposalNodeId.equals(that.proposalNodeId) : that.proposalNodeId != null)
      return false;
    if (value != null ? !value.equals(that.value) : that.value != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = clientId != null ? clientId.hashCode() : 0;
    result = 31 * result + (value != null ? value.hashCode() : 0);
    result = 31 * result + (iteration != null ? iteration.hashCode() : 0);
    result = 31 * result + (creationTime != null ? creationTime.hashCode() : 0);
    result = 31 * result + (proposalNodeId != null ? proposalNodeId.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "LoadProposal{" +
            "clientId=" + clientId +
            ", value=" + value +
            ", iteration=" + iteration +
            ", creationTime=" + creationTime +
            ", proposalNodeId='" + proposalNodeId + '\'' +
            '}';
  }

  @Override
  public long getGlobalSequenceNumber() {
    throw new UnsupportedOperationException("Not known how to get GSN");
  }

  @Override
  public Long execute(LoadLearner callBackObject) throws IOException {
    return callBackObject.handleProposal(this);
  }
}
