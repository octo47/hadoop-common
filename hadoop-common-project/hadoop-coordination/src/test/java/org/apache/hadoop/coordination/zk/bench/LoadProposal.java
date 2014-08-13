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

import org.apache.hadoop.coordination.Agreement;

/**
 * The main part of the load test, this is a value object issued by the
 * {@link LoadTool.LoadThread} and received by the {@link LoadTool.LoadGenerator}.
 * When applied to the {@link LoadTool.LoadGenerator} it returns a {@link Long}.
 */
@Immutable
public class LoadProposal implements Agreement<LoadLearner, Long> {

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

  public LoadProposal(Long clientId, Long value, Long iteration) {
    this.value = value;
    this.clientId = clientId;
    this.iteration = iteration;
    this.creationTime = System.currentTimeMillis();
  }

  private void writeObject(ObjectOutputStream out)
          throws IOException {
    out.writeLong(value);
    out.writeLong(clientId);
    out.writeLong(iteration);
    out.writeLong(creationTime);
  }

  private void readObject(ObjectInputStream in)
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
  public Long execute(String proposeIdentity, String ceIdentity, LoadLearner loadLearner)
          throws IOException {
    return loadLearner.handleProposal(ceIdentity, this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    LoadProposal that = (LoadProposal) o;

    if (!clientId.equals(that.clientId)) return false;
    if (!creationTime.equals(that.creationTime)) return false;
    if (!iteration.equals(that.iteration)) return false;
    if (!value.equals(that.value)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = clientId.hashCode();
    result = 31 * result + value.hashCode();
    result = 31 * result + iteration.hashCode();
    result = 31 * result + creationTime.hashCode();
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
