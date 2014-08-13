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
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.hadoop.coordination.Agreement;
import org.apache.hadoop.util.Time;

/**
 * A simple ConsensusProposal that is applied to the {@link SampleLearner} and,
 * when applied, results in an {@link Integer} being returned from the learner.
 */
@Immutable
class SampleProposal implements Agreement<SampleLearner, Integer> {

  private static final long serialVersionUID = -6229936612835969269L;

  private long receiveTime;
  private String user;

  public SampleProposal(final String user) {
    this.receiveTime = Time.now();
    this.user = user;
  }

  public SampleProposal() {
    this(null);
  }

  public long getReceiveTime() {
    return receiveTime;
  }

  public String getUser() {
    return user;
  }

  @Override // Agreement
  public Integer execute(final String proposeIdentity,
                         final String ceIdentity,
                         final SampleLearner learner)
      throws IOException {
    return learner.updateState(receiveTime);
  }

  @Override // Object
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof SampleProposal)) return false;

    SampleProposal proposal = (SampleProposal) o;

    if (receiveTime != proposal.receiveTime) return false;
    if (user != null ? !user.equals(proposal.user) : proposal.user != null)
      return false;

    return true;
  }

  @Override // Object
  public int hashCode() {
    int result = (int) (receiveTime ^ (receiveTime >>> 32));
    result = 31 * result + (user != null ? user.hashCode() : 0);
    return result;
  }

  @Override // Object
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("SampleProposal. receiveTime: ");
    sb.append(receiveTime);
    if (user != null) {
      sb.append(", user: ");
      sb.append(user);
    }
    return sb.toString();
  }

  /**
   * The writeObject method is responsible for writing the state of the object
   * for its particular class so that the corresponding readObject method can
   * restore it.
   */
  private void writeObject(final ObjectOutputStream out)
      throws IOException {
    out.writeLong(receiveTime);
    if (user != null) {
      out.writeBoolean(true);
      out.writeUTF(user);
    } else {
      out.writeBoolean(false);
    }
  }

  /**
   * The readObject method is responsible for reading from the stream and
   * restoring the classes fields.
   */
  private void readObject(final ObjectInputStream in)
      throws IOException, ClassNotFoundException {
    receiveTime = in.readLong();
    boolean hasUser = in.readBoolean();
    if (hasUser) {
      user = in.readUTF();
    }
  }
}
