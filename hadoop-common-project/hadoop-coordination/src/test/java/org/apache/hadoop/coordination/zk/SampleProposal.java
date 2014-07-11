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
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import java.security.PrivilegedAction;

import org.apache.hadoop.coordination.ConsensusProposal;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;

/**
 * A simple ConsensusProposal.
 */
class SampleProposal extends ConsensusProposal<SampleLearner, Integer> {
  private static final long serialVersionUID = 1L;

  private long receiveTime;
  private String user;

  public SampleProposal(final Serializable proposerNodeId) {
    super(proposerNodeId);
    this.receiveTime = Time.now();
  }

  public long getReceiveTime() {
    return receiveTime;
  }

  public void setUser(String user) throws IOException {
    this.user = user;
  }

  public String getUser() {
    return user;
  }

  @Override // Object
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof SampleProposal)) return false;
    if (!super.equals(o)) return false;

    SampleProposal that = (SampleProposal) o;
    if (receiveTime != that.receiveTime) return false;

    return true;
  }

  @Override // Object
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (int) (receiveTime ^ (receiveTime >>> 32));
    return result;
  }

  @Override // ConsensusProposal
  public Integer execute(final SampleLearner learner) throws IOException {
    UserGroupInformation proxyUser =
        UserGroupInformation.createProxyUser(getUser(),
            UserGroupInformation.getCurrentUser());
    return proxyUser.doAs(new PrivilegedAction<Integer>() {
      @Override
      public Integer run() {
        learner.updateState(getReceiveTime());
        return learner.getState();
      }
    });
  }

  /**
   * Custom de-serialization.
   */
  private void readObject(ObjectInputStream in)
      throws IOException, ClassNotFoundException {
    receiveTime = in.readLong();
    user = in.readUTF();
  }

  /**
   * Custom serialization.
   */
  private void writeObject(ObjectOutputStream out) throws IOException {
    out.writeLong(receiveTime);
    out.writeUTF(user);
  }

  @Override // ConsensusProposal
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(super.toString());
    sb.append(", receiveTime: ");
    sb.append(receiveTime);
    sb.append(", user: ");
    sb.append(user);
    return sb.toString();
  }
}
