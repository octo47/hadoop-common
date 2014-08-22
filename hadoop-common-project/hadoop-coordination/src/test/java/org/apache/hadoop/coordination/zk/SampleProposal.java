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
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.UUID;

import org.apache.hadoop.coordination.Agreement;
import org.apache.hadoop.coordination.ConsensusProposal;
import org.apache.hadoop.coordination.Proposal;
import org.apache.hadoop.util.Time;

/**
 * A simple ConsensusProposal that is applied to the {@link SampleLearner} and,
 * when applied, results in an {@link Integer} being returned from the learner.
 */
@Immutable
class SampleProposal implements
        Proposal, Agreement<SampleLearner, Integer>, Serializable {

  private static final long serialVersionUID = -6229936612835969269L;

  private String id;
  private long receiveTime;
  private String user;

  public SampleProposal() {
    this(null);
  }

  public SampleProposal(final String user) {
    this.id = UUID.randomUUID().toString();
    this.user = user;
    this.receiveTime = Time.now();
  }

  public long getReceiveTime() {
    return receiveTime;
  }

  public String getUser() {
    return user;
  }

  @Override
  public long getGlobalSequenceNumber() {
    throw new UnsupportedOperationException("Not known how to get GSN");
  }

  @Override // Agreement
  public Integer execute(final SampleLearner learner)
      throws IOException {
    return learner.updateState(receiveTime);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    SampleProposal that = (SampleProposal) o;

    if (receiveTime != that.receiveTime) return false;
    if (id != null ? !id.equals(that.id) : that.id != null) return false;
    if (user != null ? !user.equals(that.user) : that.user != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = id != null ? id.hashCode() : 0;
    result = 31 * result + (int) (receiveTime ^ (receiveTime >>> 32));
    result = 31 * result + (user != null ? user.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "SampleProposal{" +
            "id='" + id + '\'' +
            ", receiveTime=" + receiveTime +
            ", user='" + user + '\'' +
            '}';
  }

  /**
   * The writeObject method is responsible for writing the state of the object
   * for its particular class so that the corresponding readObject method can
   * restore it.
   */
  private void writeObject(final ObjectOutputStream out)
      throws IOException {
    out.writeUTF(id);
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
    id = in.readUTF();
    receiveTime = in.readLong();
    boolean hasUser = in.readBoolean();
    if (hasUser) {
      user = in.readUTF();
    }
  }

  private void readObjectNoData()
          throws ObjectStreamException {
    throw new IllegalArgumentException("Can't handle empty object");
  }
}
