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

import org.apache.hadoop.coordination.Agreement;
import org.apache.hadoop.coordination.Proposal;

import javax.annotation.concurrent.Immutable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamException;
import java.io.Serializable;

/**
 * A value object used for {@link LoadTool.LoadThread} registration. When
 * applied to the {@link LoadTool.LoadThread} it returns {@link Void}.
 */
@Immutable
public class RegisterProposal implements Proposal, Agreement<LoadLearner, Long>, Serializable {

  private static final long serialVersionUID = 1L;

  private String proposerNodeId;
  private int requestId;

  public int getRequestId() {
    return requestId;
  }

  public RegisterProposal(String proposerNodeId, int requestId) {
    this.proposerNodeId = proposerNodeId;
    this.requestId = requestId;
  }

  /**
   * The writeObject method is responsible for writing the state of the object
   * for its particular class so that the corresponding readObject method can
   * restore it
   */
  private void writeObject(final ObjectOutputStream oos)
      throws IOException {
    DataOutputStream out = new DataOutputStream(oos);
    out.writeInt(requestId);
    out.writeUTF(proposerNodeId);
    out.close();
  }

  /**
   * The readObject method is responsible for reading from the stream and
   * restoring the classes fields.
   */
  private void readObject(final ObjectInputStream ois )
      throws IOException, ClassNotFoundException {
    DataInputStream in = new DataInputStream(ois);
    requestId = in.readInt();
    proposerNodeId = in.readUTF();
    in.close();
  }

  private void readObjectNoData()
          throws ObjectStreamException {
    throw new IllegalArgumentException("Can't handle empty object");
  }

  @Override
  public long getGlobalSequenceNumber() {
    throw new UnsupportedOperationException("Don't know how to get GSN");
  }

  @Override
  public Long execute(LoadLearner callBackObject) throws IOException {
    return callBackObject.handleRegister(proposerNodeId, this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    RegisterProposal that = (RegisterProposal) o;

    if (requestId != that.requestId) return false;
    if (proposerNodeId != null ? !proposerNodeId.equals(that.proposerNodeId) : that.proposerNodeId != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = proposerNodeId != null ? proposerNodeId.hashCode() : 0;
    result = 31 * result + requestId;
    return result;
  }

  @Override
  public String toString() {
    return "RegisterProposal{" +
            "proposerNodeId=" + proposerNodeId +
            ", requestId=" + requestId +
            '}';
  }
}
