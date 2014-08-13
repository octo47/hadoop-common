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
package org.apache.hadoop.coordination;

import javax.annotation.concurrent.Immutable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.UUID;

/**
 * ConsensusProposal is an implementation of a value object accepted by the
 * {@link CoordinationEngine}. It is immutable once created.
 */
@Immutable
public class ConsensusProposal implements Proposal {

  private static final long serialVersionUID = -7792555096613056598L;

  /** The unique identity of the proposal */
  private String proposalIdentity;

  /** The identity of CE instance making the proposal */
  private String ceIdentity;

  /** The serialized value being proposed */
  private byte[] value;

  /**
   * @param ceIdentity  The identity of the coordinationEngine instance making the
   *                    proposal.
   * @param value       The value to be agreed.
   */
  public ConsensusProposal(final String ceIdentity,
                           final byte[] value) {
    this.proposalIdentity = UUID.randomUUID().toString();
    this.ceIdentity = ceIdentity;
    this.value = value;
  }

  /**
   * @return the unique identity of the proposal.
   */
  @Override // Proposal
  public String getProposalIdentity() {
    return proposalIdentity;
  }

  /**
   * @return the identity of the node making the proposal.
   */
  @Override // Proposal
  public String getCeIdentity() {
    return ceIdentity;
  }

  /**
   * @return the value being proposed.
   */
  @Override // Proposal
  public byte[] getValue() {
    return value;
  }

  /**
   * Implementation of equals for comparison of ConsensusProposals.
   */
  @Override // Object
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ConsensusProposal)) return false;

    ConsensusProposal proposal = (ConsensusProposal) o;

    if (!proposalIdentity.equals(proposal.proposalIdentity)) return false;
    if (!ceIdentity.equals(proposal.ceIdentity)) return false;
    if (!Arrays.equals(value, proposal.value)) return false;

    return true;
  }

  /**
   * @return a unique hash code for this proposal
   */
  @Override // Object
  public int hashCode() {
    int result = proposalIdentity.hashCode();
    result = 31 * result + ceIdentity.hashCode();
    result = 31 * result + Arrays.hashCode(value);
    return result;
  }

  /**
   * @return a String representation of this proposal.
   */
  @Override // Object
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("ConsensusProposal. proposalIdentity: ");
    sb.append(proposalIdentity);
    sb.append(", ceIdentity");
    sb.append(ceIdentity);
    sb.append(", size of value: ");
    sb.append(value.length);
    sb.append(" bytes");
    return sb.toString();
  }

  /**
   * The writeObject method is responsible for writing the state of the object
   * so the corresponding readObject method can restore it.
   */
  private void writeObject(final ObjectOutputStream oos)
      throws IOException {
    DataOutputStream out = new DataOutputStream(oos);
    out.writeUTF(proposalIdentity);
    out.writeUTF(ceIdentity);
    out.writeInt(value.length);
    out.write(value);
    out.close();
  }

  /**
   * The readObject method is responsible for reading from the stream and
   * restoring the classes fields.
   */
  private void readObject(final ObjectInputStream ois )
      throws IOException, ClassNotFoundException {
    DataInputStream in = new DataInputStream(ois);
    proposalIdentity = in.readUTF();
    ceIdentity = in.readUTF();
    int valueLen = in.readInt();
    value = new byte[valueLen];
    in.read(value);
    in.close();
  }
}
