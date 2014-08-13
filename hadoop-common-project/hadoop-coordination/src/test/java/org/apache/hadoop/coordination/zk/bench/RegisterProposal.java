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

import javax.annotation.concurrent.Immutable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * A value object used for {@link LoadTool.LoadThread} registration. When
 * applied to the {@link LoadTool.LoadThread} it returns {@link Void}.
 */
@Immutable
public class RegisterProposal implements Agreement<LoadLearner, Void> {

  private static final long serialVersionUID = 1L;

  private int requestId;

  public int getRequestId() {
    return requestId;
  }

  public RegisterProposal(final int requestId) {
    this.requestId = requestId;
  }

  @Override
  public Void execute(String proposeIdentity, String ceIdentity, LoadLearner learner)
          throws IOException {
    learner.handleRegister(ceIdentity, this);
    return null;
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
    in.close();
  }
}
