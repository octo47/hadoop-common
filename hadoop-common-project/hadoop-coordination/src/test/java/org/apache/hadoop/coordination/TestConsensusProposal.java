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

import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;

public class TestConsensusProposal extends TestProposal {

  private final String ceIdentity = "abc";
  private final byte[] value = new byte[] {97, 98, 99};

  @Test
  public void testInstantiation() {

    ConsensusProposal p = new ConsensusProposal(ceIdentity, value);

    assertThat(p.getProposalIdentity(), is(notNullValue()));
    assertThat(ceIdentity, is(equalTo(p.getCeIdentity())));
    assertThat(value, is(equalTo(p.getValue())));
  }

  @Test()
  public void testSerializationDeserialization()
      throws IOException, ClassNotFoundException {

    ConsensusProposal p1 = new ConsensusProposal(ceIdentity, value);
    byte[] ba = serialize(p1);

    ConsensusProposal p2 = (ConsensusProposal) deserialize(ba);

    assertThat(p2, is(equalTo(p1)));
  }
}
