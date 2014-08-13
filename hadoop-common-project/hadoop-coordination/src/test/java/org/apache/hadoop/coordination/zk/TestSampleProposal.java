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

import org.apache.hadoop.coordination.TestProposal;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;

public class TestSampleProposal extends TestProposal {

  private final String user = "user";

  @Test
  public void testInstantiation() {

    SampleProposal p1 = new SampleProposal();

    assertThat(p1.getReceiveTime(), is(notNullValue()));
    assertThat(p1.getUser(), is(nullValue()));

    SampleProposal p2 = new SampleProposal(user);
    assertThat(p2.getUser(), is(user));
  }

  @Test()
  public void testSerializationDeserialization()
      throws IOException, ClassNotFoundException {

    SampleProposal p1 = new SampleProposal(user);
    byte[] ba = serialize(p1);

    SampleProposal p2 = (SampleProposal) deserialize(ba);

    assertThat(p2, is(equalTo(p1)));
  }
}
