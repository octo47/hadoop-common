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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QueueNameTest {

  @Test
  public void testNaming() {
    assertEquals("root.a.b", QueueName.makeRootQualified("a.b"));
    assertEquals("root.a.b", QueueName.makeRootQualified("root.a.b"));
    assertEquals("", QueueName.makeRootQualified(""));

    assertTrue(QueueName.isRoot("root"));
    assertTrue(QueueName.isDefault("default"));
  }

  @Test
  public void testHierarcy() {
    final String q = "root.a.b";
    assertEquals(
            Lists.newArrayList("root.a.b", "root.a", "root"),
            Lists.newArrayList(QueueName.pathToRoot(q)));
  }
}
