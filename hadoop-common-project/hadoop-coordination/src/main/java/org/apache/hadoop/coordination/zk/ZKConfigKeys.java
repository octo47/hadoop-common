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

/**
 * Contains constants needed for ZK-based implementation of
 * {@link org.apache.hadoop.coordination.CoordinationEngine}.
 */
public class ZKConfigKeys {

  /**
   * Key for ZooKeeper quorum string.
   */
  public static final String CE_ZK_QUORUM_KEY = "ce.zk.quorum";

  /**
   * Default for ZooKeeper quorum string.
   */
  public static final String CE_ZK_QUORUM_DEFAULT = "localhost:3000";

  /**
   * Key for ZK session timeout.
   */
  public static final String CE_ZK_SESSION_TIMEOUT_KEY =
    "ce.zk.session.timeout";

  /**
   * Default for ZK session timeout.
   */
  public static final int CE_ZK_SESSION_TIMEOUT_DEFAULT = 3000;

  /**
   * Unique ID of the node for coordination engine. No default value.
   */
  public static final String CE_ZK_NODE_ID_KEY = "ce.zk.node.id";

  /**
   * Default value for prefix of znodes which keep all agreements made.
   */
  public static final String CE_ZK_AGREEMENT_ZNODE_PATH_DEFAULT =
      "/agreement-";

  /**
   * Default value for znode under which {@link ZKCoordinationEngine}
   * writes its data.
   */
  public static final String CE_ZK_ENGINE_ZNODE_DEFAULT =
    "/coordination_engine";

  /**
   * Default value for suffix of znodes which keep GSN (global
   * sequence number) for each cluster node.
   */
  public static final String CE_ZK_GSN_ZNODE_SUFFIX_DEFAULT = "_gsn";
}
