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
  public static final String CE_ZK_QUORUM_DEFAULT = "localhost:3000";

  /**
   * Key for ZooKeeper znode under which {@link ZKCoordinationEngine}
   * writes its data
   */
  public static final String CE_ZK_QUORUM_PATH_KEY = "ce.zk.path";
  public static final String CE_ZK_QUORUM_PATH_DEFAULT = "/ce";

  /**
   * Key for ZK session timeout.
   */
  public static final String CE_ZK_SESSION_TIMEOUT_KEY =
    "ce.zk.session.timeout";
  public static final int CE_ZK_SESSION_TIMEOUT_DEFAULT = 3000;

  /**
   * Key for ZK batch size.
   */
  public static final String CE_ZK_BATCH_SIZE_KEY =
          "ce.zk.batch.size";
  public static final int CE_ZK_BATCH_SIZE_DEFAULT = 100;

  /**
   * Key for ZK batch GSN commit.
   */
  public static final String CE_ZK_BATCH_COMMIT_KEY =
          "ce.zk.batch.commit";
  public static final boolean CE_ZK_BATCH_COMMIT_DEFAULT = false;

  /**
   * Unique ID of the node for coordination engine. No default value.
   */
  public static final String CE_ZK_NODE_ID_KEY = "ce.zk.node.id";

  /**
   * Zookeeper sub znode, containing aggreements.
   */
  public static final String CE_ZK_AGREEMENTS_ZNODE_PATH = "/agreements";

  /**
   * Sequential znodes prefix.
   */
  public static final String CE_ZK_AGREEMENTS_ZNODE_PREFIX_PATH = "/a-";

  /**
   * Default sub znode, containing quorum members state (GSN f.e.)
   */
  public static final String CE_ZK_GSN_ZNODE_PATH = "/gsn";
}
