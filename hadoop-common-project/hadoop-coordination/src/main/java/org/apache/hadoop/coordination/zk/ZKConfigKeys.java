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

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Contains constants needed for ZK-based implementation of
 * {@link org.apache.hadoop.coordination.CoordinationEngine}.
 */
@InterfaceAudience.Private
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
  public static final int CE_ZK_SESSION_TIMEOUT_DEFAULT = 30000;

  /**
   * Key for ZK batch size.
   */
  public static final String CE_ZK_BATCH_SIZE_KEY =
          "ce.zk.batch.size";
  public static final int CE_ZK_BATCH_SIZE_DEFAULT = 400;

  /**
   * Key for ZK batch GSN commit.
   */
  public static final String CE_ZK_BATCH_COMMIT_KEY =
          "ce.zk.batch.commit";
  public static final boolean CE_ZK_BATCH_COMMIT_DEFAULT = true;

  /**
   * Number for bucket decimal digits in znode sequence, that limits
   * number of allowed agreements per bucket. That is 10^digits.
   * Cannot be changed after CE initialized.
   */
  public static final String CE_ZK_BUCKET_DIGITS_KEY = "ce.zk.bucket.digits";

  /**
   * Defaults to 10k agreements per bucket (4 digits of an agreement seq)
   */
  public static final int CE_ZK_BUCKET_DIGITS_DEFAULT = 4;

  /**
   * Maximum number of buckets, keeping in zookeeper. Background
   * thread will evict old buckets.
   */
  public static final String CE_ZK_MAX_BUCKETS_KEY = "ce.zk.max.buckets";

  /**
   * Default number of buckets to keep in zookeeper.
   */
  public static final int CE_ZK_MAX_BUCKETS_DEFAULT = 20;

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

  /**
   * ZNode for bucket states storage.
   */
  public static final String ZK_BUCKETS_STATE_PATH = "/buckets.state";
}
