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

import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.util.Iterator;
import java.util.Map;

/**
 * Scheduler hierarchical queue abstraction methods.
 * In general queue name consists of queues path separated by '.'
 * Fully qualified queue name always begins with 'root' queue.
 * Also here is a notion of "undefined" which represented by empty queue name.
 */
public class QueueName {

  public static final String ROOT_QUEUE = "root";
  public static final String DEFAULT_QUEUE = makeRootQualified(YarnConfiguration.DEFAULT_QUEUE_NAME);
  public static final String ROOT_PREFIX = ROOT_QUEUE + ".";
  public static final String UNDEFINED_QUEUE = "";

  private QueueName() {
  }

  /**
   * Ensure, that queue name if 'root' qualifeid (i.e. has root prefix)
   * Undefined queue remains undefined.
   * @param name name to be qualified
   * @return fully qualifed queue name, fqqn :)
   */
  public static String makeRootQualified(String name) {
    if (name.length() == 0 || name.equals(ROOT_QUEUE) || name.startsWith(ROOT_PREFIX))
      return name;
    else
      return ROOT_PREFIX + name;
  }

  /**
   * Get parent for given queue name
   * @param name
   * @return queue name or "" (undefined)
   */
  public static String getParent(String queueName) {
    final String name = makeRootQualified(queueName);
    final int idx = name.lastIndexOf('.');
    if (idx == -1)
      return UNDEFINED_QUEUE;
    else
      return name.substring(0, idx);
  }

  /**
   * Check that given queue is defined.
   * @return true if queue is defined
   */
  public boolean isDefined(String name) {
    return name.length() > 0;
  }

  /**
   * Returns iterable on hierarchy from bottom to top.
   * Example: root.a.b gives [ root.a.b, root.a, root ] sequence
   * @return Iterable
   */
  public static Iterable<String> pathToRoot(final String name) {
    return new Iterable<String>() {
      @Override
      public Iterator<String> iterator() {
        return pathToRootIterator(name);
      }
    };
  }

  /**
   * @see #hierarchy()
   * @return paths iterator
   */
  public static Iterator<String> pathToRootIterator(final String queueName) {
    return new Iterator<String>() {
      private String name = makeRootQualified(queueName);
      private int idx = name.length();
      private String path = name;

      @Override
      public boolean hasNext() {
        return idx != -1;
      }

      @Override
      public String next() {
        String rpath = path;
        idx = path.lastIndexOf('.');
        if (idx != -1)
          path = path.substring(0, idx);
        return rpath;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("immutable iterator");
      }
    };
  }

  public static boolean isRoot(String name) {
    return makeRootQualified(name).equals(ROOT_QUEUE);
  }

  public static boolean isDefault(String name) {
    return makeRootQualified(name).equals(DEFAULT_QUEUE);
  }

  /**
   * Find least common accessor.
   * @param queueName1 first
   * @param queueName2 second
   * @return minimal common parent or any of queues if they are identical
   */
  public static String lca(String queueName1, String queueName2) {
    // Because queue names include ancestors, separated by periods, we can find
    // the lowest common ancestors by going from the start of the names until
    // there's a character that doesn't match.
    final String name1 = makeRootQualified(queueName1);
    final String name2 = makeRootQualified(queueName2);
    // We keep track of the last period we encounter to avoid returning root.apple
    // when the queues are root.applepie and root.appletart
    int lastPeriodIndex = -1;
    for (int i = 0; i < Math.max(name1.length(), name2.length()); i++) {
      if (name1.length() <= i || name2.length() <= i ||
              name1.charAt(i) != name2.charAt(i)) {
        return name1.substring(lastPeriodIndex);
      } else if (name1.charAt(i) == '.') {
        lastPeriodIndex = i;
      }
    }
    return queueName1;
  }

  /**
   * Find in given map value querying in hierarchical order.
   * Hierarchy represented as path-keyed map.
   * @param map map for values, keys are queue names
   * @param queueName queue name lookup for
   * @param defaultValue default value if none found
   * @param <T> type of return value
   * @return value for given queue in hierarchy
   */
  public static <T> T findInHierarchy(Map<String, T> map, String queueName, T defaultValue) {
    final Iterable<String> path = QueueName.pathToRoot(queueName);
    for (String q : path) {
      T found = map.get(q);
      if (found != null)
        return found;
    }
    return defaultValue;
  }

}
