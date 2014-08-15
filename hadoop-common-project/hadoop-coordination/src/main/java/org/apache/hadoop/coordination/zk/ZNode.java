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

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.zookeeper.data.Stat;

/**
 * Class represent znode data with associated Stat object.
 * Semantics is the same as method getData provides, i.e.
 * data can be null
 */
@InterfaceAudience.Private
@Immutable
public abstract class ZNode {

  private final String path;

  protected ZNode(String path) {
    this.path = path;
  }

  /**
   * Path for given znode
   *
   * @return path
   */
  public String getPath() {
    return path;
  }

  /**
   * Stat for given node
   *
   * @return Stat
   */
  public abstract Stat getStat();

  /**
   * Data for given node
   *
   * @return data
   */
  public abstract byte[] getData();

  /**
   * @return true if node exists
   */
  public abstract boolean isExists();

  @Immutable
  @InterfaceAudience.Private
  public static class None extends ZNode {

    public None(String path) {
      super(path);
    }

    @Override
    public Stat getStat() {
      throw new IllegalStateException("Node not exists: " + getPath());
    }

    @Nullable
    @Override
    public byte[] getData() {
      throw new IllegalStateException("Node not exists: " + getPath());
    }

    @Override
    public boolean isExists() {
      return false;
    }
  }

  @Immutable
  @InterfaceAudience.Private
  public static class Data extends ZNode {

    private final Stat stat;
    private final byte[] data;

    public Data(String path, byte[] data, Stat stat) {
      super(path);
      this.stat = stat;
      this.data = data;
    }

    @Override
    public Stat getStat() {
      return stat;
    }

    @Nullable
    @Override
    public byte[] getData() {
      return data;
    }

    @Override
    public boolean isExists() {
      return true;
    }
  }

  @Immutable
  @InterfaceAudience.Private
  public static class Exists extends ZNode {

    private final Stat stat;

    public Exists(String path, Stat stat) {
      super(path);
      this.stat = stat;
    }

    @Override
    public Stat getStat() {
      return stat;
    }

    @Nullable
    @Override
    public byte[] getData() {
      throw new IllegalStateException("'exists' calls doesn't return any data for znodes");
    }

    @Override
    public boolean isExists() {
      return true;
    }
  }

}
