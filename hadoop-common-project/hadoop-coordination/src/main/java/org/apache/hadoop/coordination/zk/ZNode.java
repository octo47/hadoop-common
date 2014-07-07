package org.apache.hadoop.coordination.zk;

import javax.annotation.Nullable;

import org.apache.zookeeper.data.Stat;

/**
 * Class represent znode data with associated Stat object.
 * Sematic is the same as method getData provides, i.e.
 * data can be null
 */
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

}
