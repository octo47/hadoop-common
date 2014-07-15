package org.apache.hadoop.coordination.zk;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.coordination.NoQuorumException;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.client.ConnectStringParser;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;

/**
 * Recoverable async zookeeper wrapper
 * <p/>
 * TODO: refactor out retry logic to some RetryLogic class
 */
@InterfaceAudience.Private
public class ZkConnection implements Closeable, Watcher {

  /**
   * Class for abstracting ZooKeeper creation code.
   */
  public interface ZkFactory {
    String getQuorumString();

    int getSessionTimeout();

    ZooKeeper create(Watcher defaultWatcher)
            throws IOException;
  }

  public interface RetryPolicy {
    /**
     * Retry operation logic
     * @param op to retry
     * @return false if no retries should be done
     */
    boolean retryOperation(ZkAsyncOperation<?> op);
  }

  public static final Log LOG = LogFactory.getLog(ZkConnection.class);
  public static final int RETRY_SLEEP_MILLIS = 500;

  private final String chrootPath;

  private volatile ZooKeeper zk;

  // used in async retries
  private final HashedWheelTimer timer = new HashedWheelTimer();

  // TODO: replace with path specific listeners
  private final Set<Watcher> watchers = new LinkedHashSet<Watcher>();

  private final ZkFactory zkFactory;
  private final RetryPolicy timerPolicy = new RetryPolicy() {
    @Override
    public boolean retryOperation(final ZkAsyncOperation<?> op) {
      timer.newTimeout(new TimerTask() {
        @Override
        public void run(Timeout timeout) throws Exception {
          try {
            op.submitAsyncOperation();
          } catch (Exception e) {
            op.setException(e);
          }
        }
      }, RETRY_SLEEP_MILLIS, TimeUnit.MILLISECONDS);
      return true;
    }
  };

  public int getSessionTimeout() {
    return zkFactory.getSessionTimeout();
  }

  public ZkConnection(final String quorumString, final int sessionTimeout)
          throws IOException {
    this(new ZkFactory() {
      @Override
      public String getQuorumString() {
        return quorumString;
      }

      @Override
      public int getSessionTimeout() {
        return sessionTimeout;
      }

      @Override
      public ZooKeeper create(Watcher defaultWatcher) throws IOException {
        return new ZooKeeper(quorumString, sessionTimeout, defaultWatcher);
      }
    });
  }

  public ZkConnection(ZkFactory zkFactory)
          throws IOException {
    this.zkFactory = zkFactory;
    // assume we have rights for read in chroot
    String parsedChroot = new ConnectStringParser(zkFactory.getQuorumString()).getChrootPath();
    if (parsedChroot == null)
      parsedChroot = "/";
    this.chrootPath = parsedChroot;
    connect();
  }

  public synchronized boolean isConnected() {
    return zk != null && zk.getState().isConnected();
  }

  public synchronized boolean isAlive() {
    return zk != null && zk.getState().isAlive();
  }

  public synchronized long getSessionId() {
    if (zk != null)
      return zk.getSessionId();
    return -1;
  }

  public synchronized void addWatcher(Watcher watcher) {
    watchers.add(watcher);
  }

  public synchronized void removeWatcher(Watcher watcher) {
    watchers.remove(watcher);
  }

  public synchronized void close() {
    if (zk != null) {
      LOG.error("Closing zk session 0x" + Long.toHexString(zk.getSessionId()));
      try {
        zk.close();
      } catch (InterruptedException e) {
        LOG.error("Interrupted to close zk session 0x" + Long.toHexString(zk.getSessionId()));
      }
    }
    zk = null;
  }

  public ZooKeeper getZk() {
    return zk;
  }

  private void connect() throws IOException {
    if (isConnected())
      return;
    LOG.info("Connecting to zk ensemble: " + zkFactory.getQuorumString());

    final SettableFuture<Boolean> connected = SettableFuture.create();
    try {
      this.zk = zkFactory.create(new Watcher() {
        @Override
        public void process(WatchedEvent event) {
          switch (event.getState()) {
            case SyncConnected:
              // set global watcher
              zk.register(ZkConnection.this);
              connected.set(true);
              break;
            case Expired:
              connected.setException(
                      new NoQuorumException("Failed to establish connection to " +
                              zkFactory.getQuorumString()));
          }
          ZkConnection.this.process(event);
        }
      });
      connected.get(getSessionTimeout(), TimeUnit.MILLISECONDS);
      // do sync check
      final Stat exists = zk.exists(chrootPath, false);
      if (exists == null)
        throw new NoQuorumException("Root " + chrootPath + " path not exists");
    } catch (Exception e) {
      close();
      throw new NoQuorumException("Failed to initialize zk quorum", e);
    }
    LOG.error("Created zk session 0x" + Long.toHexString(zk.getSessionId()));
  }

  /**
   * Exists will retried on connection loss.
   *
   * @param path path to check
   * @return Stat object or null, null means path not exists
   * @throws IOException
   * @throws KeeperException
   */
  public ZNode exists(String path) throws IOException, KeeperException, InterruptedException {
    return waitForFeature(existsAsync(path, false));
  }

  public ZNode exists(String path, boolean watch) throws IOException, KeeperException, InterruptedException {
    return waitForFeature(existsAsync(path, watch));
  }


  /**
   * Async version of exists()
   *
   * @param path  path to check
   * @param watch if true, subscribe for changes
   * @return Future
   * @see #exists(String)
   */
  public ListenableFuture<ZNode> existsAsync(String path, boolean watch) {
    final ExistsOp op = new ExistsOp(path, watch);
    op.submitAsyncOperation();
    return op;
  }


  /**
   * Get data will retried on connection loss.
   *
   * @param path path to get data for
   * @return ZNode object
   * @throws IOException
   * @throws KeeperException
   * @see org.apache.hadoop.coordination.zk.ZNode
   */
  public ZNode getData(String path) throws IOException, KeeperException, InterruptedException {
    return waitForFeature(getDataAsync(path, false));
  }

  public ZNode getData(String path, boolean watch) throws IOException, KeeperException, InterruptedException {
    return waitForFeature(getDataAsync(path, watch));
  }

  /**
   * Async version of getChildren()
   *
   * @param path  to get children for
   * @param watch if true, subscribe for changes
   * @return Future with ZNode
   * @see #addWatcher(String, org.apache.zookeeper.Watcher)
   */
  public ListenableFuture<List<String>> getChildrenAsync(String path, boolean watch) {
    final GetChildrenOp op = new GetChildrenOp(path, watch);
    op.submitAsyncOperation();
    return op;
  }

  /**
   * Get children will retry on connection loss.
   *
   * @param path path to get children for
   * @return ZNode object
   * @throws IOException
   * @throws KeeperException
   * @see org.apache.hadoop.coordination.zk.ZNode
   */
  public List<String> getChildren(String path) throws IOException, KeeperException, InterruptedException {
    return waitForFeature(getChildrenAsync(path, false));
  }

  public List<String> getChildren(String path, boolean watch) throws IOException, KeeperException, InterruptedException {
    return waitForFeature(getChildrenAsync(path, watch));
  }

  /**
   * Async version of getData()
   *
   * @param path  to get data for
   * @param watch if true, subscribe for changes
   * @return Future with ZNode
   * @see #getData(String)
   * @see #addWatcher(String, org.apache.zookeeper.Watcher)
   */
  public ListenableFuture<ZNode> getDataAsync(String path, boolean watch) {
    final GetDataOp op = new GetDataOp(path, watch);
    op.submitAsyncOperation();
    return op;
  }

  /**
   * Create node with specified parameters.
   * If mode is nonsequential, will retry operation (operation considered indempotent)
   *
   * @param path  path to create (or prefix, in case of sequential)
   * @param bytes payload
   * @param acl   acl assign to znode
   * @param mode  create mode
   * @return resulting path
   * @throws IOException
   * @throws KeeperException
   */
  public String create(String path, byte[] bytes, ArrayList<ACL> acl, CreateMode mode)
          throws IOException, KeeperException, InterruptedException {
    return waitForFeature(createAsync(path, bytes, acl, mode));
  }

  /**
   * Create node with specified parameters.
   * If mode is nonsequential, will retry operation (operation considered indempotent)
   *
   * @param path             path to create (or prefix, in case of sequential)
   * @param bytes            payload
   * @param acl              acl assign to znode
   * @param mode             create mode
   * @param ignoreNodeExists don't fail if node already exists
   * @return resulting path
   * @throws IOException
   * @throws KeeperException
   */
  public String create(String path, byte[] bytes, ArrayList<ACL> acl, CreateMode mode, boolean ignoreNodeExists)
          throws IOException, KeeperException, InterruptedException {
    return waitForFeature(createAsync(path, bytes, acl, mode, ignoreNodeExists));
  }

  /**
   * Asynchronous version of create().
   *
   * @param path  path to create (or prefix, in case of sequential)
   * @param bytes payload
   * @param acl   acl assign to znode
   * @param mode  create mode
   * @return Future for resulting path
   */
  public ListenableFuture<String> createAsync(String path, byte[] bytes, ArrayList<ACL> acl, CreateMode mode) {
    final CreateOp op = new CreateOp(path, bytes, acl, mode, false);
    op.submitAsyncOperation();
    return op;
  }

  public ListenableFuture<String> createAsync(String path, byte[] bytes, ArrayList<ACL> acl, CreateMode mode,
                                              boolean ignoreExists) {
    final CreateOp op = new CreateOp(path, bytes, acl, mode, ignoreExists);
    op.submitAsyncOperation();
    return op;
  }

  public ZNode.Exists setData(String path, byte[] bytes, int version) throws IOException, KeeperException, InterruptedException {
    return waitForFeature(setDataAsync(path, bytes, version));
  }

  public ListenableFuture<ZNode.Exists> setDataAsync(String path, byte[] bytes, int version) {
    final SetDataOp op = new SetDataOp(path, bytes, version);
    op.submitAsyncOperation();
    return op;
  }

  public void delete(String path) throws IOException, KeeperException, InterruptedException {
    waitForFeature(deleteAsync(path, -1));
  }

  public void delete(String path, int version) throws IOException, KeeperException, InterruptedException {
    waitForFeature(deleteAsync(path, version));
  }

  public ListenableFuture<Void> deleteAsync(String path, int version) {
    final DeleteOp op = new DeleteOp(path, version);
    op.submitAsyncOperation();
    return op;
  }

  @Override
  public void process(WatchedEvent event) {
    for (Watcher watcher : watchers) {
      try {
        watcher.process(event);
      } catch (Exception e) {
        LOG.error("Failed to process event", e);
      }
    }
    switch (event.getState()) {
      case Disconnected:
        LOG.info("Disconnected session 0x" + Long.toHexString(zk.getSessionId()));
        break;
      case SyncConnected:
        LOG.info("Reconnected session 0x" + Long.toHexString(zk.getSessionId()));
        break;
      case Expired:
        LOG.info("Lost session 0x" + Long.toHexString(zk.getSessionId()));
        close();
      default:
        // FALL THROUGH
    }

  }

  private <R> R waitForFeature(ListenableFuture<R> op) throws IOException, KeeperException, InterruptedException {
    try {
      return op.get(getSessionTimeout(), TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      final Throwable cause = e.getCause();
      Throwables.propagateIfPossible(cause, KeeperException.class);
      Throwables.propagateIfPossible(cause, NoQuorumException.class);
      throw Throwables.propagate(cause);
    } catch (TimeoutException e) {
      throw new NoQuorumException("Operation timed out", e);
    }
  }

  /**
   * Base class for zk async operations.
   * Used for generality in error handling and reconnection.
   *
   * @param <R> return type
   */
  public abstract class ZkAsyncOperation<V> extends AbstractFuture<V> {

    private RetryPolicy policy;

    protected ZkAsyncOperation() {
      this.policy = getRetryPolicy();
    }

    /**
     * Return description of current operation.
     * Typically called for meaningful exceptions.
     *
     * @return description
     */
    public abstract String getDescription();

    public void setPolicy(RetryPolicy policy) {
      this.policy = policy;
    }

    public RetryPolicy getPolicy() {
      return policy;
    }

    protected boolean isIdempotent() {
      return true;
    }

    /**
     * Submit async operation
     */
    protected abstract void submitAsyncOperation();

    @Override
    protected boolean setException(@Nullable Throwable cause) {
      if (cause == null)
        return super.setException(new IllegalArgumentException("Null throwable passed"));

      final boolean rc;
      if (cause instanceof KeeperException) {
        switch (((KeeperException) cause).code()) {
          case CONNECTIONLOSS:
          case OPERATIONTIMEOUT:
            LOG.info("Lost connection to zk quorum performing " + getDescription() +
                    ", will retry");
            if (!isIdempotent()) // don't retry for non-idempotent operations
              rc = super.setException(cause);
            else {
              rc = !getPolicy().retryOperation(this)
                      && super.setException(
                      new IOException("Can't retry" + getDescription(), cause));
            }
            break;
          case SESSIONEXPIRED:
            LOG.error("Failed zk operation " + getDescription(), cause);
            rc = super.setException(new NoQuorumException("Session expired"));
            break;
          default:
            LOG.error("Failed zk operation " + getDescription(), cause);
            rc = super.setException(cause);
        }
      } else {
        LOG.error("Failed zk operation " + getDescription(), cause);
        rc = super.setException(cause);
      }
      return rc;
    }
  }

  private RetryPolicy getRetryPolicy() {
    return timerPolicy;
  }

  /**
   * Future for exists() operation
   */
  public class ExistsOp extends ZkAsyncOperation<ZNode> implements AsyncCallback.StatCallback {

    @Override
    public void processResult(int rc, String path, Object ctx, Stat stat) {
      final KeeperException.Code code = KeeperException.Code.get(rc);
      if (isSuccess(code)) {
        set(new ZNode.Exists(path, stat));
      } else if (isNodeDoesNotExist(code)) {
        set(new ZNode.None(path));
      } else {
        setException(KeeperException.create(code));
      }
    }

    private final String path;
    private final boolean watch;

    public ExistsOp(String path, boolean watch) {
      this.path = path;
      this.watch = watch;
    }

    @Override
    public void submitAsyncOperation() {
      zk.exists(path, watch ? ZkConnection.this : null, this, this);
    }

    @Override
    public String getDescription() {
      return "exists(" + path + ")";
    }
  }

  /**
   * Future for create() operation
   */
  public class CreateOp extends ZkAsyncOperation<String> implements AsyncCallback.StringCallback {

    private final String path;
    private final byte[] bytes;
    private final List<ACL> acl;
    private final CreateMode mode;
    private final boolean ignoreExists;

    @Override
    public void processResult(int rc, String path, Object ctx, String name) {
      final KeeperException.Code code = KeeperException.Code.get(rc);
      if (isSuccess(code)) {
        set(name);
      } else if (ignoreExists && isNodeExists(code)) {
        set(name);
      } else {
        setException(KeeperException.create(code));
      }
    }

    public CreateOp(String path, byte[] bytes, List<ACL> acl, CreateMode mode, boolean ignoreExists) {
      this.path = path;
      this.bytes = bytes;
      this.mode = mode;
      this.ignoreExists = ignoreExists;
      this.acl = Lists.newArrayList(acl);
    }

    @Override
    protected boolean isIdempotent() {
      return !mode.isSequential();
    }

    @Override
    public void submitAsyncOperation() {
      zk.create(path, bytes, acl, mode, this, this);
    }

    @Override
    public String getDescription() {
      return "create(" + path + ", " + mode + ")";
    }
  }

  /**
   * Future for setData() operation
   */
  public class SetDataOp extends ZkAsyncOperation<ZNode.Exists> implements AsyncCallback.StatCallback {

    @Override
    public void processResult(int rc, String path, Object ctx, Stat stat) {
      final KeeperException.Code code = KeeperException.Code.get(rc);
      if (isSuccess(code)) {
        set(new ZNode.Exists(path, stat));
      } else {
        setException(KeeperException.create(code));
      }
    }

    private final String path;
    private final byte[] data;
    private final int version;

    public SetDataOp(String path, byte[] data, int version) {
      this.path = path;
      this.data = data;
      this.version = version;
    }

    @Override
    protected boolean isIdempotent() {
      return version != -1;
    }

    @Override
    public void submitAsyncOperation() {
      zk.setData(path, data, version, this, this);
    }

    @Override
    public String getDescription() {
      return "setData(" + path + ", " + version + ")";
    }
  }

  /**
   * Future for getData() operation
   */
  public class GetDataOp extends ZkAsyncOperation<ZNode> implements AsyncCallback.DataCallback {

    @Override
    public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
      final KeeperException.Code code = KeeperException.Code.get(rc);
      if (isSuccess(code)) {
        set(new ZNode.Data(path, data, stat));
      } else if (isNodeDoesNotExist(code)) {
        set(new ZNode.None(path));
      } else {
        setException(KeeperException.create(code));
      }
    }

    private final String path;
    private final boolean watch;

    public GetDataOp(String path, boolean watch) {
      this.path = path;
      this.watch = watch;
    }

    @Override
    public void submitAsyncOperation() {
      zk.getData(path, watch ? ZkConnection.this : null, this, this);
    }

    @Override
    public String getDescription() {
      return "getData(" + path + ")";
    }
  }

  /**
   * Future for getChildren() operation
   */
  public class GetChildrenOp extends ZkAsyncOperation<List<String>> implements AsyncCallback.ChildrenCallback {

    @Override
    public void processResult(int rc, String path, Object ctx, List<String> children) {
      final KeeperException.Code code = KeeperException.Code.get(rc);
      if (isSuccess(code)) {
        set(children);
      } else {
        setException(KeeperException.create(code));
      }
    }

    private final String path;
    private final boolean watch;

    public GetChildrenOp(String path, boolean watch) {
      this.path = path;
      this.watch = watch;
    }

    @Override
    public void submitAsyncOperation() {
      zk.getChildren(path, watch ? ZkConnection.this : null, this, this);
    }

    @Override
    public String getDescription() {
      return "getChildren(" + path + ")";
    }

  }

  /**
   * Future for setData() operation
   */
  public class DeleteOp extends ZkAsyncOperation<Void> implements AsyncCallback.VoidCallback {

    @Override
    public void processResult(int rc, String path, Object ctx) {
      final KeeperException.Code code = KeeperException.Code.get(rc);
      if (isSuccess(code) || (ignoreNonExists && isNodeDoesNotExist(code))) {
        set(null);
      } else {
        setException(KeeperException.create(code));
      }
    }

    private final String path;
    private final int version;
    private final boolean ignoreNonExists;

    public DeleteOp(String path, int version, boolean ignoreNonExists) {
      this.path = path;
      this.version = version;
      this.ignoreNonExists = ignoreNonExists;
    }

    public DeleteOp(String path, int version) {
      this(path, version, true);
    }

    @Override
    public void submitAsyncOperation() {
      zk.delete(path, version, this, this);
    }

    @Override
    public String getDescription() {
      return "delete(" + path + ", " + version + ")";
    }
  }


  private static boolean isSuccess(KeeperException.Code code) {
    return (code == KeeperException.Code.OK);
  }

  private static boolean isNodeExists(KeeperException.Code code) {
    return (code == KeeperException.Code.NODEEXISTS);
  }

  private static boolean isNodeDoesNotExist(KeeperException.Code code) {
    return (code == KeeperException.Code.NONODE);
  }

}
