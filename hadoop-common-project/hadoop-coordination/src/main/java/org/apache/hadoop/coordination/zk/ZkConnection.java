package org.apache.hadoop.coordination.zk;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractFuture;
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

  public static final Log LOG = LogFactory.getLog(ZkConnection.class);
  public static final int RETRY_SLEEP_MILLIS = 500;

  private final String quorumString;
  private final int sessionTimeout;
  private final String chrootPath;

  private volatile ZooKeeper zk;

  // used in async retries
  private final HashedWheelTimer timer = new HashedWheelTimer();

  // TODO: replace with path specific listeners
  private final List<Watcher> watchers = new ArrayList<Watcher>();

  public int getSessionTimeout() {
    return sessionTimeout;
  }

  // helper watcher class used to filter out unnecessary path events
  private static class PathWatcher implements Watcher {

    private final String path;
    private final Watcher delegate;

    private PathWatcher(String path, Watcher delegate) {
      this.path = path;
      this.delegate = delegate;
    }

    @Override
    public void process(WatchedEvent event) {
      if (event.getPath() != null && event.getPath().equals(path))
        delegate.process(event);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      PathWatcher that = (PathWatcher) o;

      if (delegate != null ? !delegate.equals(that.delegate) : that.delegate != null) return false;
      if (path != null ? !path.equals(that.path) : that.path != null) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = path != null ? path.hashCode() : 0;
      result = 31 * result + (delegate != null ? delegate.hashCode() : 0);
      return result;
    }
  }

  public ZkConnection(String quorumString, int sessionTimeout)
          throws IOException {
    this.quorumString = quorumString;
    this.sessionTimeout = sessionTimeout;
    // assume we have rights for read in chroot
    String parsedChroot = new ConnectStringParser(quorumString).getChrootPath();
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

  public synchronized void addWatcher(String path, Watcher watcher) {
    addWatcher(new PathWatcher(path, watcher));
  }

  public synchronized void removeWatcher(Watcher watcher) {
    watchers.remove(watcher);
  }

  public synchronized void removeWatcher(String path, Watcher watcher) {
    removeWatcher(new PathWatcher(path, watcher));
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

  @VisibleForTesting
  ZooKeeper getZk() {
    return zk;
  }

  private void connect() throws IOException {
    if (isConnected())
      return;
    LOG.info("Connecting to zk ensemble: " + quorumString);

    final SettableFuture<Boolean> connected = SettableFuture.create();
    this.zk = new ZooKeeper(quorumString, sessionTimeout, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        switch (event.getState()) {
          case SyncConnected:
            connected.set(true);
            // set global watcher
            zk.register(ZkConnection.this);
            break;
          case Expired:
            connected.setException(
                    new NoQuorumException("Failed to establish connection to " + quorumString));
        }
      }
    });
    try {
      connected.get(this.sessionTimeout, TimeUnit.MILLISECONDS);
      // do sync check
      zk.exists(chrootPath, false);
    } catch (Exception e) {
      close();
      throw new NoQuorumException("Failed to initialize zk quorum", e);
    }
    LOG.error("Created zk session 0x" + Long.toHexString(zk.getSessionId()));
  }

  /**
   * Exists will retried on connection loss.
   * @param path path to check
   * @return Stat object or null, null means path not exists
   * @throws IOException
   * @throws KeeperException
   */
  public Stat exists(String path) throws IOException, KeeperException {
    return waitForFeature(existsAsync(path));
  }

  /**
   * Async version of exists()
   * @param path path to check
   * @return Future
   * @see #exists(String)
   */
  public Future<Stat> existsAsync(String path) {
    final ExistsOp op = new ExistsOp(path);
    op.submitAsyncOperation();
    return op;
  }


  /**
   * Get data will retried on connection loss.
   * @param path path to get data for
   * @return ZNode object
   * @throws IOException
   * @throws KeeperException
   * @see org.apache.hadoop.coordination.zk.ZNode
   */
  public ZNode getData(String path) throws IOException, KeeperException {
    return waitForFeature(getDataAsync(path));
  }

  /**
   * Async version of getData()
   * @param path to get data for
   * @return Future with ZNode
   * @throws IOException
   * @throws KeeperException
   * @see #getData(String)
   */
  public Future<ZNode> getDataAsync(String path) throws IOException, KeeperException {
    final GetDataOp op = new GetDataOp(path);
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
          throws IOException, KeeperException {
    return waitForFeature(createAsync(path, bytes, acl, mode));
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
  public Future<String> createAsync(String path, byte[] bytes, ArrayList<ACL> acl, CreateMode mode) {
    final CreateOp op = new CreateOp(path, bytes, acl, mode);
    op.submitAsyncOperation();
    return op;
  }

  public Stat setData(String path, byte[] bytes, int version) throws IOException, KeeperException {
    return waitForFeature(setDataAsync(path, bytes, version));
  }

  public Future<Stat> setDataAsync(String path, byte[] bytes, int version) {
    final SetDataOp op = new SetDataOp(path, bytes, version);
    op.submitAsyncOperation();
    return op;
  }

  public void delete(String path) throws IOException, KeeperException {
    waitForFeature(deleteAsync(path, -1));
  }

  public void delete(String path, int version) throws IOException, KeeperException {
    waitForFeature(deleteAsync(path, version));
  }

  public Future<Void> deleteAsync(String path, int version) {
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
      case SyncConnected:
        break;
      case Expired:
        close();
      default:
        // FALL THROUGH
    }

  }

  /**
   * Base class for zk async operations.
   * Used for generality in error handling and reconnection.
   *
   * @param <R> return type
   */
  public abstract class ZkOperationFuture<V> extends AbstractFuture<V> implements TimerTask {

    /**
     * Return description of current operation.
     * Typically called for meaningful exceptions.
     *
     * @return description
     */
    public abstract String getDescription();

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
      if (!isIdempotent()) // don't retry for non-idempotent operations
        return super.setException(cause);

      final boolean rc;
      if (cause instanceof KeeperException) {
        switch (((KeeperException) cause).code()) {
          case CONNECTIONLOSS:
          case OPERATIONTIMEOUT:
            LOG.info("Lost connection to zk quorum performing " + getDescription() +
                    ", will retry");
            timer.newTimeout(this, RETRY_SLEEP_MILLIS, TimeUnit.MILLISECONDS);
            rc = false;
            break;
          case SESSIONEXPIRED:
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

    @Override
    final public void run(Timeout timeout) throws Exception {
      submitAsyncOperation();
    }
  }

  /**
   * Future for exists() operation
   */
  public class ExistsOp extends ZkOperationFuture<Stat> implements AsyncCallback.StatCallback {

    @Override
    public void processResult(int rc, String path, Object ctx, Stat stat) {
      final KeeperException.Code code = KeeperException.Code.get(rc);
      if (isSuccess(code)) {
        set(stat);
      } else if (isNodeDoesNotExist(code)) {
        set(null);
      } else {
        setException(KeeperException.create(code));
      }
    }

    private final String path;

    public ExistsOp(String path) {
      this.path = path;
    }

    @Override
    public void submitAsyncOperation() {
      zk.exists(path, false, this, this);
    }

    @Override
    public String getDescription() {
      return "exists(" + path + ")";
    }
  }

  /**
   * Future for create() operation
   * TODO: create shouldn't be idempotent for *_SEQENTIAL
   */
  public class CreateOp extends ZkOperationFuture<String> implements AsyncCallback.StringCallback {

    private final String path;
    private final byte[] bytes;
    private final List<ACL> acl;
    private final CreateMode mode;

    @Override
    public void processResult(int rc, String path, Object ctx, String name) {
      final KeeperException.Code code = KeeperException.Code.get(rc);
      if (isSuccess(code)) {
        set(name);
      } else {
        setException(KeeperException.create(code));
      }
    }

    public CreateOp(String path, byte[] bytes, List<ACL> acl, CreateMode mode) {
      this.path = path;
      this.bytes = bytes;
      this.mode = mode;
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
  public class SetDataOp extends ZkOperationFuture<Stat> implements AsyncCallback.StatCallback {

    @Override
    public void processResult(int rc, String path, Object ctx, Stat stat) {
      final KeeperException.Code code = KeeperException.Code.get(rc);
      if (isSuccess(code)) {
        set(stat);
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
  public class GetDataOp extends ZkOperationFuture<ZNode> implements AsyncCallback.DataCallback {

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

    public GetDataOp(String path) {
      this.path = path;
    }

    @Override
    public void submitAsyncOperation() {
      zk.getData(path, false, this, this);
    }

    @Override
    public String getDescription() {
      return "exists(" + path + ")";
    }
  }

  private <R> R waitForFeature(Future<R> op) throws IOException, KeeperException {
    try {
      return op.get(sessionTimeout, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw new NoQuorumException("Operation interrupted", e);
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
   * Future for setData() operation
   */
  public class DeleteOp extends ZkOperationFuture<Void> implements AsyncCallback.VoidCallback {

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
      return "setData(" + path + ", " + version + ")";
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
