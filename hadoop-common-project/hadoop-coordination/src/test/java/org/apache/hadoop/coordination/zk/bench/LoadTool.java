package org.apache.hadoop.coordination.zk.bench;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.coordination.Agreement;
import org.apache.hadoop.coordination.AgreementHandler;
import org.apache.hadoop.coordination.ConsensusProposal;
import org.apache.hadoop.coordination.NoQuorumException;
import org.apache.hadoop.coordination.ProposalNotAcceptedException;
import org.apache.hadoop.coordination.zk.ZKCoordinationEngine;

/**
 * Simple load tool for CE.
 *
 * Idea is simple: exploiting idea of global sequencing.
 * Each load thread sumbits RegisterProposal and awaits
 * for agreement accepted.
 * Learner awaits for proposals arrival. Registration proposals
 * used to assign unique id for generator threads. If
 * thread is found in Learner, it assigned with GSN as id.
 * Also for all threads Learner maintains Random initialized
 * with seed == GSN. That gives predictable pseudorandom sequence
 * in any JVM. So, upon LoadProposal arrivals Learner
 * checks that sequence is the same as on generator side
 * (generator uses Random also initialized with GSN).
 *
 * Ideally, system should behave well even with zk disconnects
 * and restarts.
 */
public class LoadTool {

  private static final Log LOG = LogFactory.getLog(LoadTool.class);

  public static final String CE_BENCH_ITERATIONS_KEY = "ce.bench.iterations";
  public static final String CE_BENCH_SECONDS_KEY = "ce.bench.seconds";
  public static final String CE_BENCH_THREADS_KEY = "ce.bench.threads";

  private final ZKCoordinationEngine engine;
  private final Configuration conf;

  private volatile LoadGenerator generator;

  LoadTool(Configuration conf) {
    this.conf = conf;
    this.engine = new ZKCoordinationEngine("engine");
  }

  public void run() throws ProposalNotAcceptedException, NoQuorumException, InterruptedException {
    generator = new LoadGenerator();
    engine.init(conf);
    engine.registerHandler(new CoordinationHandler(generator));
    engine.start();
    final int numThreads = conf.getInt(CE_BENCH_THREADS_KEY, 5);
    LOG.info("Starting " + numThreads + " threads");
    generator.spawn(numThreads);
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          generator.stop();
        } catch (InterruptedException e) {
          LOG.error(e);
        }
        engine.stop();
      }
    }));
    generator.awaitThreads();
  }

  class LoadThread {
    private final String name;
    private final LoadGenerator generator;
    private final CountDownLatch barrier = new CountDownLatch(1);
    private final CountDownLatch done = new CountDownLatch(1);
    private final long maxIteration = conf.getLong(CE_BENCH_ITERATIONS_KEY, Long.MAX_VALUE);

    private Long id;
    private Random rnd;
    private Thread thread;
    private volatile long seenIteration = -1;

    public LoadThread(String name, LoadGenerator generator) {
      this.name = name;
      this.generator = generator;
    }

    public void assignId(Long id) {
      this.id = id;
      this.rnd = new Random(id);
      LOG.info("Thread " + name + " activated with id=" + id);
      barrier.countDown();
    }

    public synchronized void seenIteration(long iteration) {
      if (seenIteration < iteration)
        seenIteration = iteration;
      if (seenIteration >= maxIteration)
        done.countDown();
    }

    public synchronized void start() {
      if (thread != null)
        return;
      LOG.info("Starting thread " + name);
      thread = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            barrier.await();
            long millis = conf.getLong(CE_BENCH_SECONDS_KEY, Long.MAX_VALUE / 10000) * 1000;
            long startMillis = System.currentTimeMillis();
            long iteration = 0;
            while (!Thread.currentThread().isInterrupted()
                    && (iteration++ < maxIteration)
                    && System.currentTimeMillis() - startMillis < millis) {
              final LoadProposal proposal = new LoadProposal(engine.getLocalNodeId(), id, rnd.nextLong(), iteration);
              if (LOG.isTraceEnabled())
                LOG.trace("Proposing " + proposal + " from " + name);
              engine.submitProposal(proposal, false);
            }
            LOG.info("Finished proposing, awaiting catch up " + name);
            done.await();
            LOG.info("Done " + name);
          } catch (Exception e) {
            LOG.error("Failed to submit " + name, e);
            throw Throwables.propagate(e);
          } finally {
            generator.deregister(LoadThread.this);
          }
        }
      });
      thread.start();
    }

    public synchronized void stop() throws InterruptedException {
      if (thread != null) {
        thread.interrupt();
        thread.join();
        thread = null;
      }
    }
  }

  private static String threadId(Serializable nodeId, int requestId) {
    return nodeId.toString() + ":" + requestId;
  }

  class LoadGenerator {
    private int requestId = 0;
    private Lock lock = new ReentrantLock();
    private Condition cond = lock.newCondition();
    private Map<String, LoadThread> threads = Maps.newHashMap();
    private Map<Long, LoadThread> id2Thread = Maps.newHashMap();
    private Map<Long, Random> state = Maps.newHashMap();

    /**
     * Advance state of thread, compare with expected random sequence.
     */
    public Long advance(LoadProposal proposal) {
      final Long generatorId = proposal.getGeneratorId();
      Random random = state.get(generatorId);
      if (random == null) {
        random = new Random(generatorId);
        state.put(generatorId, random);
      }
      final LoadThread lt = id2Thread.get(generatorId);
      if (lt != null)
        lt.seenIteration(proposal.getIteration());
      final Long value = proposal.getValue();
      if (!(random.nextLong() == value)) {
        throw new IllegalStateException("Failed at " + generatorId +
                " and seq " + value + " on iteration " + proposal.getIteration());
      }
      if (LOG.isTraceEnabled())
        LOG.trace("Check passed " + proposal);

      return value;
    }

    public void deregister(LoadThread lt) {
      lock.lock();
      try {
        threads.remove(lt.name);
        id2Thread.remove(lt.id);
        cond.signal();
      } finally {
        lock.unlock();
      }
    }

    /**
     * Whence registration arrived, we either register new state for remote
     * generator or start our own. In both case GSN should be the same, so
     * it is safe to assume that Random will be initialized with the same GSN.
     */
    public void register(Serializable proposerNodeId, int requestId) {
      Long id = engine.getGlobalSequenceNumber();
      lock.lock();
      try {
        state.put(id, new Random(id));
        final LoadThread lt = threads.get(threadId(proposerNodeId, requestId));
        if (lt != null) {
          lt.assignId(id);
          id2Thread.put(id, lt);
        }
      } finally {
        lock.unlock();
      }
    }

    public void spawn(int amount) {
      for (int i = 0; i < amount; i++) {
        lock.lock();
        try {
          final int req = requestId++;
          final LoadThread lt = new LoadThread(threadId(engine.getLocalNodeId(), req), this);
          threads.put(lt.name, lt);
          lt.start();
          engine.submitProposal(new RegisterProposal(engine.getLocalNodeId(), req), true);
        } catch (Exception e) {
          throw Throwables.propagate(e);
        } finally {
          lock.unlock();
        }
      }
    }

    public void stop() throws InterruptedException {
      for (LoadThread loadThread : threads.values()) {
        loadThread.stop();
      }
    }

    public void awaitThreads() throws InterruptedException {
      lock.lock();
      try {
        while (threads.size() > 0) {
          cond.await();
        }
      } finally {
        lock.unlock();
      }
    }
  }

  static class CoordinationHandler implements AgreementHandler<LoadGenerator> {

    private LoadGenerator loadGenerator;

    CoordinationHandler(LoadGenerator loadGenerator) {
      this.loadGenerator = loadGenerator;
    }

    @Override
    public LoadGenerator getLearner() {
      return loadGenerator;
    }

    @Override
    public void setLearner(LoadGenerator loadGenerator) {
      this.loadGenerator = loadGenerator;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void executeAgreement(Agreement<?, ?> agreement) {
      ConsensusProposal agreed = (ConsensusProposal) agreement;
      try {
        agreed.execute(getLearner());
      } catch (IOException e) {
        LOG.error("Failed to apply agreement: " + agreement, e);
      }
    }
  }

  public static void main(String[] args) throws ProposalNotAcceptedException, NoQuorumException, InterruptedException {
    Configuration conf = new Configuration();
    conf.addResource(args[0]);
    new LoadTool(conf).run();
  }
}
