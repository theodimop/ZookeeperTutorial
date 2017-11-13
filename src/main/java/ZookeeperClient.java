import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;


import java.io.IOException;
import java.util.Set;
import java.util.concurrent.*;
import java.util.logging.Logger;

/**
 * Created by dj_di_000 on 11/11/2017.
 */
public class ZookeeperClient  {

    public static final Logger LOG = Logger.getLogger(ZookeeperClient.class.getName());
    private static final int sessionTimeoutMs = 5_000;
    private final Set<Watcher> watchers = new CopyOnWriteArraySet<>();


    private final BlockingQueue<WatchedEvent> eventQueue = new LinkedBlockingQueue<>();

    private String zookeeperServers;
    private SessionState sessionState;
    private volatile ZooKeeper zookeeper;

    public ZookeeperClient(String zookeeperServers) {

        Thread watcherProcessor = new Thread("ZookeeperClient-watcherProcessor") {
            @Override
            public void run() {
                while (true) {
                    try {
                        WatchedEvent event = eventQueue.take();
                        for (Watcher watcher : watchers) {
                            watcher.process(event);
                        }
                    } catch (InterruptedException e) { /* ignore */ }
                }
            }
        };
        watcherProcessor.setDaemon(true);
        watcherProcessor.start();

        this.zookeeperServers = zookeeperServers;

    }

    public synchronized ZooKeeper get(long timeout) throws IOException, InterruptedException, TimeoutException {

        if (zookeeper == null) {
            final CountDownLatch connected = new CountDownLatch(1);
            Watcher watcher = event -> {
                switch (event.getType()) {
                    // Guard the None type since this watch may be used as the default watch on calls by
                    // the client outside our control.
                    case None:
                        switch (event.getState()) {
                            case Expired:
                                LOG.info("Zookeeper session expired. Event: " + event);
                                close();
                                break;
                            case SyncConnected:
                                LOG.info("Zookeeper connected!");
                                connected.countDown();
                                break;
                            //I dont need the disconnected
                            case Disconnected:
                                LOG.info("Zookeeper disconnected!");
                                break;
                        }
                }

                eventQueue.offer(event);
            };

            try {
                zookeeper = (sessionState != null)
                        ? new ZooKeeper(zookeeperServers, sessionTimeoutMs, watcher, sessionState.sessionId,
                        sessionState.sessionPasswd)
                        : new ZooKeeper(zookeeperServers, sessionTimeoutMs, watcher);
            } catch (IOException e) {
                throw new IOException(
                        "Problem connecting to servers: " + zookeeperServers, e);
            }

            if (timeout > 0) {
                if (!connected.await(timeout, TimeUnit.MILLISECONDS)) {
                    close();
                    throw new TimeoutException("Timed out waiting for a ZK connection after "
                            + timeout);
                }
            } else {
                try {
                    connected.await();
                } catch (InterruptedException ex) {
                    LOG.info("Interrupted while waiting to connect to zooKeeper");
                    close();
                    throw ex;
                }
            }
            sessionState = new SessionState(zookeeper.getSessionId(), zookeeper.getSessionPasswd());
        }
        return zookeeper;

    }


    public void register(Watcher watcher) {
        watchers.add(watcher);
    }

    /**
     * Clients can attempt to unregister a top-level {@code Watcher} that has previously been
     * registered.
     *
     * @param watcher the {@code Watcher} to unregister as a top-level, persistent watch
     * @return whether the given {@code Watcher} was found and removed from the active set
     */
    public boolean unregister(Watcher watcher) {
        return watchers.remove(watcher);
    }

    public Watcher registerExpirationHandler(final Command onExpired) {
        Watcher watcher = event -> {
            if (event.getType() == Watcher.Event.EventType.None &&
                    event.getState() == Watcher.Event.KeeperState.Expired) {
                try {
                    onExpired.execute();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (TimeoutException e) {
                    e.printStackTrace();
                }
            }
        };
        register(watcher);
        return watcher;
    }

    public synchronized void close() {
        if (zookeeper != null) {
            try {
                zookeeper.close();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warning("Interrupted trying to close zooKeeper");
            } finally {
                zookeeper = null;
                sessionState = null;
            }
        }
    }

    private boolean isClosed() {
        return zookeeper == null;
    }

    protected void finalize() throws Throwable {

        close();

        super.finalize();

    }

    private final class SessionState {
        private final long sessionId;
        private final byte[] sessionPasswd;

        private SessionState(long sessionId, byte[] sessionPasswd) {
            this.sessionId = sessionId;
            this.sessionPasswd = sessionPasswd;
        }
    }



}
