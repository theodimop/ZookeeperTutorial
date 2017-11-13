import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Created by dj_di_000 on 11/11/2017.
 */
public class SimpleWatcher implements Watcher {

    private ZooKeeper zooKeeper;
    private ZookeeperClient zookeeperClient;
    private int i;
    public SimpleWatcher(int i) {
        zookeeperClient = new ZookeeperClient("localhost:27051,localhost:27054,localhost:27057");
        this.i = i;
        do {
            try {
                zooKeeper = zookeeperClient.get(10000);
            } catch (IOException | InterruptedException | TimeoutException e) {
                e.printStackTrace();
            }

            System.out.println("dsa");
        } while (zooKeeper == null);


        zooKeeper.register(this);
//        createSimpleEphemeralNode();
        zookeeperClient.registerExpirationHandler(this::reCreateSimpleEphemeralNode);

    }

    public void createSimpleEphemeralNode() {
        try {
            Stat stat = zooKeeper.exists("/crazy_"+i, true);
            if (stat == null)
                zooKeeper.create("/crazy_"+i, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }

    }

    private void reCreateSimpleEphemeralNode() throws InterruptedException, TimeoutException, IOException {
        ZookeeperClient.LOG.warning("Recreated");
        zookeeperClient = new ZookeeperClient("localhost:27051");
        zooKeeper = zookeeperClient.get(20000);

        try {
            Stat stat = zooKeeper.exists("/crazy_"+i, false);
            if (stat != null)
                return;

            zooKeeper.create("/crazy_"+i, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        switch (watchedEvent.getType()) {
            case NodeCreated:
                ZookeeperClient.LOG.info("Node " + watchedEvent.getPath() + " was created!");
                break;
        }
    }
}
