import java.io.IOException;
import java.util.Scanner;

/**
 * Created by dj_di_000 on 11/11/2017.
 */
public class Main {

    public static void main(String[] args) {
        ZookeeperClient zookeeperClient = new ZookeeperClient("localhost:27051,localhost:27054,localhost:27057");
        new SimpleWatcher(1).createSimpleEphemeralNode();
        Scanner scanner = new Scanner(System.in);
            while(scanner.next().equals("y")) {
                int i = 1;
                new SimpleWatcher(++i);
            }

//            ZooKeeper zooKeeper = zookeeperClient.get(20000);
//            zooKeeper.create("/test",new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        try {
            System.in.read("Press any key to continue...".getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
