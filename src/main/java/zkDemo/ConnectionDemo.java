package zkDemo;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.CountDownLatch;

public class ConnectionDemo {
    public static void main(String[] args) throws Exception {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        ZooKeeper zooKeeper = new ZooKeeper("localhost:2181", 4000, watchedEvent -> {
            if (Watcher.Event.KeeperState.SyncConnected == watchedEvent.getState()) {
                countDownLatch.countDown();
            }
        });
        countDownLatch.await();
        System.out.println(zooKeeper.getState());

        // 增
        zooKeeper.create("/zk-persist", "0".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        Thread.sleep(1000);

        // 查
        Stat stat = new Stat();
        byte[] bytes = zooKeeper.getData("/zk-persist", null, stat);
        System.out.println(new String(bytes));

        // 改
        zooKeeper.setData("/zk-persist", "1".getBytes(), stat.getVersion());
        byte[] bytes1 = zooKeeper.getData("/zk-persist", null, stat);
        System.out.println(new String(bytes1));

        // 删
        zooKeeper.delete("/zk-persist", stat.getVersion());

        zooKeeper.close();
    }
}
