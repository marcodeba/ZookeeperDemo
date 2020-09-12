package zkDemo;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * 事件机制
 * 我们对事件进行了订阅，只要这个对象发生变化都有消息转发
 * watcher 特性：当数据发生变化的时候，zookeeper 会产生一个 watcher 事件并且会发送到客户端，但是客户端只会收到一次通知。
 * 如果后续这个节点再次发生变化，那么之前设置 watcher 的客户端不会再次收到消息(watcher 是一次性的操作)。
 * 可以通过循环监听去达到 永久监听效果
 * 凡是事务类型的操作，都会触发监听事件，create /delete /setData
 */
public class WatcherDemo {
//    public static void main(String[] args) throws Exception {
//        final CountDownLatch countDownLatch = new CountDownLatch(1);
//        final ZooKeeper zooKeeper = new ZooKeeper("localhost:2181", 4000, watchedEvent -> {
//            System.out.println("默认事件： " + watchedEvent.getType());
//            if (Watcher.Event.KeeperState.SyncConnected == watchedEvent.getState()) {
//                //如果收到了服务端的响应事件，连接成功
//                countDownLatch.countDown();
//            }
//        });
//        countDownLatch.await();
//
//        // 创建结点，绑定事件
//        zooKeeper.create("/zk-persist", "1".getBytes(),
//                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
//
//        // 通过exists绑定事件
//        Stat stat = zooKeeper.exists("/zk-persist", new Watcher() {
//            public void process(WatchedEvent watchedEvent) {
//                System.out.println(watchedEvent.getType() + "->" + watchedEvent.getPath());
//                try {
//                    //再一次去绑定事件
//                    zooKeeper.exists(watchedEvent.getPath(), true);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//        });
//        // 通过修改的事务类型操作来触发监听事件
//        stat = zooKeeper.setData("/zk-persist", "2".getBytes(), stat.getVersion());
//
//        Thread.sleep(1000);
//
//        zooKeeper.delete("/zk-persist", stat.getVersion());
//    }

    private static String CONNECTION_PATH = "localhost:2181";

    public static void main(String[] args) throws Exception {
        //PathChildCache  针对于子节点的创建、删除和更新触发事件
        //NodeCache  针对当前节点的变化触发事件
        //TreeCache  综合事件
        CuratorFramework curatorFramework = CuratorFrameworkFactory.builder().
                connectString(CONNECTION_PATH).sessionTimeoutMs(5000).
                retryPolicy(new ExponentialBackoffRetry(1000, 3)).build();
        curatorFramework.start();

//        addListenerWithNode(curatorFramework);
        addListenerWithChild(curatorFramework);

        System.in.read();
    }

    //配置中心
    // 当zookeeper中相应路径的节点发生更新、创建或者删除操作时，NodeCache将会得到响应，并且会将最新的数据拉到本地缓存中
    // NodeCache只会监听路径本身的变化，并不会监听子节点的变化
    private static void addListenerWithNode(CuratorFramework curatorFramework) throws Exception {
        NodeCache nodeCache = new NodeCache(curatorFramework, "/data", false);
        NodeCacheListener nodeCacheListener = () -> {
            System.out.println("receive Node Changed");

            System.out.println(nodeCache.getCurrentData().getPath() + "---" + new String(nodeCache.getCurrentData().getData()));
        };
        nodeCache.getListenable().addListener(nodeCacheListener);
        nodeCache.start();
    }

    //实现服务注册中心的时候，可以针对服务做动态感知
    private static void addListenerWithChild(CuratorFramework curatorFramework) throws Exception {
        PathChildrenCache nodeCache = new PathChildrenCache(curatorFramework, "/data", true);
        PathChildrenCacheListener nodeCacheListener =
                (curatorFramework1, pathChildrenCacheEvent) -> System.out.println(pathChildrenCacheEvent.getType()
                        + "->" + new String(pathChildrenCacheEvent.getData().getData()));
        nodeCache.getListenable().addListener(nodeCacheListener);
        nodeCache.start(PathChildrenCache.StartMode.NORMAL);
    }
}
