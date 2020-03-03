package zkDemo;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class LockDemo {
    private static String CONNECTION_PATH = "localhost:2181";

    public static void main(String[] args) {
        CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
                .connectString(CONNECTION_PATH)
                .sessionTimeoutMs(5000)
                .connectionTimeoutMs(1000)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
        curatorFramework.start();

        ExecutorService executorService = Executors.newFixedThreadPool(10);
        final InterProcessMutex lock = new InterProcessMutex(curatorFramework, "/locks");
        for (int i = 0; i < 10; i++) {
            executorService.execute(() -> {
                try {
                    System.out.println(Thread.currentThread().getName() + "->尝试竞争锁");
                    if (lock.acquire(4000, TimeUnit.SECONDS)){
                        System.out.println(Thread.currentThread().getName() + "->成功获得了锁");
                        Thread.sleep(2000);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    try {
                        lock.release();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }
    }
}
