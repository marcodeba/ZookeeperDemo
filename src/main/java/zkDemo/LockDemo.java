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
        CuratorFramework curatorFramework = CuratorFrameworkFactory
                .builder()
                .connectString(CONNECTION_PATH)
                .connectionTimeoutMs(5000)
                .sessionTimeoutMs(3000)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
        curatorFramework.start();

        final InterProcessMutex mutexLock = new InterProcessMutex(curatorFramework, "/locks");
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        for (int i = 0; i < 10; i++) {
            executorService.execute(() -> {
                try {
                    System.out.println(Thread.currentThread().getName() + "->尝试竞争锁");
                    if (mutexLock.acquire(4000, TimeUnit.SECONDS)) {
                        Thread.sleep(2000);
                        System.out.println(Thread.currentThread().getName() + "->成功获得了锁");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    try {
                        mutexLock.release();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }
    }
}