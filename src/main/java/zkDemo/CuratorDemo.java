package zkDemo;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;

import java.util.ArrayList;
import java.util.List;

public class CuratorDemo {
    private static final String CONNECTION_PATH = "localhost:2181";

    public static void main(String[] args) {
        CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
                .connectString(CONNECTION_PATH)
                .sessionTimeoutMs(5000).retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
        curatorFramework.start();

        createData(curatorFramework);
        updateData(curatorFramework);
        deleteData(curatorFramework);
        createDataWithACL(curatorFramework);

        curatorFramework.close();
    }

    public static void createData(CuratorFramework curatorFramework) {
        try {
            curatorFramework.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
                    .forPath("/data/program", "test".getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void updateData(CuratorFramework curatorFramework) {
        try {
            curatorFramework.setData().forPath("/data/program", "update".getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void deleteData(CuratorFramework curatorFramework) {
        Stat stat = new Stat();

        try {
            curatorFramework.getData().storingStatIn(stat).forPath("/data/program");
            curatorFramework.delete().withVersion(stat.getVersion()).forPath("/data/program");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void createDataWithACL(CuratorFramework curatorFramework) {
        List<ACL> aclList = new ArrayList<>();
        try {
            ACL acl = new ACL(ZooDefs.Perms.READ | ZooDefs.Perms.WRITE,
                    new Id("digest",
                            DigestAuthenticationProvider.generateDigest("admin:admin")));
            aclList.add(acl);

            curatorFramework.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
                    .withACL(aclList)
                    .forPath("/auth", "auth".getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void setAuthForExistedNode(CuratorFramework curatorFramework) {
        List<ACL> aclList = new ArrayList<>();
        try {
            ACL acl = new ACL(ZooDefs.Perms.READ | ZooDefs.Perms.WRITE,
                    new Id("digest",
                            DigestAuthenticationProvider.generateDigest("admin:admin")));
            aclList.add(acl);

            curatorFramework.setACL().withACL(aclList).forPath("/temp");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
