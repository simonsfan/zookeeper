package com.simonsfan.zookeeper.namespaceservice;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 类描述：分布式唯一ID生成器
 * <p>
 * 创建人：simonsfan
 */
@Component
public class DistributedIdGeneraterService {

    private static CuratorFramework curatorFrameworkClient;

    private static RetryPolicy retryPolicy;

    private static ExecutorService executorService;

    private static String IP_TOSTRING = "10.200.121.46:2181,10.200.121.43:2181,10.200.121.167:2181";

    private static String ROOT = "/root";

    private static String NODE_NAME = "idgenerator";

    static {
        retryPolicy = new ExponentialBackoffRetry(1000, 3);
        curatorFrameworkClient = CuratorFrameworkFactory
                .builder()
                .connectString(IP_TOSTRING)
                .sessionTimeoutMs(5000)
                .connectionTimeoutMs(5000)
                .retryPolicy(retryPolicy)
                .build();
        curatorFrameworkClient.start();
        try {
            executorService = Executors.newFixedThreadPool(10);

            Stat stat = curatorFrameworkClient.checkExists().forPath(ROOT);
            if (stat == null) {  //请先判断/root节点是否存在
                curatorFrameworkClient.create().withMode(CreateMode.PERSISTENT).forPath(ROOT, null);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String generateId() {
        String backPath = "";

        String fullPath = ROOT.concat("/").concat(NODE_NAME);
        try {
            // 关键点：创建持久顺序节点
            backPath = curatorFrameworkClient.create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath(fullPath, null);
            //为防止生成的节点浪费系统资源，故生成后异步删除此节点
            String finalBackPath = backPath;
            executorService.execute(() -> {
                try {
                    curatorFrameworkClient.delete().forPath(finalBackPath);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            String ID = this.splitID(backPath);
            System.out.println("生成的ID=" + ID);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return backPath;
    }

    public String splitID(String path) {
        int index = path.lastIndexOf(NODE_NAME);
        if (index >= 0) {
            index += NODE_NAME.length();
            return index <= path.length() ? path.substring(index) : "";
        }
        return path;

    }


}
