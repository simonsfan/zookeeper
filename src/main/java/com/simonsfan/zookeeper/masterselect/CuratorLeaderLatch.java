package com.simonsfan.zookeeper.masterselect;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 类描述：基于Curator的Leader Latch实现的master选举
 * 创建人：simonsfan
 */
@Component
public class CuratorLeaderLatch {

    private static CuratorFramework curatorFramework;
    private static LeaderLatch leaderLatch;
    private static final String path = "/root/leaderlatch";
    private static final String connectStr = "10.200.121.46:2181,10.200.121.159:2181,10.200.121.168:2181";

    static {
        curatorFramework = CuratorFrameworkFactory
                .builder()
                .connectString(connectStr)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .connectionTimeoutMs(5000)
                .sessionTimeoutMs(5000)
                .build();

        curatorFramework.start();
        leaderLatch = new LeaderLatch(curatorFramework, path.concat("/testtast"));
    }

    @Lazy
    @Scheduled(cron = "")
    public void testTask() {
        try {
            //是master节点的执行业务流程
            if (!leaderLatch.hasLeadership()) return;
            //TODO something

        } catch (Exception e) {

        } finally {
            curatorFramework.close();
        }
    }
}
