package com.simonsfan.zookeeper.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 类描述：zookeeper客户端curator使用demo
 */
public class CuratorDemo {

    private static final String NODE_PATH = "/node_8";
    private static final String CONNECT_TOSTRING = "10.200.121.46:2181";

    public static ExecutorService executorService = Executors.newCachedThreadPool();


    public static void main(String[] args) throws Exception {
        try {
/*
        */
/*重试策略一：重试三次，每重试一次，重试的间隔时间会越来越大*//*

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);

        */
/*重试策略二：最多重试三次，每次重试间隔1s*//*

        RetryPolicy retryPolicy1 = new RetryNTimes(3,1000);
*/

        /*重试策略三：最大重试时间总和不超过5s，每次重试间隔为1s*/
            RetryPolicy retryPolicy2 = new RetryUntilElapsed(5000, 1000);

/*
        */
/*方式一建立zookeeper连接*//*

        CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(CONNECT_TOSTRING,5000,5000,retryPolicy2);
*/

        /*方式二建立zookeeper连接*/
            CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
                    .connectString(CONNECT_TOSTRING)
                    .sessionTimeoutMs(5000)
                    .connectionTimeoutMs(5000)
                    .retryPolicy(retryPolicy2)
                    .build();

            curatorFramework.start();

        /*创建节点数据*/
            curatorFramework.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(NODE_PATH, "456".getBytes());

        /*删除节点（包含子节点）*/
            curatorFramework.delete().guaranteed().deletingChildrenIfNeeded().withVersion(-1).forPath(NODE_PATH);

        /*获取子节点*/
            List<String> strings = curatorFramework.getChildren().forPath(NODE_PATH);

        /*获取节点数据内容*/
            byte[] bytes = curatorFramework.getData().forPath(NODE_PATH);
            System.out.println(new String(bytes));
        
       /*获取节点数据内容+状态信息*/
            Stat stat = new Stat();
            byte[] result = curatorFramework.getData().storingStatIn(stat).forPath(NODE_PATH);
            System.out.println(new String(result));

        /*修改节点数据内容*/
            curatorFramework.setData().forPath(NODE_PATH, "123".getBytes());

       /*判断节点是否存在*/
            Stat stat1 = curatorFramework.checkExists().forPath(NODE_PATH);

        /*异步操作，以判断节点是否存在为例，注意使用线程池节省单个线程的创建销毁开销，及最后线程的关闭*/
            curatorFramework.checkExists().inBackground(new BackgroundCallback() {
                @Override
                public void processResult(CuratorFramework curatorFramework, CuratorEvent curatorEvent) throws Exception {

                }
            }, "12345", executorService).forPath(NODE_PATH);

        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            executorService.shutdown();
        }
    }

}