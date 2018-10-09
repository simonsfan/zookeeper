package com.simonsfan.zookeeper.zkclient;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.SerializableSerializer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 类描述：zookeeper客户端zkclient使用demo
 */
public class ZkClientDemo {

    private static Logger logger = LoggerFactory.getLogger(ZkClientDemo.class);

    public static final String NODE_PATH = "/node_5";

    public static void main(String[] args) {

        ZkClient zkClient = new ZkClient("10.200.121.46:2181", 5000, 5000, new SerializableSerializer());

        logger.info("zookeeper connected success");

        NodeObject nodeObject = new NodeObject("simonsfan", "national_day");

        //创建持久、顺序节点 /node_5
        String path = zkClient.create(NODE_PATH, nodeObject, CreateMode.PERSISTENT_SEQUENTIAL);
        logger.info("create /node_5 return path={}", path);

        //读取指定节点下的数据内容
        //由于使用了序列化器，这里接收到的便不再是字节，而是直接反序列化好后的对象
        NodeObject nodeObj = zkClient.readData(NODE_PATH);
        logger.info("nodeObj=", nodeObj.toString());

        Stat stat = new Stat();
        NodeObject no = zkClient.readData(NODE_PATH, stat);
        logger.info(NODE_PATH + " stat=" + stat.toString());

        //获取子节点
        List<String> children = zkClient.getChildren(NODE_PATH);

        //判断是否存在节点
        boolean exists = zkClient.exists(NODE_PATH);

        //删除没有子节点的节点
        boolean del_no_node = zkClient.delete(NODE_PATH);

        //删除包含子节点的节点
        boolean del_has_node = zkClient.deleteRecursive(NODE_PATH);

        //修改节点数据内容
        NodeObject nodeObject1 = new NodeObject("jack", "mid_autumn_day");
        zkClient.writeData(NODE_PATH, nodeObject1);

        //订阅节点变化事件，包含  当前节点删除、子节点删除、子节点新增 均能收到通知
        zkClient.subscribeChildChanges(NODE_PATH, new NodeChangeListener());

        //订阅节点内容变化事件，包含 当前节点删除和节点数据内容变化
        zkClient.subscribeDataChanges(NODE_PATH,new DataChangeListener());

    }

    //订阅节点数据内容变化事件（监听），包含 当前节点删除、子节点删除、子节点新增 均能收到通知
    public static class NodeChangeListener implements IZkChildListener {
        @Override
        public void handleChildChange(String s, List<String> list) throws Exception {
            logger.info("subscriable node changes current path:{},children path:{}", s, list.toString());
        }
    }

    //订阅节点数据内容变化事件（监听），包含 节点删除、内容修改
    public static class DataChangeListener implements IZkDataListener {
        @Override
        public void handleDataChange(String s, Object o) throws Exception {
            logger.info("subscriable data changes current path:{},data:{}", s, o.toString());
        }

        @Override
        public void handleDataDeleted(String s) throws Exception {
            logger.info("subscriable data deleted current path:{}", s);
        }
    }

    @Getter
    @Setter
    @AllArgsConstructor
    @ToString
    public static class NodeObject {
        private String name;
        private String actId;
    }

}
