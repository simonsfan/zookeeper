package com.simonsfan.zookeeper.zookeeper;

import com.simonsfan.zookeeper.ZookeeperApplicationTests;
import com.simonsfan.zookeeper.namespaceservice.DistributedIdGeneraterService;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * 类描述：分布式id生成器测试类

 */
public class DistributedIdGeneraterTest extends ZookeeperApplicationTests {

    @Autowired
    private DistributedIdGeneraterService distributedIdGeneraterService;

    @Test
    public void testIdGenerate(){
        for (int i = 0; i < 10; i++) {
            distributedIdGeneraterService.generateId();
        }
    }

}
