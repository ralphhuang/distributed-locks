package io.github.ralphhuang.distrbute.locks.examples.zookeeper;

import io.github.ralphhuang.distrbute.locks.api.domain.LockParam;
import io.github.ralphhuang.distrbute.locks.examples.BaseTest;
import io.github.ralphhuang.distrbute.locks.impl.zookeeper.ZookeeperLock;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * @author huangfeitao
 * @version LockTest.java 2023/6/2 11:29 create by: huangfeitao
 **/
public class ZookeeperLockTest extends BaseTest {

    private static final String zookeeperAddress = "127.0.0.1:2181";

    private static ZookeeperLock zookeeperLock;

    @BeforeClass
    public static void init() {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(3000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient(zookeeperAddress, retryPolicy);
        client.start();
        zookeeperLock = new ZookeeperLock(client);
    }

    @Test
    public void run() {

        //time in mis to hold lock after apply lock success
        Supplier<Integer> supplier = () -> new Random().nextInt(100) + 20;
        Supplier<LockParam> lockParamSupplier = () -> LockParam.of("lockKey-" + UUID.randomUUID(), 3, TimeUnit.SECONDS);

        //32 thread apply for a lock in loop
        multiThreadForOneLock(32, supplier, lockParamSupplier, zookeeperLock);

        //8 thread apply for a lock in loop
        multiThreadForOneLock(8, supplier, lockParamSupplier, zookeeperLock);

        //12 thread apply for a lock in loop
        multiThreadForOneLock(12, supplier, lockParamSupplier, zookeeperLock);

        pause();

    }

    @Test
    public void runWithTemplate() {

        //time in mis to hold lock after apply lock success
        Supplier<Integer> supplier = () -> new Random().nextInt(100) + 20;
        Supplier<LockParam> lockParamSupplier = () -> LockParam.of("lockKey-" + UUID.randomUUID(), 3, TimeUnit.SECONDS);

        //32 thread apply for a lock in loop
        //multiThreadForOneLockWithLockTemplate(32, supplier, lockParamSupplier, zookeeperLock);

        //8 thread apply for a lock in loop
        multiThreadForOneLockWithLockTemplate(8, supplier, lockParamSupplier, zookeeperLock);

        //12 thread apply for a lock in loop
      //  multiThreadForOneLockWithLockTemplate(12, supplier, lockParamSupplier, zookeeperLock);

        pause();

    }

}