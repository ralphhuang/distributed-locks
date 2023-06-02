package io.github.ralphhuang.distrbute.locks.examples.etcd;

import io.etcd.jetcd.Client;
import io.github.ralphhuang.distrbute.locks.api.domain.LockParam;
import io.github.ralphhuang.distrbute.locks.examples.BaseTest;
import io.github.ralphhuang.distrbute.locks.impl.etcd.EtcdLock;
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
 * @version EtcdLockTest.java 2023/6/2 11:29 create by: huangfeitao
 **/
public class EtcdLockTest extends BaseTest {

    private static final String etcdTarget = "http://127.0.0.1:2379";

    private static EtcdLock etcdLock;

    @BeforeClass
    public static void init() {
        Client etcdClient = Client.builder().target(etcdTarget).build();
        etcdLock = new EtcdLock(etcdClient);
    }

    @Test
    public void run() {

        //time in mis to hold lock after apply lock success
        Supplier<Integer> supplier = () -> new Random().nextInt(100) + 20;
        Supplier<LockParam> lockParamSupplier = () -> LockParam.of("lockKey-" + UUID.randomUUID(), 3, TimeUnit.SECONDS);

        //32 thread apply for a lock in loop
        multiThreadForOneLock(1, supplier, lockParamSupplier, etcdLock);

        pause();

    }

    @Test
    public void runWithTemplate() {

        //time in mis to hold lock after apply lock success
        Supplier<Integer> supplier = () -> new Random().nextInt(100) + 20;
        Supplier<LockParam> lockParamSupplier = () -> LockParam.of("lockKey-" + UUID.randomUUID(), 3, TimeUnit.SECONDS);

        //32 thread apply for a lock in loop
        multiThreadForOneLockWithLockTemplate(8, supplier, lockParamSupplier, etcdLock);

        pause();

    }

}