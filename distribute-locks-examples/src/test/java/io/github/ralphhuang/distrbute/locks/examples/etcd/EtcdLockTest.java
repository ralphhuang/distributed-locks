package io.github.ralphhuang.distrbute.locks.examples.etcd;

import io.etcd.jetcd.Client;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.lease.LeaseGrantResponse;
import io.github.ralphhuang.distrbute.locks.api.domain.LockParam;
import io.github.ralphhuang.distrbute.locks.api.exception.LockException;
import io.github.ralphhuang.distrbute.locks.api.util.StopWatch;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * @author huangfeitao
 * @version EtcdLockTest.java 2023/6/2 11:29 create by: huangfeitao
 **/
public class EtcdLockTest extends BaseTest {

    //private static final String etcdTarget = "http://127.0.0.1:2379";
    private static final String etcdTarget = "127.0.0.1:2379";

    private static EtcdLock etcdLock;

    @BeforeClass
    public static void init() {
        Client etcdClient = Client.builder().target(etcdTarget).build();
        etcdLock = new EtcdLock(etcdClient);
    }

    @Test
    public void simpleCase() throws LockException {
        Supplier<Integer> supplier = () -> new Random().nextInt(100) + 20;
        Supplier<LockParam> lockParamSupplier = () -> LockParam.of(
            "lockKey-" + UUID.randomUUID(), 3, TimeUnit.SECONDS, 10
        );

        LockParam lockParam = lockParamSupplier.get();

        int reentrantTimes = 3;

        for (int i = 0; i < reentrantTimes; i++) {
            etcdLock.lock(lockParam);
            pause();
        }

        for (int i = 0; i < reentrantTimes; i++) {
            etcdLock.release(lockParam);
        }

        pause();
    }

    @Test
    public void run() {

        //time in mis to hold lock after apply lock success
        Supplier<Integer> supplier = () -> 1000;
        //Supplier<Integer> supplier = () -> new Random().nextInt(100) + 20;
        Supplier<LockParam> lockParamSupplier = () -> LockParam.of(
            "lockKey-" + UUID.randomUUID(), 1000, TimeUnit.SECONDS, 100
        );

        //32 thread apply for a lock in loop
        multiThreadForOneLock(16, supplier, lockParamSupplier, etcdLock);

        pause();

    }

    @Test
    public void runWithTemplate() {

        //time in mis to hold lock after apply lock success
        Supplier<Integer> supplier = () -> new Random().nextInt(100) + 20;
        Supplier<LockParam> lockParamSupplier = () -> LockParam.of(
            "lockKey-" + UUID.randomUUID(), 3, TimeUnit.SECONDS, 1
        );

        //32 thread apply for a lock in loop
        multiThreadForOneLockWithLockTemplate(8, supplier, lockParamSupplier, etcdLock);

        pause();

    }

    @Test
    public void lease() throws ExecutionException, InterruptedException {

        Client etcdClient = Client.builder().target(etcdTarget).build();

        int i = 32;

        Lease lease = etcdClient.getLeaseClient();

        for (int j = 0; j < i; j++) {
            int finalJ = j;
            new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        StopWatch stopWatch = StopWatch.start();
                        try {
                            LeaseGrantResponse response = lease.grant(3, 10, TimeUnit.SECONDS).get();
                        } catch (Throwable e) {
                            e.printStackTrace();
                        }
                        LOGGER.info("index={},timeCost={}ms", finalJ, stopWatch.get());
                        sleep(new Random().nextInt(100));
                    }
                }
            }).start();
        }

        pause();
    }

}