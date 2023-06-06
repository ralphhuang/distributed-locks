package io.github.ralphhuang.distrbute.locks.examples.etcd;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.lease.LeaseGrantResponse;
import io.github.ralphhuang.distrbute.locks.api.domain.LockParam;
import io.github.ralphhuang.distrbute.locks.api.exception.LockException;
import io.github.ralphhuang.distrbute.locks.api.util.StopWatch;
import io.github.ralphhuang.distrbute.locks.examples.BaseTest;
import io.github.ralphhuang.distrbute.locks.impl.etcd.EtcdLock;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
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
        Client etcdClient = Client.builder()
                                  .namespace(ByteSequence.from("/ectcLock/".getBytes(StandardCharsets.UTF_8)))
                                  .target(etcdTarget)
                                  .waitForReady(true)
                                  //最大重连持续30分钟
                                  .retryMaxDuration(Duration.of(30, ChronoUnit.MINUTES))
                                  //重试时间间隔，10秒
                                  .retryDelay(10)
                                  .retryChronoUnit(ChronoUnit.SECONDS)
                                  .keepaliveWithoutCalls(true)
                                  .keepaliveTime(Duration.of(2, ChronoUnit.MINUTES))
                                  .keepaliveTimeout(Duration.of(10, ChronoUnit.SECONDS))
                                  .build();
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
        //Supplier<Integer> supplier = () -> 1000;
        Supplier<Integer> supplier = () -> new Random().nextInt(300) + 20;
        Supplier<LockParam> lockParamSupplier = () -> LockParam.of(
            "lockKey-" + UUID.randomUUID(), 1000, TimeUnit.SECONDS, 30);

        //3 group,16 thread per group apply for a lock in loop

        for (int i = 0; i < 2; i++) {
            multiThreadForOneLock(16, supplier, lockParamSupplier, etcdLock);
        }

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

    /**
     * 问题：lease.grant 每个线程的第一次执行，耗时比较久
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void leaseTime() throws ExecutionException, InterruptedException {

        Client etcdClient = Client.builder().target(etcdTarget).build();

        int i = 32;

        Lease lease = etcdClient.getLeaseClient();

        for (int j = 0; j < i; j++) {
            int finalJ = j;
            new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int k = 0; k < 5; k++) {
                        StopWatch stopWatch = StopWatch.start();
                        try {
                            LeaseGrantResponse response = lease.grant(3, 10, TimeUnit.SECONDS).get();
                        } catch (Throwable e) {
                            e.printStackTrace();
                        }
                        LOGGER.info("index={},timeCost={}ms", finalJ, stopWatch.get());
                        //sleep(new Random().nextInt(100));
                    }
                }
            }).start();
        }

        pause();
    }

}