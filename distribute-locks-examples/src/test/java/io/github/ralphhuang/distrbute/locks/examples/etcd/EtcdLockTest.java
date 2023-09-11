package io.github.ralphhuang.distrbute.locks.examples.etcd;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.lease.LeaseGrantResponse;
import io.etcd.jetcd.lease.LeaseKeepAliveResponse;
import io.etcd.jetcd.options.LeaseOption;
import io.github.ralphhuang.distrbute.locks.api.domain.LockParam;
import io.github.ralphhuang.distrbute.locks.api.exception.LockException;
import io.github.ralphhuang.distrbute.locks.examples.BaseTest;
import io.github.ralphhuang.distrbute.locks.impl.etcd.EtcdLock;
import io.grpc.stub.StreamObserver;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
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
                                  .retryMaxDuration(Duration.of(30, ChronoUnit.MINUTES))
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
            etcdLock.unlock(lockParam.getLockKey());
        }

        pause();
    }

    @Test
    public void run() {

        //time in mis to hold lock after apply lock success
        Supplier<Integer> supplier = () -> new Random().nextInt(300) + 20;
        //lock param
        Supplier<LockParam> lockParamSupplier = () -> LockParam.of(
            "lockKey-" + UUID.randomUUID(), 1000, TimeUnit.SECONDS, 30);

        //3 group,16 thread per group apply for a lock in loop

        multiThreadForOneLock(16, supplier, lockParamSupplier, etcdLock);
        multiThreadForOneLock(11, supplier, lockParamSupplier, etcdLock);
        multiThreadForOneLock(14, supplier, lockParamSupplier, etcdLock);

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
        multiThreadForOneLockWithLockTemplate(18, supplier, lockParamSupplier, etcdLock);
        multiThreadForOneLockWithLockTemplate(28, supplier, lockParamSupplier, etcdLock);

        pause();

    }

    /**
     * lease 自动续期？
     */
    @Test
    public void leaseTime() throws ExecutionException, InterruptedException {

        Client etcdClient = Client.builder().target(etcdTarget).build();

        Lease lease = etcdClient.getLeaseClient();

        //apply lease with N sesc
        int time = 5;
        LeaseGrantResponse response = lease.grant(time).get();
        LOGGER.info("leaseGrant:{}", response);

        //add auto renew
        lease.keepAlive(response.getID(), new StreamObserver<LeaseKeepAliveResponse>() {
            @Override
            public void onNext(LeaseKeepAliveResponse response) {
                LOGGER.info("onNext:{}", response);
            }

            @Override
            public void onError(Throwable throwable) {
                LOGGER.info("onError:",throwable);
            }

            @Override
            public void onCompleted() {
                LOGGER.info("onCompleted");
            }
        });

        new Thread(() -> {

            LOGGER.info("watcher thread started!");

            sleep(time * 1000 + 2000);

            //每5秒查看一次lease情况
           // while (true) {
                try {
                    LOGGER.info("lease ttl: {}", lease.timeToLive(response.getID(), LeaseOption.DEFAULT).get());
                    //释放了
                    LOGGER.info("lease revoke: {}", lease.revoke(response.getID()).get());
                } catch (Exception e) {
                    LOGGER.error("error:", e);
                    System.exit(-1);
                }

                sleep(1000);
           // }
        }, "lease-watch-thread").start();

        pause();
    }

}