package io.github.ralphhuang.distrbute.locks.impl.etcd;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.Lock;
import io.etcd.jetcd.lease.LeaseKeepAliveResponse;
import io.etcd.jetcd.lock.LockResponse;
import io.github.ralphhuang.distrbute.locks.api.LockFacade;
import io.github.ralphhuang.distrbute.locks.api.collection.Pair;
import io.github.ralphhuang.distrbute.locks.api.domain.LockParam;
import io.github.ralphhuang.distrbute.locks.api.exception.LockException;
import io.github.ralphhuang.distrbute.locks.api.exception.LockExceptionCode;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author huangfeitao
 * @version EtcdLock.java 2023/6/2 15:44 create by: huangfeitao
 **/
public class EtcdLock implements LockFacade {

    private static final Logger LOGGER = LoggerFactory.getLogger(EtcdLock.class);
    private static final String DEFAULT_ROOT = "/DISTRIBUTE-LOCKS/";
    private static final StreamObserver<LeaseKeepAliveResponse> DISCARD_OBSERVER = new DiscardStreamObserver();

    /**
     * in one thread may apply several locks
     */
    private static final ThreadLocal<Map<String, Pair<Long, ByteSequence>>> tl = ThreadLocal.withInitial(HashMap::new);

    /**
     * all PERSISTENT nodes root path for locks
     * default is /DISTRIBUTE-LOCKS/
     */
    public String rootPath;
    private Lock lockClient;
    private Lease leaseClient;

    public EtcdLock(Client etcdClient) {
        this(DEFAULT_ROOT, etcdClient);
    }

    public EtcdLock(String rootPath, Client etcdClient) {
        if (rootPath == null || rootPath.trim().isEmpty()) {
            rootPath = DEFAULT_ROOT;
        } else {
            if (!rootPath.startsWith("/")) {
                rootPath = "/" + rootPath;
            }
            if (!rootPath.endsWith("/")) {
                rootPath = rootPath + "/";
            }
        }
        this.rootPath = rootPath;
        this.lockClient = etcdClient.getLockClient();
        this.leaseClient = etcdClient.getLeaseClient();
    }

    /**
     * executes for async tasks
     */
    private static final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(4);

    @Override
    public void lock(LockParam lockParam) throws LockException {

        String lockPath = buildLockKey(lockParam);
        // for Reentrant
        Pair<Long, ByteSequence> pair = tl.get().get(lockPath);
        if (pair != null) {
            return;
        }

        ByteSequence key = ByteSequence.from(lockPath.getBytes());
        try {
            //apply lease
            int maxHoldSeconds = lockParam.getMaxHoldSeconds();
            long leaseId = leaseClient.grant(maxHoldSeconds)
                                      .get(lockParam.getTimeout(), lockParam.getTimeoutUnit()).getID();
            //apply lock
            LockResponse response = lockClient.lock(key, leaseId)
                                              .get(lockParam.getTimeout(), lockParam.getTimeoutUnit());
            //save key to thread cache
            tl.get().put(lockPath, Pair.of(leaseId, response.getKey()));

            //auto renew
            int renewDelay = maxHoldSeconds / 2;
            if (renewDelay > 0) {
                executorService.scheduleWithFixedDelay(
                    new RenewTask(leaseClient, leaseId), 0, renewDelay, TimeUnit.SECONDS);
            }

        } catch (TimeoutException te) {
            throw new LockException(LockExceptionCode.TIME_OUT);
        } catch (Exception e) {
            throw new LockException(LockExceptionCode.LOCK_FAILED);
        }
    }

    @Override
    public void release(LockParam lockParam) {

        String lockPath = buildLockKey(lockParam);
        Pair<Long, ByteSequence> pair = tl.get().get(lockPath);

        if (pair != null) {
            try {
                //release lock
                lockClient.unlock(pair.getRight()).get(lockParam.getTimeout(), lockParam.getTimeoutUnit());
            } catch (Exception e) {
                LOGGER.error("error in release lock,key={}", lockPath, e);
            } finally {
                tl.get().remove(lockPath);
                // clean lock node after 1S,if there is no others apply or hold on this path , this lock node will be  delete
                executorService.schedule(new CleanerTask(leaseClient, pair.getLeft()), 1L, TimeUnit.SECONDS);
            }
        }
    }

    private String buildLockKey(LockParam lockParam) {
        return rootPath + lockParam.getLockKey();
    }

    static class CleanerTask implements Runnable {
        private final Lease leaseClient;
        private final long leaseId;

        public CleanerTask(Lease leaseClient, long leaseId) {
            this.leaseClient = leaseClient;
            this.leaseId = leaseId;
        }

        public void run() {
            try {
                leaseClient.revoke(leaseId).get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                LOGGER.error("error in lock cleaner job,leaseId={}", leaseId, e);
            }
        }
    }

    static class RenewTask implements Runnable {
        private final Lease leaseClient;
        private final long leaseId;

        public RenewTask(Lease leaseClient, long leaseId) {
            this.leaseClient = leaseClient;
            this.leaseId = leaseId;
        }

        public void run() {
            try {
                leaseClient.keepAlive(leaseId, DISCARD_OBSERVER);
            } catch (Exception e) {
                LOGGER.error("error in lock renew job,leaseId={}", leaseId, e);
            }
        }
    }

    static class DiscardStreamObserver implements StreamObserver<LeaseKeepAliveResponse> {

        @Override
        public void onNext(LeaseKeepAliveResponse response) {
            LOGGER.debug("DiscardStreamObserver onNext:{}", response);
        }

        @Override
        public void onError(Throwable throwable) {
            LOGGER.error("DiscardStreamObserver onError");
        }

        @Override
        public void onCompleted() {
            LOGGER.debug("DiscardStreamObserver onCompleted");
        }
    }
}
