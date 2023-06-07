package io.github.ralphhuang.distrbute.locks.impl.etcd;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.lease.LeaseGrantResponse;
import io.etcd.jetcd.lease.LeaseRevokeResponse;
import io.etcd.jetcd.lock.LockResponse;
import io.etcd.jetcd.lock.UnlockResponse;
import io.github.ralphhuang.distrbute.locks.api.AbstractLock;
import io.github.ralphhuang.distrbute.locks.api.Lock;
import io.github.ralphhuang.distrbute.locks.api.collection.Pair;
import io.github.ralphhuang.distrbute.locks.api.domain.LockParam;
import io.github.ralphhuang.distrbute.locks.api.exception.LockException;
import io.github.ralphhuang.distrbute.locks.api.exception.LockExceptionCode;
import io.github.ralphhuang.distrbute.locks.api.util.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * distribute lock by EtcdV3
 * reentrant support
 *
 * @author huangfeitao
 * @version EtcdLock.java 2023/6/2 15:44 create by: huangfeitao
 **/
public class EtcdLock extends AbstractLock implements Lock {

    private static final Logger LOGGER = LoggerFactory.getLogger(EtcdLock.class);
    private static final String DEFAULT_ROOT = "/DISTRIBUTE-LOCKS/";
    private static final Long DEFAULT_TIMEOUT = 10L;
    private static final Long ERROR_LEASE = -1L;

    /**
     * executes for async tasks
     */
    private static final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(4);

    /**
     * in one thread may apply several locks
     */
    private static final ThreadLocal<Map<String, Pair<Long, ByteSequence>>> tl = ThreadLocal.withInitial(HashMap::new);

    /**
     * all PERSISTENT nodes root path for locks
     * default is /DISTRIBUTE-LOCKS/
     */
    public String rootPath;
    private final io.etcd.jetcd.Lock lockClient;
    private final Lease leaseClient;

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

    @Override
    public void lock(LockParam lockParam) throws LockException {

        String lockPath = buildLockPath(lockParam.getLockKey());
        final long timeout = lockParam.getTimeout();
        final TimeUnit timeoutUnit = lockParam.getTimeoutUnit();

        long timeoutRemainMis = timeoutUnit.toMillis(timeout);
        StopWatch stopWatch = StopWatch.start();

        long leaseId = ERROR_LEASE;
        try {
            //lock reentrant
            if (isReentrant(lockPath)) {
                return;
            }

            //new lock process
            //apply lease
            LeaseGrantResponse leaseGrantResponse = applyLease(leaseClient, lockParam, true);
            leaseId = leaseGrantResponse.getID();

            //check timeout
            timeoutRemainMis -= stopWatch.get();
            if (timeoutRemainMis <= 0L) {
                throw new LockException(LockExceptionCode.TIME_OUT);
            }

            //apply lock
            ByteSequence key = ByteSequence.from(lockPath.getBytes(StandardCharsets.UTF_8));
            LockResponse response = lockClient.lock(key, leaseId).get(timeoutRemainMis, TimeUnit.MILLISECONDS);
            //save key to local thread cache,for lock reentrant
            tl.get().put(lockPath, Pair.of(leaseId, response.getKey()));

        } catch (Throwable e) {
            if (ERROR_LEASE != leaseId && leaseId > 0L) {
                executorService.submit(new LeaseCleanTask(leaseClient, leaseId));
            }
            throw new LockException(e, e instanceof TimeoutException ? LockExceptionCode.TIME_OUT
                                                                     : LockExceptionCode.LOCK_FAILED);
        }
    }

    @Override
    public void unlock(String lockKey) {
        String lockPath = buildLockPath(lockKey);

        Pair<Long, ByteSequence> previousPair = tl.get().get(lockPath);
        if (previousPair != null) {
            try {
                //release lock
                UnlockResponse response = lockClient.unlock(previousPair.getRight())
                                                    .get(DEFAULT_TIMEOUT, TimeUnit.SECONDS);
                LOGGER.debug("release lock response={}", response);
            } catch (Exception e) {
                LOGGER.error("error in release lock,key={}", lockPath, e);
            } finally {
                tl.get().remove(lockPath);
                // clean lock node after 1S,if there is no others apply or hold on this path , this lock node will be  delete
                executorService.submit(new LeaseCleanTask(leaseClient, previousPair.getLeft()));
            }
        }
    }

    private String buildLockPath(String lockKey) {
        return rootPath + lockKey;
    }

    static class LeaseCleanTask implements Runnable {
        private final Lease leaseClient;
        private final long leaseId;

        public LeaseCleanTask(Lease leaseClient, long leaseId) {
            this.leaseClient = leaseClient;
            this.leaseId = leaseId;
        }

        public void run() {
            try {
                LeaseRevokeResponse response = leaseClient.revoke(leaseId).get(DEFAULT_TIMEOUT, TimeUnit.SECONDS);
                LOGGER.debug("lock cleaner job response={},leaseId={}", response, leaseId);
            } catch (ExecutionException ignored) {
            } catch (Throwable e) {
                LOGGER.warn("error in lock cleaner job,leaseId={}", leaseId, e);
            }
        }
    }

    private boolean isReentrant(String lockPath) throws Throwable {

        Pair<Long, ByteSequence> previousPair = tl.get().get(lockPath);
        if (previousPair == null) {
            return false;
        }

        // check leaseId,must not be null
        Long lockLeaseId = previousPair.getLeft();
        if (lockLeaseId == null) {
            //may never happen
            LOGGER.error("Reentrant path:{},can't find previous lease info.", lockPath);
            throw new LockException(LockExceptionCode.LOCK_FAILED);
        }

        //lease will auto renew,so if reentrant,just return
        return true;
    }

    private static LeaseGrantResponse applyLease(Lease leaseClient, LockParam lockParam, boolean autoRenew)
        throws LockException {

        final int maxHoldSeconds = lockParam.getMaxHoldSeconds();
        try {
            LeaseGrantResponse response = leaseClient.grant(maxHoldSeconds).get(DEFAULT_TIMEOUT, TimeUnit.SECONDS);
            LOGGER.debug("applyLease response={}", response);
            if (response.getID() <= 0L) {
                throw new LockException(LockExceptionCode.LOCK_FAILED);
            }

            if (autoRenew) {
                //auto renew lease
                leaseClient.keepAlive(response.getID(), NoopStreamObserver.INSTANCE);
            }

            return response;
        } catch (Throwable t) {
            throw new LockException(LockExceptionCode.LOCK_FAILED);
        }
    }

}
