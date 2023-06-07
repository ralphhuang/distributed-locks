package io.github.ralphhuang.distrbute.locks.impl.etcd;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.Lock;
import io.etcd.jetcd.common.exception.ErrorCode;
import io.etcd.jetcd.common.exception.EtcdException;
import io.etcd.jetcd.lease.LeaseGrantResponse;
import io.etcd.jetcd.lease.LeaseKeepAliveResponse;
import io.etcd.jetcd.lease.LeaseRevokeResponse;
import io.etcd.jetcd.lock.LockResponse;
import io.etcd.jetcd.lock.UnlockResponse;
import io.github.ralphhuang.distrbute.locks.api.LockFacade;
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
 * @author huangfeitao
 * @version EtcdLock.java 2023/6/2 15:44 create by: huangfeitao
 **/
public class EtcdLock implements LockFacade {

    private static final Logger LOGGER = LoggerFactory.getLogger(EtcdLock.class);
    private static final String DEFAULT_ROOT = "/DISTRIBUTE-LOCKS/";
    private static final Long DEFAULT_TIMEOUT = 10L;

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
    private final Lock lockClient;
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

        String lockPath = buildLockKey(lockParam);
        final long timeout = lockParam.getTimeout();
        final TimeUnit timeoutUnit = lockParam.getTimeoutUnit();

        try {
            //lock reentrant
            if (isReentrant(lockPath, leaseClient, lockParam)) {
                LOGGER.debug("reentrant lock,path={}", lockPath);
                return;
            }

            //new lock process
            //apply lease
            LeaseGrantResponse leaseGrantResponse = applyLease(leaseClient, lockParam);
            long leaseId = leaseGrantResponse.getID();

            //start an auto renew task for every Lock lease
            submitLeaseRenewTask(leaseClient, leaseId, lockParam);

            //apply lock
            ByteSequence key = ByteSequence.from(lockPath.getBytes(StandardCharsets.UTF_8));
            StopWatch stopWatch = StopWatch.start();
            try {
                LockResponse response = lockClient.lock(key, leaseId).get(timeout, timeoutUnit);
                //save key to local thread cache,for lock reentrant
                tl.get().put(lockPath, Pair.of(leaseId, response.getKey()));
            } finally {
                LOGGER.debug("lock timeCost={}ms", stopWatch.get());
            }

        } catch (TimeoutException te) {
            throw new LockException(LockExceptionCode.TIME_OUT);
        } catch (Throwable e) {
            LOGGER.debug("lock error:", e);
            throw new LockException(LockExceptionCode.LOCK_FAILED);
        }
    }

    @Override
    public void release(LockParam lockParam) {

        String lockPath = buildLockKey(lockParam);
        final long timeout = lockParam.getTimeout();
        final TimeUnit timeoutUnit = lockParam.getTimeoutUnit();

        Pair<Long, ByteSequence> previousPair = tl.get().get(lockPath);

        if (previousPair != null) {
            StopWatch stopWatch = StopWatch.start();
            try {
                //release lock
                UnlockResponse response = lockClient.unlock(previousPair.getRight()).get(timeout, timeoutUnit);
                LOGGER.debug("release lock response={}", response);
            } catch (Exception e) {
                LOGGER.error("error in release lock,key={}", lockPath, e);
            } finally {
                tl.get().remove(lockPath);
                // clean lock node after 1S,if there is no others apply or hold on this path , this lock node will be  delete
                executorService.submit(new LeaseCleanTask(leaseClient, previousPair.getLeft()));
                LOGGER.debug("unlock timeCost={}ms", stopWatch.get());
            }
        }
    }

    private String buildLockKey(LockParam lockParam) {
        return rootPath + lockParam.getLockKey();
    }

    static class LeaseCleanTask implements Runnable {
        private final Lease leaseClient;
        private final long leaseId;

        public LeaseCleanTask(Lease leaseClient, long leaseId) {
            this.leaseClient = leaseClient;
            this.leaseId = leaseId;
        }

        public void run() {
            StopWatch stopWatch = StopWatch.start();
            try {
                LeaseRevokeResponse response = leaseClient.revoke(leaseId).get(DEFAULT_TIMEOUT, TimeUnit.SECONDS);
                LOGGER.debug("lock cleaner job response={},leaseId={}", response, leaseId);
            } catch (ExecutionException ignored) {
            } catch (Throwable e) {
                LOGGER.warn("error in lock cleaner job,leaseId={}", leaseId, e);
            } finally {
                LOGGER.debug("RenewalLeaseTask timeCost={}ms", stopWatch.get());
            }
        }
    }

    static class RenewalLeaseTask implements Runnable {
        private final Lease leaseClient;
        private final long leaseId;
        private final LockParam lockParam;

        public RenewalLeaseTask(Lease leaseClient, long leaseId, LockParam lockParam) {
            this.leaseClient = leaseClient;
            this.leaseId = leaseId;
            this.lockParam = lockParam;
        }

        public void run() {
            StopWatch stopWatch = StopWatch.start();
            try {
                renewalLeaseSilence(leaseClient, leaseId);
                submitLeaseRenewTask(leaseClient, leaseId, lockParam);
            } catch (Throwable t) {
                LOGGER.error("unExpected error in lock renew job,leaseId={}", leaseId, t);
            } finally {
                LOGGER.debug("RenewalLeaseTask timeCost={}ms", stopWatch.get());
            }
        }
    }

    private boolean isReentrant(String lockPath, Lease leaseClient, LockParam lockParam) throws Throwable {

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

        try {
            renewalLease(leaseClient, lockLeaseId, lockParam.getTimeout(), lockParam.getTimeoutUnit());
        } catch (EtcdException e) {
            if (e.getErrorCode() == ErrorCode.NOT_FOUND) {
                // renew failed,treat as a new lock
                tl.get().remove(lockPath);
                return false;
            }
        }
        return true;
    }

    private static LeaseGrantResponse applyLease(Lease leaseClient, LockParam lockParam) throws LockException {


        final int maxHoldSeconds = lockParam.getMaxHoldSeconds();
        final long timeout = lockParam.getTimeout();
        final TimeUnit timeoutUnit = lockParam.getTimeoutUnit();

        StopWatch stopWatch = StopWatch.start();
        try {
            LeaseGrantResponse response = leaseClient.grant(maxHoldSeconds).get(timeout, timeoutUnit);
            LOGGER.debug("applyLease response={}", response);
            if (response.getID() <= 0L) {
                throw new LockException(LockExceptionCode.LOCK_FAILED);
            }
            return response;
        } catch (Throwable t) {
            throw new LockException(LockExceptionCode.LOCK_FAILED);
        } finally {
            LOGGER.debug("applyLease timeCost={}ms", stopWatch.get());
        }
    }

    private static void submitLeaseRenewTask(Lease leaseClient, long leaseId, LockParam lockParam) {
        StopWatch stopWatch = StopWatch.start();
        try {
            int renewDelay = lockParam.getMaxHoldSeconds() / 2;
            renewDelay = renewDelay >= 1 ? renewDelay : lockParam.getMaxHoldSeconds();
            //run once
            executorService.schedule(
                new RenewalLeaseTask(leaseClient, leaseId, lockParam),
                renewDelay,
                TimeUnit.SECONDS
            );
        } finally {
            LOGGER.debug("submitLeaseRenewTask timeCost={}ms", stopWatch.get());
        }
    }

    private static void renewalLeaseSilence(Lease client, long leaseId) {
        try {
            renewalLease(client, leaseId, EtcdLock.DEFAULT_TIMEOUT, TimeUnit.SECONDS);
        } catch (Throwable expected) {
            // LOGGER.warn("expected error in lock renew job,leaseId={}", leaseId, expected);
        }
    }

    private static void renewalLease(Lease client, long leaseId, Long timeout, TimeUnit timeoutUnit)
        throws Throwable {
        StopWatch stopWatch = StopWatch.start();
        try {
            // Reentrant  renew lease
            LeaseKeepAliveResponse response = client.keepAliveOnce(leaseId).get(timeout, timeoutUnit);
            LOGGER.debug("renewalLease job response={},leaseId={}", response, leaseId);
            //must Renewal successful,if failed ,ttl will return -1
            if (response.getTTL() <= 0) {
                throw new LockException(LockExceptionCode.LOCK_FAILED);
            }
        } finally {
            LOGGER.debug("renewalLease timeCost={}ms", stopWatch.get());
        }
    }
}
