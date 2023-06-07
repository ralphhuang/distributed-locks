package io.github.ralphhuang.distrbute.locks.api;

import io.github.ralphhuang.distrbute.locks.api.domain.LockParam;
import io.github.ralphhuang.distrbute.locks.api.exception.LockException;

import java.util.concurrent.TimeUnit;

/**
 * Lock api
 * must be ThreadSafe
 *
 * @author huangfeitao
 * @version Lock.java 2023/6/2 09:54 create by: huangfeitao
 **/
public interface Lock {

    /**
     * Acquires the lock.
     * this method will block until two scenes occur.
     * 1: return true if get lock successfully.
     * 2：return false if get lock failed or internal error occur.
     *
     * @param lockKey the key represent the lock.
     * @return lock result.
     */
    boolean lock(String lockKey);

    /**
     * Acquires the lock with specific timeout.
     *
     * @param lockKey     the key represent the lock.
     * @param timeout     the max time to wait before get lock
     * @param timeoutUnit the max timeUnit to wait before get lock
     * @return lock result.
     */
    boolean tryLock(String lockKey, long timeout, TimeUnit timeoutUnit);

    /**
     * @param lockParam lock params
     * @throws LockException when lock failed
     */
    void lock(LockParam lockParam) throws LockException;

    /**
     * release lock with lockParam
     *
     * @param lockKey the key represent the lock.
     */
    void unlock(String lockKey);

}