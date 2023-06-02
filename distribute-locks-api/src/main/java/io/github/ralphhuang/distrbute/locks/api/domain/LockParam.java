package io.github.ralphhuang.distrbute.locks.api.domain;

import java.util.concurrent.TimeUnit;

/**
 * @author huangfeitao
 * @version LockParam.java 2023/6/2 10:23 create by: huangfeitao
 **/
public class LockParam {

    /**
     * lockKey
     */
    private String lockKey;

    /**
     * the time to wait lock before timeout
     */
    private long timeout;

    /**
     * the time unit to wait lock before timeout
     */
    private TimeUnit timeoutUnit;

    /**
     * the time that one lock hold by one thread
     * if no renewal do by lock holder,the lock will be auto released
     * default is 60s
     * not support in Zookeeper
     * supported in Etcd、Redis
     */
    private int maxHoldSeconds;

    public LockParam(String lockKey) {
        this(lockKey, Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    public LockParam(String lockKey, long timeout, TimeUnit timeoutUnit) {
        this(lockKey, timeout, timeoutUnit, 60);
    }

    public LockParam(String lockKey, long timeout, TimeUnit timeoutUnit, int maxHoldSeconds) {
        this.lockKey = lockKey;
        this.timeout = timeout;
        this.timeoutUnit = timeoutUnit;
        this.maxHoldSeconds = maxHoldSeconds;
    }

    public static LockParam of(String lockKey) {
        return new LockParam(lockKey);
    }

    public static LockParam of(String lockKey, long timeout, TimeUnit timeoutUnit) {
        return new LockParam(lockKey, timeout, timeoutUnit);
    }

    public static LockParam of(String lockKey, long timeout, TimeUnit timeoutUnit, int maxHoldSeconds) {
        return new LockParam(lockKey, timeout, timeoutUnit);
    }

    public String getLockKey() {
        return lockKey;
    }

    public void setLockKey(String lockKey) {
        this.lockKey = lockKey;
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    public TimeUnit getTimeoutUnit() {
        return timeoutUnit;
    }

    public void setTimeoutUnit(TimeUnit timeoutUnit) {
        this.timeoutUnit = timeoutUnit;
    }

    public int getMaxHoldSeconds() {
        return maxHoldSeconds;
    }

    public void setMaxHoldSeconds(int maxHoldSeconds) {
        this.maxHoldSeconds = maxHoldSeconds;
    }

    @Override
    public String toString() {
        return "LockParam{" + "lockKey='" + lockKey + '\''
               + ", timeout=" + timeout
               + ", timeoutUnit=" + timeoutUnit
               + ", maxHoldSeconds=" + maxHoldSeconds
               + '}';
    }
}
