package io.github.ralphhuang.distrbute.locks.api;

import io.github.ralphhuang.distrbute.locks.api.domain.LockParam;
import io.github.ralphhuang.distrbute.locks.api.exception.LockException;

import java.util.concurrent.TimeUnit;

/**
 * @author huangfeitao
 * @version AbstractLock.java 2023/6/7 14:26 create by: huangfeitao
 **/
public abstract class AbstractLock implements Lock {

    @Override
    public boolean lock(String lockKey) {
        return tryLock(lockKey, Integer.MAX_VALUE, TimeUnit.SECONDS);
    }

    @Override
    public boolean tryLock(String lockKey, long timeout, TimeUnit timeoutUnit) {
        try {
            LockParam lockParam = LockParam.of(lockKey, timeout, timeoutUnit);
            lock(lockParam);
            return true;
        } catch (LockException e) {
            return false;
        }
    }
}
