package io.github.ralphhuang.distrbute.locks.api;

import io.github.ralphhuang.distrbute.locks.api.domain.LockParam;
import io.github.ralphhuang.distrbute.locks.api.exception.LockException;

/**
 * Lock api
 *
 * @author huangfeitao
 * @version LockFacade.java 2023/6/2 09:54 create by: huangfeitao
 **/
public interface LockFacade {

    /**
     * @param lockParam
     * @throws LockException when lock failed
     */
    void lock(LockParam lockParam) throws LockException;

    /**
     * release lock with lockKey
     *
     * @param lockParam
     * @return
     */
    void release(LockParam lockParam);

}