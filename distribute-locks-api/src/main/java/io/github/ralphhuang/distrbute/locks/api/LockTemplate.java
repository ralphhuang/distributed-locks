package io.github.ralphhuang.distrbute.locks.api;

import io.github.ralphhuang.distrbute.locks.api.callback.LockCallback;
import io.github.ralphhuang.distrbute.locks.api.domain.LockParam;
import io.github.ralphhuang.distrbute.locks.api.exception.LockException;
import io.github.ralphhuang.distrbute.locks.api.util.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>lock operation template</p>
 * ThreadSafe
 *
 * @author huangfeitao
 * @version : LockTemplate.java, v 0.1 2021-04-01  上午09:40:19 huangfeitao Exp $
 */
public class LockTemplate {

    private static final Logger LOGGER = LoggerFactory.getLogger(LockTemplate.class);

    private final Lock lockImpl;

    public LockTemplate(Lock lockImpl) {
        this.lockImpl = lockImpl;
    }

    public void doWithCallback(final String lockKey, LockCallback callback) {

        LockParam lockParam = LockParam.of(lockKey);
        try {
            //before lock
            callback.beforeLock();

            //get lock
            lockImpl.lock(lockParam);

            //run callback
            callback.onGetLock();

        } catch (LockException e) {
            callback.onException(e);
        } finally {
            //try to release lock
            lockImpl.unlock(lockParam.getLockKey());
        }
    }

}