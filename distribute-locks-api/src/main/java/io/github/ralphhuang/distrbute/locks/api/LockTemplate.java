package io.github.ralphhuang.distrbute.locks.api;

import io.github.ralphhuang.distrbute.locks.api.callback.LockCallback;
import io.github.ralphhuang.distrbute.locks.api.domain.LockParam;
import io.github.ralphhuang.distrbute.locks.api.exception.LockException;
import io.github.ralphhuang.distrbute.locks.api.util.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>lock operation template</p>
 *
 * @author huangfeitao
 * @version : LockTemplate.java, v 0.1 2021-04-01  上午09:40:19 huangfeitao Exp $
 */
public class LockTemplate {

    private static final Logger LOGGER = LoggerFactory.getLogger(LockTemplate.class);

    private final LockFacade lockImpl;

    public LockTemplate(LockFacade lockImpl) {
        this.lockImpl = lockImpl;
    }

    public void doWithCallback(final LockParam lockParam, LockCallback callback) {
        try {

            //get lock
            StopWatch stopWatch = StopWatch.start();
            LOGGER.debug("lock start,lockParam :{} ", lockParam);
            lockImpl.lock(lockParam);
            LOGGER.debug("lock success,lockParam :{},time cost:{}ms", lockParam, stopWatch.get());

            //run callback
            callback.onGetLock();

        } catch (LockException e) {
            callback.onException(e);
        } finally {
            //try to release lock
            lockImpl.release(lockParam);
        }
    }

}