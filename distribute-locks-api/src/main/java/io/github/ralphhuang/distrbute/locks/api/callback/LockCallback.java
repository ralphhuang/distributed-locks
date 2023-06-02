package io.github.ralphhuang.distrbute.locks.api.callback;

import io.github.ralphhuang.distrbute.locks.api.exception.LockException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>callback</p>
 *
 * @author huangfeitao
 * @version : LockCallback.java, v 0.1 2021-04-01  上午18:51:46 huangfeitao Exp $
 */
public abstract class LockCallback {

    private static final Logger logger = LoggerFactory.getLogger(LockCallback.class);

    /**
     * method be called before get lock!
     */
    public void beforeLock() {}

    /**
     * method be called after get lock success!
     */
    public abstract void onGetLock();

    /**
     * method be called when meet exception wheel get lock!
     * default log error
     */
    public void onException(LockException e) {
        logger.error("getLock meet exception:{}", e != null ? e.getErrorCode() : "");
    }

}