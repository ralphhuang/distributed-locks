package io.github.ralphhuang.distrbute.locks.api.exception;

/**
 * <p>分布式锁异常</p>
 *
 * @author huangfeitao
 * @version : LockException.java, v 0.1 2021-04-01  上午18:58:08 huangfeitao Exp $
 */
public class LockException extends Exception {

    private final LockExceptionCode errorCode;

    public LockException(LockExceptionCode errorCode) {
        this.errorCode = errorCode;
    }

    public LockExceptionCode getErrorCode() {
        return errorCode;
    }

    public static LockException defaultCode() {
        return new LockException(LockExceptionCode.LOCK_FAILED);
    }
}