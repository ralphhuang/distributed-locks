package io.github.ralphhuang.distrbute.locks.examples;

import io.github.ralphhuang.distrbute.locks.api.LockFacade;
import io.github.ralphhuang.distrbute.locks.api.LockTemplate;
import io.github.ralphhuang.distrbute.locks.api.callback.LockCallback;
import io.github.ralphhuang.distrbute.locks.api.domain.LockParam;
import io.github.ralphhuang.distrbute.locks.api.exception.LockException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * @author huangfeitao
 * @version BaseTest.java 2023/6/2 11:38 create by: huangfeitao
 **/
public class BaseTest {

    protected static final Logger LOGGER = LoggerFactory.getLogger(BaseTest.class);

    private static final AtomicInteger T_ID = new AtomicInteger();

    protected void pause() {
        sleep(Long.MAX_VALUE);
    }

    protected void sleep(long mis) {
        try {
            TimeUnit.MILLISECONDS.sleep(mis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * mock threadNums threads to run under those steps
     * 1: apply for lock A
     * 2: hold lock A
     * 3：release lock after lockHoldMis
     * <p>
     * do blow step in loop
     */
    protected void multiThreadForOneLock(
        int threadNums,
        Supplier<Integer> supplier,
        Supplier<LockParam> lockParamSupplier,
        LockFacade lockFacadeImpl
    ) {

        LockParam lockParam = lockParamSupplier.get();

        LOGGER.info("lockFacade impl is {}", lockFacadeImpl);
        LOGGER.info("lockParam is {}", lockParam);

        for (int i = 0; i < threadNums; i++) {
            new Thread(() -> {

                //while (true) {
                    try {
                        LOGGER.info("try lock");
                        lockFacadeImpl.lock(lockParam);
                        LOGGER.info("lock success");
                        LOGGER.info("try reLock");
                        lockFacadeImpl.lock(lockParam);
                        LOGGER.info("reLock success");
                    } catch (Exception e) {
                        LOGGER.error("lock error");
                    }

                    //mock hold lock for biz
                    sleep(supplier.get());

                    //release lock
                    try {
                        LOGGER.info("try unlock");
                        lockFacadeImpl.release(lockParam);
                        LOGGER.info("unlock success");
                        LOGGER.info("try unlock");
                        lockFacadeImpl.release(lockParam);
                        LOGGER.info("unlock success");
                    } catch (Exception e) {
                        LOGGER.error("unlock error");
                    }

                    sleep(supplier.get());
                //}

            }, "lock-apply-thread-" + T_ID.getAndIncrement()).start();
        }
    }

    protected void multiThreadForOneLockWithLockTemplate(
        int threadNums,
        Supplier<Integer> supplier,
        Supplier<LockParam> lockParamSupplier,
        LockFacade lockFacadeImpl
    ) {

        LockParam lockParam = lockParamSupplier.get();

        LOGGER.info("lockFacade impl is {}", lockFacadeImpl);
        LOGGER.info("lockParam is {}", lockParam);

        LockTemplate lockTemplate = new LockTemplate(lockFacadeImpl);

        for (int i = 0; i < threadNums; i++) {
            new Thread(() -> {

                while (true) {
                    lockTemplate.doWithCallback(lockParam, new LockCallback() {

                        @Override
                        public void beforeLock() {
                            LOGGER.info("try lock");
                        }

                        @Override
                        public void onGetLock() {
                            LOGGER.info("lock success");
                        }

                        @Override
                        public void onException(LockException e) {
                            LOGGER.error("lock error");
                        }
                    });

                    sleep(supplier.get());
                }

            }, "lock-apply-thread-" + T_ID.getAndIncrement()).start();
        }
    }

}
