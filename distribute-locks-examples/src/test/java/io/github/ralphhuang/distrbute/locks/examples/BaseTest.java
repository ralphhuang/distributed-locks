package io.github.ralphhuang.distrbute.locks.examples;

import io.github.ralphhuang.distrbute.locks.api.Lock;
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

    private static final AtomicInteger G_ID = new AtomicInteger();
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
        Lock lockImpl
    ) {

        //多个线程持续竞争同一把锁
        LockParam lockParam = lockParamSupplier.get();
        LOGGER.info("lockFacade impl is {}", lockImpl);
        int groupId = G_ID.incrementAndGet();
        AtomicInteger T_ID = new AtomicInteger();

        for (int i = 0; i < threadNums; i++) {
            new Thread(() -> {
                while (true) {
                    try {
                        LOGGER.info("try lock");
                        lockImpl.lock(lockParam);
                        lockImpl.lock(lockParam);
                        LOGGER.info("lock success");
                    } catch (Exception e) {
                        LOGGER.error("lock error", e);
                        continue;
                    }

                    //mock hold lock for biz
                    sleep(supplier.get());

                    //release lock
                    try {
                        LOGGER.info("try unlock");
                        lockImpl.unlock(lockParam.getLockKey());
                        lockImpl.unlock(lockParam.getLockKey());
                        LOGGER.info("unlock success");
                    } catch (Exception e) {
                        LOGGER.error("unlock error");
                    }

                    //sleep(supplier.get());
                }

            }, "lock-apply-thread-" + groupId + "-" + T_ID.getAndIncrement()).start();
        }
    }

    protected void multiThreadForOneLockWithLockTemplate(
        int threadNums,
        Supplier<Integer> supplier,
        Supplier<LockParam> lockParamSupplier,
        Lock lockImpl
    ) {

        LockParam lockParam = lockParamSupplier.get();

        LOGGER.info("lockFacade impl is {}", lockImpl);
        LOGGER.info("lockParam is {}", lockParam);

        LockTemplate lockTemplate = new LockTemplate(lockImpl);

        for (int i = 0; i < threadNums; i++) {
            new Thread(() -> {

                while (true) {
                    lockTemplate.doWithCallback(lockParam.getLockKey(), new LockCallback() {

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
