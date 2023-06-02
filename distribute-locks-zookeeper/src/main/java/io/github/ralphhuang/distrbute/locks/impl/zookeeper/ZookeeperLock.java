package io.github.ralphhuang.distrbute.locks.impl.zookeeper;

import io.github.ralphhuang.distrbute.locks.api.LockFacade;
import io.github.ralphhuang.distrbute.locks.api.domain.LockParam;
import io.github.ralphhuang.distrbute.locks.api.exception.LockException;
import io.github.ralphhuang.distrbute.locks.api.exception.LockExceptionCode;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author huangfeitao
 * @version ZookeeperLock.java 2023/6/2 10:37 create by: huangfeitao
 **/
public class ZookeeperLock implements LockFacade {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperLock.class);
    private static final String DEFAULT_ROOT = "/DISTRIBUTE-LOCKS/";

    /**
     * executes for async tasks
     */
    private static final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(4);

    /**
     * all PERSISTENT nodes root path for locks
     * default is /DISTRIBUTE-LOCKS/
     */
    public String rootPath;

    private final CuratorFramework zkClient;

    /**
     * in one thread may apply several locks
     */
    private static final ThreadLocal<Map<String, InterProcessMutex>> tl = ThreadLocal.withInitial(HashMap::new);

    public ZookeeperLock(CuratorFramework zkClient) {
        this(null, zkClient);
    }

    public ZookeeperLock(String rootPath, CuratorFramework zkClient) {
        if (rootPath == null || rootPath.trim().isEmpty()) {
            rootPath = DEFAULT_ROOT;
        } else {
            if (!rootPath.startsWith("/")) {
                rootPath = "/" + rootPath;
            }
            if (!rootPath.endsWith("/")) {
                rootPath = rootPath + "/";
            }
        }
        this.rootPath = rootPath;
        this.zkClient = zkClient;
    }

    @Override
    public void lock(LockParam lockParam) throws LockException {

        String lockPath = buildLockPath(lockParam);

        // for Reentrant
        InterProcessMutex mutex = tl.get().get(lockPath);
        if (mutex == null) {
            mutex = new InterProcessMutex(zkClient, lockPath);
            tl.get().put(lockPath, mutex);
        }

        try {
            boolean locked = mutex.acquire(lockParam.getTimeout(), lockParam.getTimeoutUnit());
            if (!locked) {
                throw new LockException(LockExceptionCode.TIME_OUT);
            }
        } catch (Throwable e) {
            throw new LockException(LockExceptionCode.LOCK_FAILED);
        }
    }

    @Override
    public void release(LockParam lockParam) {

        String lockPath = buildLockPath(lockParam);

        InterProcessMutex mutex = tl.get().get(lockPath);
        if (mutex == null) {
            return;
        }
        try {
            mutex.release();
        } catch (IllegalMonitorStateException ignored) {
            // may not hold lock in current thread,ignored
        } catch (Throwable t) {
            // other zookeeper exceptions
            LOGGER.error("error in lock release:", t);
        } finally {
            tl.get().remove(lockPath);
            // clean lock node after 1S,if there is no others apply or hold on this path , this lock node will be  delete
            executorService.schedule(new CleanerTask(zkClient, buildLockPath(lockParam)), 1L, TimeUnit.SECONDS);
        }
    }

    private String buildLockPath(LockParam lockParam) {
        return rootPath + lockParam.getLockKey();
    }

    static class CleanerTask implements Runnable {
        private final CuratorFramework client;
        private final String path;

        public CleanerTask(CuratorFramework client, String path) {
            this.client = client;
            this.path = path;
        }

        public void run() {
            try {
                //delete lock path direct
                client.delete().forPath(path);
            } catch (KeeperException.NoNodeException | KeeperException.NotEmptyException ignore) {
                // those two Exceptions are expected,ignored!
                // NoNodeException: this occur when some other had already delete this path.
                // NotEmptyException: this occur where some one other hold or apply lock on this path.
            } catch (Exception e) {
                //zookeeper error
                LOGGER.error("error in lock cleaner job,path={}", path, e);
            }
        }

    }
}
