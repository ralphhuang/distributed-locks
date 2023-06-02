package io.github.ralphhuang.distrbute.locks.api.util;

/**
 * timeWatch
 * 1：auto start
 * 2：not support pause
 *
 * @author huangfeitao
 * @version StopWatch.java 2023/3/23 11:32 create by: huangfeitao
 **/
public class StopWatch {

    private long start;

    public static StopWatch start() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start = System.currentTimeMillis();
        return stopWatch;
    }

    public long get() {
        return System.currentTimeMillis() - start;
    }

    public void resume() {
        this.start = System.currentTimeMillis();
    }

    private StopWatch() {

    }

}
