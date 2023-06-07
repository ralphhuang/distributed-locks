package io.github.ralphhuang.distrbute.locks.impl.etcd;

import io.etcd.jetcd.lease.LeaseKeepAliveResponse;
import io.grpc.stub.StreamObserver;

/**
 * @author huangfeitao
 * @version NoopStreamObserver.java 2023/6/7 15:50 create by: huangfeitao
 **/
public class NoopStreamObserver implements StreamObserver<LeaseKeepAliveResponse> {

    public static final NoopStreamObserver INSTANCE = new NoopStreamObserver();

    private NoopStreamObserver() {
    }

    @Override
    public void onNext(LeaseKeepAliveResponse value) {
    }

    @Override
    public void onError(Throwable t) {
    }

    @Override
    public void onCompleted() {
    }
}
