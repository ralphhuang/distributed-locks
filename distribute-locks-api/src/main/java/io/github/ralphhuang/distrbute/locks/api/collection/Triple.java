package io.github.ralphhuang.distrbute.locks.api.collection;

/**
 * @author huangfeitao
 * @version Triple.java 2023/3/31 17:02 create by: huangfeitao
 **/
public class Triple<L, M, R> {

    private final L left;
    private final M middle;
    private final R right;

    public Triple(L left, M middle, R right) {
        this.left = left;
        this.middle = middle;
        this.right = right;
    }

    public static <L, M, R> Triple<L, M, R> of(L left, M middle, R right) {
        return new Triple<>(left, middle, right);
    }

    public M getMiddle() {
        return middle;
    }

    public L getLeft() {
        return left;
    }

    public R getRight() {
        return right;
    }
}
