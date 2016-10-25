package com.weibo.api.motan.cluster.loadbalance;

/**
 * Created by alvin.
 */
public class SeqSessionCalc {
    /**
     * Don't let anyone instantiate this class.
     */
    private SeqSessionCalc() {
        // This constructor is intentionally empty.
    }

    public static long calcSeqSession(final long userId) {
        return userId % 10000;
    }
}
