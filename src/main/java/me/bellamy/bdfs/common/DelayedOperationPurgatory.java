package me.bellamy.bdfs.common;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

public class DelayedOperationPurgatory {
    private final static Timer timer = new HashedWheelTimer();
    private final static HashMap<String, DelayedOperation> delayedOperationHashMap = new HashMap<String, DelayedOperation>();

    public static void addOperation(String delayedOperationKey, DelayedOperation delayedOperation) {
        delayedOperationHashMap.put(delayedOperationKey, delayedOperation);
        timer.newTimeout(delayedOperation, 1000, TimeUnit.SECONDS);
    }

    public static void removeOperation(String delayedOperationKey) {

    }
}
