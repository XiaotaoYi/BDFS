package me.bellamy.bdfs.common;

import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;

public class DelayedOperation implements TimerTask {

    private ChannelHandlerContext ctx;

    private boolean hasCompleted = false;

    public ChannelHandlerContext getCtx() {
        return ctx;
    }

    public void setCtx(ChannelHandlerContext ctx) {
        this.ctx = ctx;
    }

    public void setHasCompleted(boolean hasCompleted) {
        this.hasCompleted = hasCompleted;
    }

    public void run(Timeout timeout) throws Exception {
        if (!hasCompleted) {
        }
    }
}
