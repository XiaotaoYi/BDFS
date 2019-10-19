package me.bellamy.bdfs.indexnode;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import me.bellamy.bdfs.indexnode.message.FileOperation;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;


@ChannelHandler.Sharable

public class IndexNodeServerHandler extends ChannelInboundHandlerAdapter {
    private static HashMap<String, List<Long>> indexToDataMap = new HashMap<String, List<Long>>();
    private static AtomicLong incrementer = new AtomicLong(0);

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf byteBuf = (ByteBuf)msg;
        byte[] bytes = new byte[byteBuf.readableBytes()];
        int readerIndex = byteBuf.readerIndex();
        byteBuf.getBytes(readerIndex, bytes);
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        ObjectInputStream in = new ObjectInputStream(byteArrayInputStream);
        FileOperation fileOperation = (FileOperation) in.readObject();
        in.close();

        if ("Open".equals(fileOperation.getOperation()) && "W".equals(fileOperation.getAction())) {
            long blockId = incrementer.incrementAndGet();
            List<Long> blockIds = new ArrayList<Long>();
            blockIds.add(blockId);
            indexToDataMap.put(fileOperation.getFileName(), blockIds);
            ctx.write("W|" + String.valueOf(blockId));
        } else if ("Open".equals(fileOperation.getOperation()) && "R".equals(fileOperation.getAction())) {
            List<Long> blockIds = indexToDataMap.get(fileOperation.getFileName());
            ctx.write("R|" + String.valueOf(blockIds.get(0)));
        }
    }

    @Override

    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {

        ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);

    }

    @Override

    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {

        cause.printStackTrace();

        ctx.close();

    }

    private static void sendMessage(Bootstrap bootstrap, EventLoopGroup group, Object message) throws Exception {
        try {
            ChannelFuture f = bootstrap.connect().sync();
            f.channel().writeAndFlush(message);
            f.channel().closeFuture().sync();
        } finally {
            group.shutdownGracefully().sync();
        }
    }

}