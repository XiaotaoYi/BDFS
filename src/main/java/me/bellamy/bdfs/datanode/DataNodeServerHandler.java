package me.bellamy.bdfs.datanode;

import io.netty.buffer.ByteBuf;

import io.netty.buffer.Unpooled;

import io.netty.channel.ChannelFutureListener;

import io.netty.channel.ChannelHandler;

import io.netty.channel.ChannelHandlerContext;

import io.netty.channel.ChannelInboundHandlerAdapter;

import io.netty.util.CharsetUtil;
import me.bellamy.bdfs.datanode.message.NewBlockMessage;
import me.bellamy.bdfs.datanode.message.ReadMessage;
import me.bellamy.bdfs.datanode.message.WriteMessage;

import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;


@ChannelHandler.Sharable

public class DataNodeServerHandler extends ChannelInboundHandlerAdapter {

    private static int length = 0x8FFFFFF; // 128 MB
    private static HashMap<String, MappedByteBuffer> mmap = new HashMap<String, MappedByteBuffer>();

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf in = (ByteBuf) msg;
        String operation = in.toString(CharsetUtil.UTF_8);
        if (msg instanceof NewBlockMessage) {
        } else if (msg instanceof WriteMessage) {
            WriteMessage message = new WriteMessage();
            MappedByteBuffer out = new RandomAccessFile(message.getBlockId() + ".log", "rw").getChannel()
                    .map(FileChannel.MapMode.READ_WRITE, 0, length);
            out.put(message.getData().getBytes());
            mmap.put(message.getBlockId(), out);
            ctx.write("success");
        } else if (msg instanceof ReadMessage) {
            ReadMessage message = new ReadMessage();
            ctx.write(mmap.get(message).get());
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

}