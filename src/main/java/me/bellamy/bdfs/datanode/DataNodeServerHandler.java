package me.bellamy.bdfs.datanode;

import io.netty.buffer.ByteBuf;

import io.netty.buffer.Unpooled;

import io.netty.channel.ChannelFutureListener;

import io.netty.channel.ChannelHandler;

import io.netty.channel.ChannelHandlerContext;

import io.netty.channel.ChannelInboundHandlerAdapter;

import io.netty.util.internal.StringUtil;
import me.bellamy.bdfs.proto_java.WriteMessage;

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
        ByteBuf byteBuf = (ByteBuf)msg;
        byte[] bytes = new byte[byteBuf.readableBytes()];
        int readerIndex = byteBuf.readerIndex();
        byteBuf.getBytes(readerIndex, bytes);

        WriteMessage.RequestWriteMessage writeMessage = WriteMessage.RequestWriteMessage.parseFrom(bytes);
        if (!StringUtil.isNullOrEmpty(writeMessage.getData())) {
            MappedByteBuffer out = new RandomAccessFile(writeMessage.getBlockId() + ".log", "rw").getChannel()
                    .map(FileChannel.MapMode.READ_WRITE, 0, length);
            out.put(writeMessage.getData().getBytes());
            mmap.put(writeMessage.getBlockId(), out);

            WriteMessage.ResponseWriteMessage.Builder builder = WriteMessage.ResponseWriteMessage.newBuilder();
            builder.setMessage("success");
            ctx.write(builder.build());
        } else {
            byte[] data = new byte[14];
            mmap.get(writeMessage.getBlockId()).position(0);
            mmap.get(writeMessage.getBlockId()).get(data);
            WriteMessage.ResponseWriteMessage.Builder builder = WriteMessage.ResponseWriteMessage.newBuilder();
            builder.setMessage(new String(data));
            ctx.write(builder.build());
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