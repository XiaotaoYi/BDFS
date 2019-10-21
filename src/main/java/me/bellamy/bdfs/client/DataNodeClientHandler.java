package me.bellamy.bdfs.client;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import me.bellamy.bdfs.proto_java.WriteMessage;

@ChannelHandler.Sharable
public class DataNodeClientHandler extends SimpleChannelInboundHandler<ByteBuf> {
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        //ctx.writeAndFlush(Unpooled.copiedBuffer("Netty rocks", CharsetUtil.UTF_8));
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf) throws Exception {
        //System.out.println("Client received: " + byteBuf.toString(CharsetUtil.UTF_8));
        byte[] bytes = new byte[byteBuf.readableBytes()];
        int readerIndex = byteBuf.readerIndex();
        byteBuf.getBytes(readerIndex, bytes);

        WriteMessage.ResponseWriteMessage writeMessage = WriteMessage.ResponseWriteMessage.parseFrom(bytes);
        System.out.println("write/read " + writeMessage.getMessage());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }
}