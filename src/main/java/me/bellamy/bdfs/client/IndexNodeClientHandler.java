package me.bellamy.bdfs.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.CharsetUtil;
import me.bellamy.bdfs.Constants;
import me.bellamy.bdfs.datanode.message.ReadMessage;
import me.bellamy.bdfs.datanode.message.WriteMessage;

import java.net.InetSocketAddress;

@ChannelHandler.Sharable
public class IndexNodeClientHandler extends SimpleChannelInboundHandler<ByteBuf> {
    private static EventLoopGroup dataNodeGroup;
    private static Bootstrap dataNodeBootstrap;

    static {
        dataNodeGroup = new NioEventLoopGroup();
        dataNodeBootstrap = new Bootstrap();
        dataNodeBootstrap.group(dataNodeGroup)
                .channel(NioSocketChannel.class)
                .remoteAddress(new InetSocketAddress(Constants.DATANODE_HOST, Constants.DATANODE_PORT))
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel socketChannel) throws Exception {
                        socketChannel.pipeline().addLast(new DataNodeClientHandler());
                    }
                });
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        ctx.writeAndFlush(Unpooled.copiedBuffer("Netty rocks", CharsetUtil.UTF_8));
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf) throws Exception {
        String[] res = byteBuf.toString(CharsetUtil.UTF_8).split("|");
        String operation = res[0];
        String blockId = res[1];

        if ("W".equals(operation)) {
            WriteMessage message = new WriteMessage();
            message.setBlockId(blockId);
            message.setData("Just a test!!!");
            sendMessage(dataNodeBootstrap, dataNodeGroup, message);
        } else {
            ReadMessage message = new ReadMessage();
            message.setBlockId(blockId);
            sendMessage(dataNodeBootstrap, dataNodeGroup, message);
        }

    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }

    private void sendMessage(Bootstrap bootstrap, EventLoopGroup group, Object message) throws Exception {
        try {
            ChannelFuture f = bootstrap.connect().sync();
            f.channel().writeAndFlush(message);
            f.channel().closeFuture().sync();
        } finally {
            group.shutdownGracefully().sync();
        }
    }
}