package me.bellamy.bdfs.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import me.bellamy.bdfs.Constants;
import me.bellamy.bdfs.proto_java.FileOperation;
import me.bellamy.bdfs.proto_java.WriteMessage;

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
                        ChannelPipeline pipeline = socketChannel.pipeline();
                        pipeline.addLast(new ProtobufVarint32FrameDecoder());
                        //pipeline.addLast(new ProtobufDecoder(FileOperation.RequestFileOperation.getDefaultInstance()));
                        pipeline.addLast(new ProtobufVarint32LengthFieldPrepender());
                        pipeline.addLast(new ProtobufEncoder());
                        pipeline.addLast(new DataNodeClientHandler());
                    }
                });
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        //sctx.writeAndFlush(Unpooled.copiedBuffer("Netty rocks", CharsetUtil.UTF_8));
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf) throws Exception {
        byte[] bytes = new byte[byteBuf.readableBytes()];
        int readerIndex = byteBuf.readerIndex();
        byteBuf.getBytes(readerIndex, bytes);
        FileOperation.ResponseFileOperation responseFileOperation = FileOperation.ResponseFileOperation.parseFrom(bytes);

        if ("W".equals(responseFileOperation.getOperation())) {
            WriteMessage.RequestWriteMessage.Builder builder = WriteMessage.RequestWriteMessage.newBuilder();
            builder.setBlockId(responseFileOperation.getBlockId());
            builder.setData("Just a test!!!");
            sendMessage(dataNodeBootstrap, dataNodeGroup, builder.build());
        } else {
            WriteMessage.RequestWriteMessage.Builder builder = WriteMessage.RequestWriteMessage.newBuilder();
            builder.setBlockId(responseFileOperation.getBlockId());
            sendMessage(dataNodeBootstrap, dataNodeGroup, builder.build());
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