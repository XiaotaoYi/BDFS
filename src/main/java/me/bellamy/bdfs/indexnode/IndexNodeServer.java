package me.bellamy.bdfs.indexnode;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import me.bellamy.bdfs.Constants;
import me.bellamy.bdfs.proto_java.FileOperation;

import java.net.InetSocketAddress;

public class IndexNodeServer {
    private final int port;

    public IndexNodeServer(int port){
        this.port = port;
    }

    public static void main(String[] args) throws Exception {
        new IndexNodeServer(Constants.NAMENODE_PORT).start();
    }

    public void start() throws Exception {

        final IndexNodeServerHandler serverHandler = new IndexNodeServerHandler();

        EventLoopGroup bossGroup = new NioEventLoopGroup(1);

        EventLoopGroup workGroup = new NioEventLoopGroup(2);

        try {

            ServerBootstrap bootstrap = new ServerBootstrap();

            bootstrap.group(bossGroup, workGroup)

                    .channel(NioServerSocketChannel.class)

                    .localAddress(new InetSocketAddress(port))

                    .childHandler(new ChannelInitializer<SocketChannel>() {

                        @Override

                        protected void initChannel(SocketChannel socketChannel) throws Exception {
                            ChannelPipeline pipeline = socketChannel.pipeline();
                            pipeline.addLast(new ProtobufVarint32FrameDecoder());
                            //pipeline.addLast(new ProtobufDecoder(FileOperation.RequestFileOperation.getDefaultInstance()));
                            pipeline.addLast(new ProtobufVarint32LengthFieldPrepender());
                            pipeline.addLast(new ProtobufEncoder());
                            pipeline.addLast(serverHandler);

                        }

                    });


            ChannelFuture f = bootstrap.bind().sync();

            f.channel().closeFuture().sync();

        } finally {

            bossGroup.shutdownGracefully().sync();

        }

    }
}
