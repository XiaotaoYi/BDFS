package me.bellamy.bdfs.datanode;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import me.bellamy.bdfs.Constants;

import java.net.InetSocketAddress;

public class DataNodeServer {
    private final int port;

    public DataNodeServer(int port){
        this.port = port;
    }


    public static void main(String[] args) throws Exception {
        //int port = Integer.parseInt(args[0]);

        new DataNodeServer(Constants.DATANODE_PORT).start();
    }



    public void start() throws Exception {

        final DataNodeServerHandler serverHandler = new DataNodeServerHandler();

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

                            socketChannel.pipeline().addLast(serverHandler);

                        }

                    });



            ChannelFuture f = bootstrap.bind().sync();

            f.channel().closeFuture().sync();

        } finally {

            bossGroup.shutdownGracefully().sync();

        }

    }
}
