package me.bellamy.bdfs.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import me.bellamy.bdfs.Constants;
import me.bellamy.bdfs.indexnode.message.FileOperation;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;

public class BdfsClient {
    private final String indexHost;
    private final int indexPort;

    private final String dataNodeHost;
    private final int dataNodePort;

    private EventLoopGroup indexNodeGroup;
    private Bootstrap indexNodeBootstrap;

    private String fileOperation;
    private String filePath;


    public BdfsClient(String indexHost, int indexPort, String dataNodeHost, int dataNodePort, String fileOperation, String filePath) {
        this.indexHost = indexHost;
        this.indexPort = indexPort;

        this.dataNodeHost = dataNodeHost;
        this.dataNodePort = dataNodePort;

        this.fileOperation = fileOperation;
        this.filePath = filePath;
    }

    public static void main(String[] args) throws Exception {
        if (args == null || args.length != 2) {
            throw new IllegalArgumentException("Length is not 2.");
        }

        String fileOperation = args[0];
        String filePath = args[1];

        BdfsClient bdfsClient = new BdfsClient(Constants.NAMENODE_HOST, Constants.NAMENODE_PORT, Constants.DATANODE_HOST, Constants.DATANODE_PORT, fileOperation, filePath);
        bdfsClient.initIndexNodeClient();
        bdfsClient.process();
    }

    private void initIndexNodeClient() throws Exception {
        this.indexNodeGroup = new NioEventLoopGroup();
        this.indexNodeBootstrap = new Bootstrap();
        this.indexNodeBootstrap.group(this.indexNodeGroup)
                .channel(NioSocketChannel.class)
                .remoteAddress(new InetSocketAddress(this.indexHost,this.indexPort))
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel socketChannel) throws Exception {
                        socketChannel.pipeline().addLast(new IndexNodeClientHandler());
                    }
                });
    }

    private void process() throws Exception {
        if ("upload".equals(this.fileOperation)) {
            processUploadFileOperation(this.filePath);
        }

        if ("download".equals(this.fileOperation)) {
            processDownloadFileOperation(this.filePath);
        }
    }

    private void processUploadFileOperation(String filePath) throws Exception {
        File file = new File(filePath);
        FileOperation fileOperation = new FileOperation();
        fileOperation.setOperation("Open");
        fileOperation.setFileName(file.getName());
        fileOperation.setAction("W");

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(byteArrayOutputStream);
        out.writeObject(fileOperation);
        out.close();

        sendMessage(this.indexNodeBootstrap, this.indexNodeGroup, byteArrayOutputStream.toByteArray());
    }

    private void processDownloadFileOperation(String filePath) throws Exception {
        File file = new File(filePath);
        FileOperation fileOperation = new FileOperation();
        fileOperation.setOperation("Open");
        fileOperation.setFileName(file.getName());
        fileOperation.setAction("R");

        sendMessage(this.indexNodeBootstrap, this.indexNodeGroup, fileOperation);
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
