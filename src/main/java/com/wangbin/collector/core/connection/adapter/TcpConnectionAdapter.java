package com.wangbin.collector.core.connection.adapter;

import com.wangbin.collector.common.domain.entity.DeviceInfo;
import com.wangbin.collector.common.domain.enums.ConnectionStatus;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * TCP连接适配器
 */
@Slf4j
public class TcpConnectionAdapter extends AbstractConnectionAdapter<Channel> {

    private Bootstrap bootstrap;
    private Channel channel;
    private EventLoopGroup workerGroup;
    private TcpClientHandler clientHandler;

    public TcpConnectionAdapter(DeviceInfo deviceInfo) {
        super(deviceInfo);
        initialize();
    }

    private void initialize() {
        this.workerGroup = new NioEventLoopGroup();
        this.clientHandler = new TcpClientHandler(this);

        this.bootstrap = new Bootstrap();
        this.bootstrap.group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.getConnectTimeout())
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();

                        // 添加空闲状态检测
                        pipeline.addLast(new IdleStateHandler(
                                config.getHeartbeatInterval() / 1000,
                                config.getHeartbeatInterval() / 1000,
                                config.getHeartbeatInterval() / 1000
                        ));

                        // 处理TCP粘包/拆包
                        pipeline.addLast(new LengthFieldBasedFrameDecoder(
                                config.getMaxFrameLength(),
                                0,
                                4,
                                0,
                                4
                        ));
                        pipeline.addLast(new LengthFieldPrepender(4));

                        // 添加自定义处理器
                        pipeline.addLast(clientHandler);
                    }
                });
    }

    @Override
    protected void doConnect() throws Exception {
        ChannelFuture future = bootstrap.connect(config.getHost(), config.getPort()).sync();
        this.channel = future.channel();

        // 等待连接完成
        if (!future.awaitUninterruptibly(config.getConnectTimeout())) {
            throw new Exception("TCP连接超时");
        }

        if (!future.isSuccess()) {
            try {
                throw future.cause();
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }

        log.info("TCP连接建立成功: {}:{}", config.getHost(), config.getPort());
    }

    @Override
    protected void doDisconnect() throws Exception {
        if (channel != null && channel.isActive()) {
            channel.close().sync();
            log.info("TCP连接断开成功: {}", deviceInfo != null ? deviceInfo.getDeviceId() : "UNKNOWN");
        }

        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
    }

    @Override
    protected void doSend(byte[] data) throws UnsupportedOperationException {
        try {
            if (channel == null || !channel.isActive()) {
                throw new IllegalStateException("TCP连接未激活");
            }

            ChannelFuture future = channel.writeAndFlush(data).sync();
            if (!future.isSuccess()) {
                try {
                    throw future.cause();
                } catch (Throwable e) {
                    throw new RuntimeException(e);
                }
            }
        } catch (Exception e) {
            throw new UnsupportedOperationException("TCP发送操作失败", e);
        }
    }

    @Override
    protected byte[] doReceive() throws UnsupportedOperationException {
        try {
            return clientHandler.receive(config.getReadTimeout());
        } catch (Exception e) {
            throw new UnsupportedOperationException("TCP接收操作失败", e);
        }
    }

    @Override
    protected byte[] doReceive(long timeout) throws UnsupportedOperationException {
        try {
            return clientHandler.receive(timeout);
        } catch (Exception e) {
            throw new UnsupportedOperationException("TCP接收操作失败", e);
        }
    }

    @Override
    protected void doHeartbeat() throws Exception {
        // TCP心跳通常是一个特定的心跳包
        byte[] heartbeatData = "HEARTBEAT".getBytes(StandardCharsets.UTF_8);
        doSend(heartbeatData);
    }

    @Override
    protected void doAuthenticate() throws Exception {
        // TCP认证通常是发送认证消息
        String authMessage = buildAuthMessage();
        doSend(authMessage.getBytes(StandardCharsets.UTF_8));

        // 等待认证响应
        byte[] response = doReceive(5000);
        if (response == null) {
            throw new Exception("TCP认证超时");
        }

        // 验证认证响应
        if (!verifyAuthResponse(response)) {
            throw new Exception("TCP认证失败");
        }

        log.info("TCP认证成功: {}", deviceInfo != null ? deviceInfo.getDeviceId() : "UNKNOWN");
    }

    /**
     * 构建认证消息
     */
    private String buildAuthMessage() {
        // 根据你的TCP认证协议构建消息
        // 这里是一个示例
        return String.format("AUTH|%s|%s|%s",
                config.getUsername(),
                config.getPassword(),
                System.currentTimeMillis()
        );
    }

    /**
     * 验证认证响应
     */
    private boolean verifyAuthResponse(byte[] response) {
        String responseStr = new String(response, StandardCharsets.UTF_8);
        return responseStr.startsWith("AUTH_SUCCESS");
    }

    @Override
    public Channel getClient() {
        return channel;
    }

    /**
     * TCP客户端处理器
     */
    private static class TcpClientHandler extends ChannelInboundHandlerAdapter {

        private final TcpConnectionAdapter adapter;
        private final BlockingQueue<byte[]> receiveQueue;
        private byte[] lastReceivedData;

        public TcpClientHandler(TcpConnectionAdapter adapter) {
            this.adapter = adapter;
            this.receiveQueue = new LinkedBlockingQueue<>();
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            log.debug("TCP通道激活: {}", adapter.getConnectionId());
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            log.debug("TCP通道关闭: {}", adapter.getConnectionId());
            // 通知连接断开
            adapter.status = ConnectionStatus.DISCONNECTED;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof byte[]) {
                byte[] data = (byte[]) msg;
                receiveQueue.offer(data);
                lastReceivedData = data;

                log.debug("收到TCP数据: {} bytes", data.length);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.error("TCP连接异常: {}", adapter.getConnectionId(), cause);
            ctx.close();
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
            // 处理空闲事件
            if (evt instanceof IdleStateEvent event) {
                switch (event.state()) {
                    case READER_IDLE:
                        log.debug("TCP读空闲: {}", adapter.getConnectionId());
                        break;
                    case WRITER_IDLE:
                        log.debug("TCP写空闲: {}", adapter.getConnectionId());
                        // 发送心跳
                        try {
                            adapter.heartbeat();
                        } catch (Exception e) {
                            log.error("TCP心跳发送失败", e);
                        }
                        break;
                    case ALL_IDLE:
                        log.debug("TCP读写空闲: {}", adapter.getConnectionId());
                        break;
                }
            }
        }

        /**
         * 接收数据
         */
        public byte[] receive(long timeout) throws InterruptedException {
            return receiveQueue.poll(timeout, TimeUnit.MILLISECONDS);
        }
    }
}
