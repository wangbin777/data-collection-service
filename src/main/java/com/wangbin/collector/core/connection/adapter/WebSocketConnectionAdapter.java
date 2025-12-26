package com.wangbin.collector.core.connection.adapter;

import com.wangbin.collector.common.domain.enums.ConnectionStatus;
import com.wangbin.collector.core.connection.model.ConnectionConfig;
import lombok.extern.slf4j.Slf4j;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * WebSocket连接适配器（实现AutoCloseable接口用于资源清理）
 */
@Slf4j
public class WebSocketConnectionAdapter extends AbstractConnectionAdapter<WebSocket> implements AutoCloseable {

    private WebSocket webSocket;
    private HttpClient httpClient;
    private String wsUrl;
    private Map<String, String> customHeaders;
    private BlockingQueue<byte[]> messageQueue;
    private CompletableFuture<WebSocket> wsFuture;
    private ScheduledExecutorService heartbeatScheduler;
    private AtomicBoolean closing = new AtomicBoolean(false);

    // WebSocket监听器
    private final WebSocket.Listener listener = new WebSocket.Listener() {
        @Override
        public void onOpen(WebSocket webSocket) {
            WebSocket.Listener.super.onOpen(webSocket);
            log.info("WebSocket连接已打开: {}", connectionId);
            webSocket.request(1);
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            log.debug("收到WebSocket文本消息: {} chars", data.length());
            messageQueue.offer(data.toString().getBytes(StandardCharsets.UTF_8));
            webSocket.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onBinary(WebSocket webSocket, ByteBuffer data, boolean last) {
            byte[] bytes = new byte[data.remaining()];
            data.get(bytes);
            log.debug("收到WebSocket二进制消息: {} bytes", bytes.length);
            messageQueue.offer(bytes);
            webSocket.request(1);
            return null;
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            log.error("WebSocket发生错误: {}", connectionId, error);
            errors.incrementAndGet();
            status = ConnectionStatus.ERROR;
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            log.info("WebSocket连接关闭: {} (状态码: {}, 原因: {})",
                    connectionId, statusCode, reason);
            WebSocketConnectionAdapter.this.status = ConnectionStatus.DISCONNECTED;
            closing.set(false);
            return null;
        }
    };

    public WebSocketConnectionAdapter(ConnectionConfig config) {
        super(config);
        initialize();
    }

    private void initialize() {
        this.wsUrl = buildWebSocketUrl();
        this.customHeaders = getCustomHeaders();
        this.messageQueue = new LinkedBlockingQueue<>();
        this.heartbeatScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "websocket-heartbeat-" + config.getDeviceId());
            thread.setDaemon(true);
            return thread;
        });
        this.httpClient = createHttpClient();
    }

    private String buildWebSocketUrl() {
        // 优先使用url字段
        if (config.getUrl() != null && !config.getUrl().isEmpty()) {
            return config.getUrl();
        }

        // 否则使用host和port构建
        String protocol = Boolean.TRUE.equals(config.getSslEnabled()) ? "wss" : "ws";
        String path = config.getStringConfig("path", "/ws");

        // 确保path以/开头
        if (!path.startsWith("/")) {
            path = "/" + path;
        }

        // 添加查询参数
        String queryString = buildQueryString();
        if (!queryString.isEmpty()) {
            if (path.contains("?")) {
                path += "&" + queryString;
            } else {
                path += "?" + queryString;
            }
        }

        return String.format("%s://%s:%d%s",
                protocol,
                config.getHost(),
                config.getPort(),
                path);
    }

    private String buildQueryString() {
        Map<String, Object> queryParams = config.getMapConfig("queryParams");
        if (queryParams == null || queryParams.isEmpty()) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Object> entry : queryParams.entrySet()) {
            if (sb.length() > 0) {
                sb.append("&");
            }
            sb.append(entry.getKey())
                    .append("=")
                    .append(entry.getValue().toString());
        }
        return sb.toString();
    }

    private Map<String, String> getCustomHeaders() {
        Map<String, Object> headersMap = config.getMapConfig("headers");
        if (headersMap != null) {
            // 转换为String,String Map
            Map<String, String> result = new java.util.HashMap<>();
            for (Map.Entry<String, Object> entry : headersMap.entrySet()) {
                if (entry.getValue() != null) {
                    result.put(entry.getKey(), entry.getValue().toString());
                }
            }
            return result;
        }
        return new java.util.HashMap<>();
    }

    private HttpClient createHttpClient() {
        HttpClient.Builder builder = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(config.getConnectTimeout()));

        // 配置SSL
        if (Boolean.TRUE.equals(config.getSslEnabled())) {
            builder.sslContext(createTrustAllSSLContext());
        }

        return builder.build();
    }

    private SSLContext createTrustAllSSLContext() {
        try {
            TrustManager[] trustAllCerts = new TrustManager[] {
                    new X509TrustManager() {
                        public X509Certificate[] getAcceptedIssuers() { return null; }
                        public void checkClientTrusted(X509Certificate[] certs, String authType) { }
                        public void checkServerTrusted(X509Certificate[] certs, String authType) { }
                    }
            };

            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, trustAllCerts, new SecureRandom());
            return sslContext;
        } catch (Exception e) {
            log.error("创建SSL上下文失败", e);
            return null;
        }
    }

    @Override
    protected void doConnect() throws Exception {
        if (closing.get()) {
            throw new IllegalStateException("WebSocket正在关闭中");
        }

        try {
            log.info("开始连接WebSocket: {}", wsUrl);

            // 构建WebSocket连接
            HttpClient client = httpClient;
            if (client == null) {
                client = createHttpClient();
            }

            WebSocket.Builder wsBuilder = client.newWebSocketBuilder()
                    .connectTimeout(Duration.ofMillis(config.getConnectTimeout()));

            // 设置认证头
            if (config.getUsername() != null && config.getPassword() != null) {
                String auth = config.getUsername() + ":" + config.getPassword();
                String encoded = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));
                wsBuilder.header("Authorization", "Basic " + encoded);
            }

            // 设置Bearer令牌
            if (config.getAuthToken() != null) {
                wsBuilder.header("Authorization", "Bearer " + config.getAuthToken());
            }

            // 设置自定义头
            customHeaders.forEach(wsBuilder::header);

            // 设置WebSocket子协议
            String subprotocol = config.getStringConfig("subprotocol", "collector-v1");
            if (subprotocol != null && !subprotocol.isEmpty()) {
                wsBuilder.subprotocols(subprotocol);
            }

            // 建立连接
            this.wsFuture = wsBuilder.buildAsync(URI.create(wsUrl), listener);

            // 等待连接完成
            this.webSocket = wsFuture.get(config.getConnectTimeout(), TimeUnit.MILLISECONDS);

            if (webSocket == null || webSocket.isOutputClosed()) {
                throw new Exception("WebSocket连接失败");
            }

            // 等待连接真正建立
            Thread.sleep(100);

            // 启动心跳
            startHeartbeat();

            log.info("WebSocket连接建立成功: {}", connectionId);

        } catch (TimeoutException e) {
            throw new Exception("WebSocket连接超时", e);
        } catch (Exception e) {
            throw new Exception("WebSocket连接失败: " + e.getMessage(), e);
        }
    }

    @Override
    protected void doDisconnect() throws Exception {
        if (closing.compareAndSet(false, true)) {
            try {
                // 停止心跳
                stopHeartbeat();

                if (webSocket != null && !webSocket.isOutputClosed()) {
                    // 发送关闭帧
                    webSocket.sendClose(1000, "Normal closure")
                            .get(3, TimeUnit.SECONDS);
                }

                // 关闭WebSocket Future
                if (wsFuture != null && !wsFuture.isDone()) {
                    wsFuture.cancel(true);
                }

                log.info("WebSocket连接断开成功: {}", connectionId);

            } catch (Exception e) {
                log.warn("WebSocket断开连接异常", e);
                throw e;
            } finally {
                webSocket = null;
                wsFuture = null;
                messageQueue.clear();
                closing.set(false);
            }
        }
    }

    @Override
    protected void doSend(byte[] data) throws UnsupportedOperationException {
        if (webSocket == null || webSocket.isOutputClosed()) {
            throw new IllegalStateException("WebSocket连接未激活");
        }

        if (closing.get()) {
            throw new IllegalStateException("WebSocket正在关闭中");
        }

        try {
            boolean binaryMode = config.getBoolConfig("binaryMode", false);

            CompletableFuture<WebSocket> sendFuture;
            if (binaryMode) {
                ByteBuffer buffer = ByteBuffer.wrap(data);
                sendFuture = webSocket.sendBinary(buffer, true);
            } else {
                String text = new String(data, StandardCharsets.UTF_8);
                sendFuture = webSocket.sendText(text, true);
            }

            // 等待发送完成
            sendFuture.get(config.getWriteTimeout(), TimeUnit.MILLISECONDS);

            log.debug("WebSocket发送数据成功: {} bytes", data.length);

        } catch (TimeoutException e) {
            throw new UnsupportedOperationException("WebSocket发送超时: " + e.getMessage(), e);
        } catch (Exception e) {
            throw new UnsupportedOperationException("WebSocket发送失败: " + e.getMessage(), e);
        }
    }

    @Override
    protected byte[] doReceive() throws UnsupportedOperationException {
        try {
            return doReceive(config.getReadTimeout());
        } catch (Exception e) {
            throw new UnsupportedOperationException("WebSocket接收操作失败: " + e.getMessage(), e);
        }
    }

    @Override
    protected byte[] doReceive(long timeout) throws UnsupportedOperationException {
        try {
            return messageQueue.poll(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new UnsupportedOperationException("WebSocket接收消息被中断: " + e.getMessage(), e);
        }
    }

    @Override
    public WebSocket getClient() {
        return webSocket;
    }

    @Override
    protected void doHeartbeat() throws Exception {
        if (webSocket == null || webSocket.isOutputClosed()) {
            throw new IllegalStateException("WebSocket连接未激活");
        }

        if (closing.get()) {
            throw new IllegalStateException("WebSocket正在关闭中");
        }

        String heartbeatMessage = config.getStringConfig("heartbeatMessage", "ping");
        boolean usePing = config.getBoolConfig("heartbeatUsePing", true);

        try {
            CompletableFuture<WebSocket> heartbeatFuture;
            if (usePing) {
                ByteBuffer buffer = ByteBuffer.wrap(heartbeatMessage.getBytes(StandardCharsets.UTF_8));
                heartbeatFuture = webSocket.sendPing(buffer);
            } else {
                heartbeatFuture = webSocket.sendText(heartbeatMessage, true);
            }

            heartbeatFuture.get(5000, TimeUnit.MILLISECONDS);
            log.debug("WebSocket心跳发送成功: {}", connectionId);

        } catch (Exception e) {
            throw new Exception("WebSocket心跳发送失败: " + e.getMessage(), e);
        }
    }

    @Override
    protected void doAuthenticate() throws Exception {
        // WebSocket认证通常在握手时完成
        // 如果需要额外的认证，可以发送认证消息
        String authMessage = buildAuthMessage();
        boolean waitResponse = config.getBoolConfig("authWaitResponse", true);

        doSend(authMessage.getBytes(StandardCharsets.UTF_8));

        if (waitResponse) {
            // 等待认证响应
            byte[] response = doReceive(10000);
            if (response == null) {
                throw new Exception("WebSocket认证超时");
            }

            String responseStr = new String(response, StandardCharsets.UTF_8);
            if (!isAuthResponseValid(responseStr)) {
                throw new Exception("WebSocket认证失败: " + responseStr);
            }

            log.info("WebSocket认证成功: {}", config.getDeviceId());
        } else {
            log.info("WebSocket认证消息已发送: {}", config.getDeviceId());
        }
    }

    private String buildAuthMessage() {
        Map<String, Object> authData = new java.util.HashMap<>();
        authData.put("action", "auth");
        authData.put("deviceId", config.getDeviceId());
        authData.put("timestamp", System.currentTimeMillis());

        if (config.getUsername() != null) {
            authData.put("username", config.getUsername());
        }

        if (config.getPassword() != null) {
            authData.put("password", config.getPassword());
        }

        if (config.getProductKey() != null) {
            authData.put("productKey", config.getProductKey());
        }

        if (config.getDeviceSecret() != null) {
            authData.put("deviceSecret", config.getDeviceSecret());
        }

        // 添加额外的认证参数
        if (config.getAuthParams() != null) {
            authData.putAll(config.getAuthParams());
        }

        return com.alibaba.fastjson2.JSON.toJSONString(authData);
    }

    private boolean isAuthResponseValid(String response) {
        try {
            Map<String, Object> responseData = com.alibaba.fastjson2.JSON.parseObject(response, Map.class);
            Object status = responseData.get("status");
            return "success".equals(status) || "ok".equals(status) || "authenticated".equals(status);
        } catch (Exception e) {
            log.debug("解析认证响应失败", e);
            return response.contains("success") || response.contains("auth_success");
        }
    }

    private void startHeartbeat() {
        int heartbeatInterval = config.getHeartbeatInterval();
        if (heartbeatInterval > 0) {
            heartbeatScheduler.scheduleAtFixedRate(() -> {
                try {
                    if (isConnected() && !closing.get()) {
                        heartbeat();
                    }
                } catch (Exception e) {
                    log.warn("WebSocket心跳失败", e);
                }
            }, heartbeatInterval, heartbeatInterval, TimeUnit.MILLISECONDS);
            log.debug("WebSocket心跳任务已启动，间隔: {}ms", heartbeatInterval);
        }
    }

    private void stopHeartbeat() {
        if (heartbeatScheduler != null && !heartbeatScheduler.isShutdown()) {
            heartbeatScheduler.shutdown();
            try {
                if (!heartbeatScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    heartbeatScheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                heartbeatScheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
            log.debug("WebSocket心跳任务已停止");
        }
    }

    @Override
    public void setConnectionParam(String key, Object value) {
        super.setConnectionParam(key, value);
        if (value instanceof String) {
            customHeaders.put(key, (String) value);
        }
    }

    @Override
    public void close() {
        try {
            disconnect();
        } catch (Exception e) {
            log.error("关闭WebSocket连接异常", e);
        }
    }

    // ========== 辅助方法 ==========

    /**
     * 发送Ping消息
     */
    public CompletableFuture<WebSocket> sendPing(byte[] data) {
        if (webSocket == null || webSocket.isOutputClosed()) {
            return CompletableFuture.failedFuture(new IllegalStateException("WebSocket连接未激活"));
        }
        return webSocket.sendPing(ByteBuffer.wrap(data));
    }

    /**
     * 发送Pong消息
     */
    public CompletableFuture<WebSocket> sendPong(byte[] data) {
        if (webSocket == null || webSocket.isOutputClosed()) {
            return CompletableFuture.failedFuture(new IllegalStateException("WebSocket连接未激活"));
        }
        return webSocket.sendPong(ByteBuffer.wrap(data));
    }

    /**
     * 异步发送文本消息
     */
    public CompletableFuture<WebSocket> sendTextAsync(String text) {
        if (webSocket == null || webSocket.isOutputClosed()) {
            return CompletableFuture.failedFuture(new IllegalStateException("WebSocket连接未激活"));
        }
        return webSocket.sendText(text, true);
    }

    /**
     * 异步发送二进制消息
     */
    public CompletableFuture<WebSocket> sendBinaryAsync(byte[] data) {
        if (webSocket == null || webSocket.isOutputClosed()) {
            return CompletableFuture.failedFuture(new IllegalStateException("WebSocket连接未激活"));
        }
        return webSocket.sendBinary(ByteBuffer.wrap(data), true);
    }

    /**
     * 获取WebSocket连接状态
     */
    public boolean isWebSocketOpen() {
        return webSocket != null && !webSocket.isOutputClosed();
    }

    /**
     * 获取未处理的消息数量
     */
    public int getPendingMessageCount() {
        return messageQueue.size();
    }
}