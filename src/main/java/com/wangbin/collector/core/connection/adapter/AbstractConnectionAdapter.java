package com.wangbin.collector.core.connection.adapter;

import com.wangbin.collector.common.domain.enums.ConnectionStatus;
import com.wangbin.collector.common.utils.DateUtil;
import com.wangbin.collector.common.utils.JsonUtil;
import com.wangbin.collector.core.connection.model.ConnectionConfig;
import com.wangbin.collector.core.connection.model.ConnectionMetrics;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 抽象连接适配器
 */
@Slf4j
public abstract class AbstractConnectionAdapter implements ConnectionAdapter {

    protected ConnectionConfig config;
    protected ConnectionStatus status = ConnectionStatus.DISCONNECTED;
    protected ConnectionMetrics metrics;
    protected String connectionId;
    protected long lastActivityTime;
    protected Map<String, Object> connectionParams = new ConcurrentHashMap<>();
    protected Map<String, Object> statistics = new ConcurrentHashMap<>();

    // 统计计数器
    protected AtomicLong bytesSent = new AtomicLong(0);
    protected AtomicLong bytesReceived = new AtomicLong(0);
    protected AtomicLong messagesSent = new AtomicLong(0);
    protected AtomicLong messagesReceived = new AtomicLong(0);
    protected AtomicLong errors = new AtomicLong(0);
    protected AtomicLong heartbeats = new AtomicLong(0);

    public AbstractConnectionAdapter(ConnectionConfig config) {
        this.config = config;
        this.connectionId = generateConnectionId();
        this.metrics = new ConnectionMetrics();
        this.lastActivityTime = System.currentTimeMillis();
    }

    @Override
    public void connect() throws Exception {
        if (isConnected()) {
            log.warn("连接 {} 已经建立，无需重复连接", connectionId);
            return;
        }

        try {
            log.info("开始建立连接: {}", connectionId);
            status = ConnectionStatus.CONNECTING;

            // 执行实际连接逻辑（由子类实现）
            doConnect();

            status = ConnectionStatus.CONNECTED;
            lastActivityTime = System.currentTimeMillis();
            metrics.setConnectTime(lastActivityTime);
            metrics.setStatus(status);

            log.info("连接建立成功: {}", connectionId);
        } catch (Exception e) {
            status = ConnectionStatus.ERROR;
            metrics.setLastError(e.getMessage());
            errors.incrementAndGet();
            log.error("连接建立失败: {}", connectionId, e);
            throw e;
        }
    }

    @Override
    public void disconnect() throws Exception {
        if (!isConnected()) {
            log.warn("连接 {} 已经断开，无需重复断开", connectionId);
            return;
        }

        try {
            log.info("开始断开连接: {}", connectionId);

            // 执行实际断开逻辑（由子类实现）
            doDisconnect();

            status = ConnectionStatus.DISCONNECTED;
            metrics.setDisconnectTime(System.currentTimeMillis());
            metrics.setStatus(status);

            log.info("连接断开成功: {}", connectionId);
        } catch (Exception e) {
            log.error("连接断开失败: {}", connectionId, e);
            throw e;
        }
    }

    @Override
    public void reconnect() throws Exception {
        log.info("开始重连: {}", connectionId);

        try {
            // 先断开连接
            if (isConnected()) {
                disconnect();
            }

            // 等待重连间隔
            Thread.sleep(config.getReconnectDelay());

            // 重新连接
            connect();

            log.info("重连成功: {}", connectionId);
        } catch (Exception e) {
            status = ConnectionStatus.RECONNECTING;
            log.error("重连失败: {}", connectionId, e);
            throw e;
        }
    }

    @Override
    public void send(byte[] data) throws Exception {
        checkConnection();

        try {
            // 执行实际发送逻辑（由子类实现）
            doSend(data);

            // 更新统计
            bytesSent.addAndGet(data.length);
            messagesSent.incrementAndGet();
            lastActivityTime = System.currentTimeMillis();

            log.debug("发送数据成功: {}, 大小: {} bytes", connectionId, data.length);
        } catch (Exception e) {
            errors.incrementAndGet();
            metrics.setLastError(e.getMessage());
            log.error("发送数据失败: {}", connectionId, e);
            throw e;
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void send(String data) throws Exception {
        send(data.getBytes(config.getCharset()));
    }

    @Override
    public void send(Object data) throws Exception {
        String jsonData = JsonUtil.toJsonString(data);
        send(jsonData);
    }

    @Override
    public byte[] receive() throws Exception {
        checkConnection();

        try {
            // 执行实际接收逻辑（由子类实现）
            byte[] data = doReceive();

            // 更新统计
            if (data != null) {
                bytesReceived.addAndGet(data.length);
                messagesReceived.incrementAndGet();
                lastActivityTime = System.currentTimeMillis();
            }

            return data;
        } catch (Exception e) {
            errors.incrementAndGet();
            metrics.setLastError(e.getMessage());
            log.error("接收数据失败: {}", connectionId, e);
            throw e;
        }
    }

    @Override
    public String receiveAsString() throws Exception {
        byte[] data = receive();
        return data != null ? new String(data, config.getCharset()) : null;
    }

    @Override
    public byte[] receive(long timeout) throws Exception {
        checkConnection();

        try {
            // 执行带超时的接收逻辑（由子类实现）
            byte[] data = doReceive(timeout);

            // 更新统计
            if (data != null) {
                bytesReceived.addAndGet(data.length);
                messagesReceived.incrementAndGet();
                lastActivityTime = System.currentTimeMillis();
            }

            return data;
        } catch (Exception e) {
            errors.incrementAndGet();
            metrics.setLastError(e.getMessage());
            log.error("接收数据失败: {}", connectionId, e);
            throw e;
        }
    }

    @Override
    public void heartbeat() throws Exception {
        if (!isConnected()) {
            throw new IllegalStateException("连接未建立，无法发送心跳");
        }

        try {
            // 执行心跳逻辑（由子类实现）
            doHeartbeat();

            heartbeats.incrementAndGet();
            lastActivityTime = System.currentTimeMillis();
            metrics.setLastHeartbeatTime(lastActivityTime);

            log.debug("心跳发送成功: {}", connectionId);
        } catch (Exception e) {
            errors.incrementAndGet();
            metrics.setLastError(e.getMessage());
            log.error("心跳发送失败: {}", connectionId, e);
            throw e;
        }
    }

    @Override
    public void authenticate() throws Exception {
        if (!isConnected()) {
            throw new IllegalStateException("连接未建立，无法认证");
        }

        try {
            log.info("开始认证: {}", connectionId);
            status = ConnectionStatus.AUTHENTICATING;

            // 执行认证逻辑（由子类实现）
            doAuthenticate();

            status = ConnectionStatus.AUTHENTICATED;
            metrics.setAuthTime(System.currentTimeMillis());
            metrics.setStatus(status);

            log.info("认证成功: {}", connectionId);
        } catch (Exception e) {
            status = ConnectionStatus.ERROR;
            metrics.setLastError(e.getMessage());
            errors.incrementAndGet();
            log.error("认证失败: {}", connectionId, e);
            throw e;
        }
    }

    @Override
    public ConnectionStatus getStatus() {
        return status;
    }

    @Override
    public ConnectionConfig getConfig() {
        return config;
    }

    @Override
    public ConnectionMetrics getMetrics() {
        updateMetrics();
        return metrics;
    }

    @Override
    public boolean isConnected() {
        return status.isConnected();
    }

    @Override
    public boolean isAuthenticated() {
        return status == ConnectionStatus.AUTHENTICATED;
    }

    @Override
    public String getConnectionId() {
        return connectionId;
    }

    @Override
    public String getDeviceId() {
        return config.getDeviceId();
    }

    @Override
    public long getLastActivityTime() {
        return lastActivityTime;
    }

    @Override
    public void updateActivityTime() {
        this.lastActivityTime = System.currentTimeMillis();
    }

    @Override
    public Map<String, Object> getConnectionParams() {
        return connectionParams;
    }

    @Override
    public void setConnectionParam(String key, Object value) {
        connectionParams.put(key, value);
    }

    @Override
    public Map<String, Object> getStatistics() {
        statistics.put("bytesSent", bytesSent.get());
        statistics.put("bytesReceived", bytesReceived.get());
        statistics.put("messagesSent", messagesSent.get());
        statistics.put("messagesReceived", messagesReceived.get());
        statistics.put("errors", errors.get());
        statistics.put("heartbeats", heartbeats.get());
        statistics.put("lastActivityTime", DateUtil.formatDateTimeMs(new java.util.Date(lastActivityTime)));
        statistics.put("connectionDuration", System.currentTimeMillis() - metrics.getConnectTime());
        return statistics;
    }

    @Override
    public void resetStatistics() {
        bytesSent.set(0);
        bytesReceived.set(0);
        messagesSent.set(0);
        messagesReceived.set(0);
        errors.set(0);
        heartbeats.set(0);
        statistics.clear();
    }

    /**
     * 生成连接ID
     */
    protected String generateConnectionId() {
        String timestamp = String.valueOf(System.currentTimeMillis());
        String random = String.valueOf((int)(Math.random() * 10000));
        return "CONN_" + config.getDeviceId() + "_" + timestamp + "_" + random;
    }

    /**
     * 检查连接状态
     */
    protected void checkConnection() {
        if (!isConnected()) {
            throw new IllegalStateException("连接未建立或已断开");
        }
    }

    /**
     * 更新指标
     */
    protected void updateMetrics() {
        metrics.setBytesSent(bytesSent.get());
        metrics.setBytesReceived(bytesReceived.get());
        metrics.setMessagesSent(messagesSent.get());
        metrics.setMessagesReceived(messagesReceived.get());
        metrics.setErrors(errors.get());
        metrics.setHeartbeats(heartbeats.get());
        metrics.setLastActivityTime(lastActivityTime);
        metrics.setConnectionDuration(System.currentTimeMillis() - metrics.getConnectTime());

        // 计算成功率
        long totalMessages = messagesSent.get() + messagesReceived.get();
        if (totalMessages > 0) {
            double successRate = (totalMessages - errors.get()) * 100.0 / totalMessages;
            metrics.setSuccessRate(successRate);
        }
    }

    // 抽象方法，由子类实现
    protected abstract void doConnect() throws Exception;
    protected abstract void doDisconnect() throws Exception;
    protected abstract void doSend(byte[] data) throws Throwable;
    protected abstract byte[] doReceive() throws Exception;
    protected abstract byte[] doReceive(long timeout) throws Exception;
    protected abstract void doHeartbeat() throws Exception;
    protected abstract void doAuthenticate() throws Exception;
}
