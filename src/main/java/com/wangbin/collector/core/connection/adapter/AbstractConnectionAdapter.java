package com.wangbin.collector.core.connection.adapter;

import com.wangbin.collector.common.domain.enums.ConnectionStatus;
import com.wangbin.collector.common.utils.DateUtil;
import com.wangbin.collector.common.utils.JsonUtil;
import com.wangbin.collector.core.connection.model.ConnectionConfig;
import com.wangbin.collector.core.connection.model.ConnectionMetrics;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 抽象连接适配器
 * @param <C> 协议客户端类型
 */
@Slf4j
public abstract class AbstractConnectionAdapter<C> implements ConnectionAdapter<C> {

    protected ConnectionConfig config;
    // 将status字段从private改为protected，以便子类可以访问
    protected volatile ConnectionStatus status = ConnectionStatus.DISCONNECTED;
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

    // 重连相关的成员变量
    private int reconnectAttempts = 0; // 当前重连尝试次数
    private long currentReconnectDelay; // 当前重连延迟
    private boolean reconnecting = false; // 是否正在重连

    public AbstractConnectionAdapter(ConnectionConfig config) {
        this.config = config;
        this.connectionId = generateConnectionId();
        this.metrics = new ConnectionMetrics();
        this.lastActivityTime = System.currentTimeMillis();
        // 初始化重连延迟
        this.currentReconnectDelay = config.getInitialReconnectDelay();
    }

    @Override
    public void connect() throws Exception {
        log.info("开始连接: {}", connectionId);

        // 检查当前状态
        if (status == ConnectionStatus.CONNECTED) {
            log.warn("连接已经建立: {}", connectionId);
            return;
        }
        if (status == ConnectionStatus.CONNECTING) {
            log.warn("连接正在建立中: {}", connectionId);
            return;
        }

        try {
            // 更新状态为正在连接
            status = ConnectionStatus.CONNECTING;
            metrics.setStatus(status);

            // 执行实际的连接操作
            doConnect();

            // 连接成功
            status = ConnectionStatus.CONNECTED;
            lastActivityTime = System.currentTimeMillis();
            metrics.setConnectTime(lastActivityTime);
            metrics.setStatus(status);
            metrics.setLastError(null);

            log.info("连接成功: {}", connectionId);
        } catch (Exception e) {
            // 连接失败
            status = ConnectionStatus.ERROR;
            metrics.setLastError(e.getMessage());
            errors.incrementAndGet();
            log.error("连接失败: {}", connectionId, e);
            throw e;
        }
    }

    @Override
    public void disconnect() throws Exception {
        log.info("开始断开连接: {}", connectionId);

        // 检查当前状态
        if (status == ConnectionStatus.DISCONNECTED) {
            log.warn("连接已经断开: {}", connectionId);
            return;
        }
        if (status == ConnectionStatus.CONNECTING || status == ConnectionStatus.RECONNECTING) {
            log.warn("连接正在建立中，无法立即断开: {}", connectionId);
            return;
        }

        try {
            // 更新状态为断开中（使用RECONNECTING作为临时状态）
            status = ConnectionStatus.RECONNECTING;
            metrics.setStatus(status);

            // 执行实际的断开操作
            doDisconnect();

            // 断开成功
            status = ConnectionStatus.DISCONNECTED;
            metrics.setStatus(status);
            metrics.setLastError(null);

            // 重置重连计数器和延迟
            reconnectAttempts = 0;
            currentReconnectDelay = config.getInitialReconnectDelay();

            log.info("断开连接成功: {}", connectionId);
        } catch (Exception e) {
            // 断开失败
            status = ConnectionStatus.ERROR;
            metrics.setLastError(e.getMessage());
            errors.incrementAndGet();
            log.error("断开连接失败: {}", connectionId, e);
            throw e;
        }
    }

    @Override
    public void reconnect() throws Exception {
        log.info("开始重连: {}", connectionId);

        // 检查是否正在重连
        if (reconnecting) {
            log.warn("重连正在进行中: {}", connectionId);
            return;
        }

        // 检查是否达到最大重连尝试次数
        if (config.getMaxReconnectAttempts() > 0 && reconnectAttempts >= config.getMaxReconnectAttempts()) {
            log.error("已达到最大重连尝试次数 {}，停止重连: {}", config.getMaxReconnectAttempts(), connectionId);
            status = ConnectionStatus.DISCONNECTED;
            metrics.setStatus(status);
            throw new Exception("已达到最大重连尝试次数");
        }

        try {
            reconnecting = true;
            status = ConnectionStatus.RECONNECTING;

            // 先断开连接
            if (isConnected()) {
                disconnect();
            }

            // 等待重连延迟（指数退避）
            long delay = calculateReconnectDelay();
            log.info("等待 {} 毫秒后重连: {}", delay, connectionId);
            Thread.sleep(delay);

            // 重新连接
            doConnect();

            // 连接成功，重置重连计数器和延迟
            reconnectAttempts = 0;
            currentReconnectDelay = config.getInitialReconnectDelay();
            status = ConnectionStatus.CONNECTED;
            lastActivityTime = System.currentTimeMillis();
            metrics.setConnectTime(lastActivityTime);
            metrics.setStatus(status);
            metrics.setLastError(null);

            log.info("重连成功: {}", connectionId);
        } catch (Exception e) {
            // 连接失败，更新重连计数器和延迟
            reconnectAttempts++;
            updateReconnectDelay();
            
            status = ConnectionStatus.ERROR;
            metrics.setLastError(e.getMessage());
            errors.incrementAndGet();
            log.error("重连失败 (尝试 {}): {}", reconnectAttempts, connectionId, e);
            throw e;
        } finally {
            reconnecting = false;
        }
    }

    /**
     * 计算重连延迟（指数退避算法）
     */
    private long calculateReconnectDelay() {
        // 如果是第一次重连，使用初始延迟
        if (reconnectAttempts == 0) {
            return currentReconnectDelay;
        }

        // 添加随机抖动（±10%）
        double jitter = 0.9 + Math.random() * 0.2;
        return (long) (currentReconnectDelay * jitter);
    }

    /**
     * 更新重连延迟（指数增长）
     */
    private void updateReconnectDelay() {
        // 计算新的延迟：currentDelay * multiplier
        long newDelay = (long) (currentReconnectDelay * config.getReconnectBackoffMultiplier());
        // 确保不超过最大延迟
        currentReconnectDelay = Math.min(newDelay, config.getMaxReconnectDelay());
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
        log.debug("执行心跳检查: {}", connectionId);

        // 检查连接状态
        if (!isConnected()) {
            log.warn("连接未建立，跳过心跳检查: {}", connectionId);
            return;
        }

        try {
            // 执行心跳操作
            doHeartbeat();

            // 更新心跳统计
            heartbeats.incrementAndGet();
            lastActivityTime = System.currentTimeMillis();
            metrics.setLastHeartbeatTime(lastActivityTime);
            metrics.setLastActivityTime(lastActivityTime);

            log.debug("心跳检查成功: {}", connectionId);
        } catch (Exception e) {
            // 心跳失败，记录错误并尝试重连
            log.error("心跳检查失败: {}", connectionId, e);
            errors.incrementAndGet();
            metrics.setLastError(e.getMessage());
            metrics.setLastErrorTime(System.currentTimeMillis());

            // 如果启用了自动重连，触发重连
            if (config.isAutoReconnect()) {
                try {
                    reconnect();
                } catch (Exception re) {
                    // 重连失败，更新状态
                    status = ConnectionStatus.ERROR;
                    metrics.setStatus(status);
                    log.error("心跳失败后重连也失败: {}", connectionId, re);
                }
            } else {
                // 未启用自动重连，更新状态为错误
                status = ConnectionStatus.ERROR;
                metrics.setStatus(status);
            }
        }
    }

    /**
     * 执行连接健康检查
     * @return 连接是否健康
     */
    public boolean healthCheck() {
        log.debug("执行连接健康检查: {}", connectionId);

        // 检查连接状态
        if (status != ConnectionStatus.CONNECTED) {
            log.warn("连接状态不正常，健康检查失败: {}", connectionId);
            return false;
        }

        // 检查空闲时间是否过长
        long idleTime = System.currentTimeMillis() - lastActivityTime;
        if (config.getHeartbeatTimeout() > 0 && idleTime > config.getHeartbeatTimeout()) {
            log.warn("连接空闲时间过长 ({} 毫秒 > {} 毫秒)，健康检查失败: {}", 
                    idleTime, config.getHeartbeatTimeout(), connectionId);
            return false;
        }

        // 执行心跳检查
        try {
            doHeartbeat();
            log.debug("连接健康检查成功: {}", connectionId);
            return true;
        } catch (Exception e) {
            log.error("连接健康检查失败: {}", connectionId, e);
            return false;
        }
    }

    /**
     * 验证连接状态，如果连接已断开则尝试重连
     * @throws Exception 如果重连失败
     */
    protected void validateConnection() throws Exception {
        if (!isConnected()) {
            if (config.isAutoReconnect()) {
                log.info("连接已断开，尝试自动重连: {}", connectionId);
                reconnect();
            } else {
                throw new Exception("连接已断开");
            }
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
    public synchronized Map<String, Object> getStatistics() {
        Map<String, Object> statistics = new HashMap<>();
        statistics.put("connectionId", connectionId);
        statistics.put("status", status.name());
        statistics.put("bytesSent", bytesSent.get());
        statistics.put("bytesReceived", bytesReceived.get());
        statistics.put("messagesSent", messagesSent.get());
        statistics.put("messagesReceived", messagesReceived.get());
        statistics.put("errors", errors.get());
        statistics.put("heartbeats", heartbeats.get());
        statistics.put("lastActivityTime", lastActivityTime);
        statistics.put("connectionDuration", status == ConnectionStatus.CONNECTED ? 
                System.currentTimeMillis() - metrics.getConnectTime() : 0);
        statistics.put("reconnectAttempts", reconnectAttempts);
        statistics.put("currentReconnectDelay", currentReconnectDelay);
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

    // 具体方法，提供默认实现，子类可以选择是否重写
    protected void doSend(byte[] data) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("该适配器不支持原始发送操作");
    }

    protected byte[] doReceive() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("该适配器不支持原始接收操作");
    }

    protected byte[] doReceive(long timeout) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("该适配器不支持原始接收操作");
    }

    // 抽象方法，由子类实现
    protected abstract void doConnect() throws Exception;
    protected abstract void doDisconnect() throws Exception;
    protected abstract void doHeartbeat() throws Exception;
    protected abstract void doAuthenticate() throws Exception;
}