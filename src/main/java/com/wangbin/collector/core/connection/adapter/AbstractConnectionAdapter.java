package com.wangbin.collector.core.connection.adapter;

import com.wangbin.collector.common.domain.entity.DeviceConnection;
import com.wangbin.collector.common.domain.entity.DeviceInfo;
import com.wangbin.collector.common.domain.enums.ConnectionStatus;
import com.wangbin.collector.common.utils.JsonUtil;
import com.wangbin.collector.core.connection.model.ConnectionMetrics;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Base implementation that handles reconnection, heartbeat and statistics.
 */
@Slf4j
public abstract class AbstractConnectionAdapter<C> implements ConnectionAdapter<C> {

    protected final DeviceInfo deviceInfo;
    protected final DeviceConnection config;

    protected volatile ConnectionStatus status = ConnectionStatus.DISCONNECTED;
    protected ConnectionMetrics metrics;
    protected String connectionId;
    protected long lastActivityTime;
    protected Map<String, Object> connectionParams = new ConcurrentHashMap<>();
    protected Map<String, Object> statistics = new ConcurrentHashMap<>();

    protected AtomicLong bytesSent = new AtomicLong(0);
    protected AtomicLong bytesReceived = new AtomicLong(0);
    protected AtomicLong messagesSent = new AtomicLong(0);
    protected AtomicLong messagesReceived = new AtomicLong(0);
    protected AtomicLong errors = new AtomicLong(0);
    protected AtomicLong heartbeats = new AtomicLong(0);

    private int reconnectAttempts = 0;
    private long currentReconnectDelay;
    private boolean reconnecting = false;

    protected AbstractConnectionAdapter(DeviceInfo deviceInfo) {
        this.deviceInfo = deviceInfo;
        DeviceConnection source = deviceInfo != null ? deviceInfo.getConnectionConfig() : null;
        if (source == null) {
            source = new DeviceConnection();
        }
        if (source.getHost() == null && deviceInfo != null) {
            source.setHost(deviceInfo.getIpAddress());
        }
        if (source.getPort() == null && deviceInfo != null) {
            source.setPort(deviceInfo.getPort());
        }
        if (source.getConnectionType() == null && deviceInfo != null) {
            source.setConnectionType(deviceInfo.getConnectionType());
        }
        this.config = source;
        this.connectionId = generateConnectionId();
        this.metrics = new ConnectionMetrics();
        this.lastActivityTime = System.currentTimeMillis();
        this.currentReconnectDelay = config.getInitialReconnectDelay();
    }

    @Override
    public void connect() throws Exception {
        log.info("Connecting: {}", connectionId);
        if (status == ConnectionStatus.CONNECTED) {
            log.warn("Connection already established: {}", connectionId);
            return;
        }
        if (status == ConnectionStatus.CONNECTING) {
            log.warn("Connection is in progress: {}", connectionId);
            return;
        }
        try {
            status = ConnectionStatus.CONNECTING;
            metrics.setStatus(status);
            doConnect();
            status = ConnectionStatus.CONNECTED;
            lastActivityTime = System.currentTimeMillis();
            metrics.setConnectTime(lastActivityTime);
            metrics.setStatus(status);
            metrics.setLastError(null);
            log.info("Connection established: {}", connectionId);
        } catch (Exception e) {
            status = ConnectionStatus.ERROR;
            metrics.setLastError(e.getMessage());
            errors.incrementAndGet();
            log.error("Connection failed: {}", connectionId, e);
            throw e;
        }
    }

    @Override
    public void disconnect() throws Exception {
        log.info("Disconnecting: {}", connectionId);
        if (status == ConnectionStatus.DISCONNECTED) {
            log.warn("Connection already closed: {}", connectionId);
            return;
        }
        if (status == ConnectionStatus.CONNECTING || status == ConnectionStatus.RECONNECTING) {
            log.warn("Connection still in progress, skip disconnect: {}", connectionId);
            return;
        }
        try {
            status = ConnectionStatus.RECONNECTING;
            metrics.setStatus(status);
            doDisconnect();
            status = ConnectionStatus.DISCONNECTED;
            metrics.setStatus(status);
            metrics.setLastError(null);
            reconnectAttempts = 0;
            currentReconnectDelay = config.getInitialReconnectDelay();
            log.info("Connection closed: {}", connectionId);
        } catch (Exception e) {
            status = ConnectionStatus.ERROR;
            metrics.setLastError(e.getMessage());
            errors.incrementAndGet();
            log.error("Disconnect failed: {}", connectionId, e);
            throw e;
        }
    }

    @Override
    public void reconnect() throws Exception {
        log.info("Reconnecting: {}", connectionId);
        if (reconnecting) {
            log.warn("Reconnect already in progress: {}", connectionId);
            return;
        }
        if (config.getMaxReconnectAttempts() > 0 && reconnectAttempts >= config.getMaxReconnectAttempts()) {
            log.error("Reach max reconnect attempts {}: {}", config.getMaxReconnectAttempts(), connectionId);
            status = ConnectionStatus.DISCONNECTED;
            metrics.setStatus(status);
            throw new Exception("Reach max reconnect attempts");
        }
        try {
            reconnecting = true;
            status = ConnectionStatus.RECONNECTING;
            if (isConnected()) {
                disconnect();
            }
            long delay = calculateReconnectDelay();
            log.info("Reconnect after {} ms: {}", delay, connectionId);
            Thread.sleep(delay);
            doConnect();
            reconnectAttempts = 0;
            currentReconnectDelay = config.getInitialReconnectDelay();
            status = ConnectionStatus.CONNECTED;
            lastActivityTime = System.currentTimeMillis();
            metrics.setConnectTime(lastActivityTime);
            metrics.setStatus(status);
            metrics.setLastError(null);
            log.info("Reconnect success: {}", connectionId);
        } catch (Exception e) {
            reconnectAttempts++;
            updateReconnectDelay();
            status = ConnectionStatus.ERROR;
            metrics.setLastError(e.getMessage());
            errors.incrementAndGet();
            log.error("Reconnect failed ({}): {}", reconnectAttempts, connectionId, e);
            throw e;
        } finally {
            reconnecting = false;
        }
    }

    private long calculateReconnectDelay() {
        if (reconnectAttempts == 0) {
            return currentReconnectDelay;
        }
        double jitter = 0.9 + Math.random() * 0.2;
        return (long) (currentReconnectDelay * jitter);
    }

    private void updateReconnectDelay() {
        long newDelay = (long) (currentReconnectDelay * config.getReconnectBackoffMultiplier());
        currentReconnectDelay = Math.min(newDelay, config.getMaxReconnectDelay());
    }

    @Override
    public void send(byte[] data) throws Exception {
        checkConnection();
        try {
            doSend(data);
            bytesSent.addAndGet(data.length);
            messagesSent.incrementAndGet();
            lastActivityTime = System.currentTimeMillis();
            log.debug("Send {} bytes via {}", data.length, connectionId);
        } catch (Exception e) {
            errors.incrementAndGet();
            metrics.setLastError(e.getMessage());
            log.error("Send failed: {}", connectionId, e);
            throw e;
        }
    }

    @Override
    public void send(String data) throws Exception {
        send(data.getBytes(config.getCharset()));
    }

    @Override
    public void send(Object data) throws Exception {
        send(JsonUtil.toJsonString(data));
    }

    @Override
    public byte[] receive() throws Exception {
        checkConnection();
        try {
            byte[] data = doReceive();
            if (data != null) {
                bytesReceived.addAndGet(data.length);
                messagesReceived.incrementAndGet();
                lastActivityTime = System.currentTimeMillis();
            }
            return data;
        } catch (Exception e) {
            errors.incrementAndGet();
            metrics.setLastError(e.getMessage());
            log.error("Receive failed: {}", connectionId, e);
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
            byte[] data = doReceive(timeout);
            if (data != null) {
                bytesReceived.addAndGet(data.length);
                messagesReceived.incrementAndGet();
                lastActivityTime = System.currentTimeMillis();
            }
            return data;
        } catch (Exception e) {
            errors.incrementAndGet();
            metrics.setLastError(e.getMessage());
            log.error("Receive failed: {}", connectionId, e);
            throw e;
        }
    }

    @Override
    public void heartbeat() throws Exception {
        log.debug("Heartbeat: {}", connectionId);
        if (!isConnected()) {
            log.warn("Connection not established, skip heartbeat: {}", connectionId);
            return;
        }
        try {
            doHeartbeat();
            heartbeats.incrementAndGet();
            lastActivityTime = System.currentTimeMillis();
            metrics.setLastHeartbeatTime(lastActivityTime);
            metrics.setLastActivityTime(lastActivityTime);
        } catch (Exception e) {
            log.error("Heartbeat failed: {}", connectionId, e);
            errors.incrementAndGet();
            metrics.setLastError(e.getMessage());
            metrics.setLastErrorTime(System.currentTimeMillis());
            if (config.isAutoReconnect()) {
                try {
                    reconnect();
                } catch (Exception re) {
                    status = ConnectionStatus.ERROR;
                    metrics.setStatus(status);
                    log.error("Reconnect after heartbeat failure also failed: {}", connectionId, re);
                }
            } else {
                status = ConnectionStatus.ERROR;
                metrics.setStatus(status);
            }
        }
    }

    @Override
    public boolean healthCheck() {
        if (status != ConnectionStatus.CONNECTED) {
            log.warn("Connection status abnormal: {}", connectionId);
            return false;
        }
        long idleTime = System.currentTimeMillis() - lastActivityTime;
        if (config.getHeartbeatTimeout() > 0 && idleTime > config.getHeartbeatTimeout()) {
            log.warn("Connection idle timeout detected: {}", connectionId);
            return false;
        }
        try {
            doHeartbeat();
            return true;
        } catch (Exception e) {
            log.error("Health check failed: {}", connectionId, e);
            return false;
        }
    }

    protected void validateConnection() throws Exception {
        if (!isConnected()) {
            if (config.isAutoReconnect()) {
                log.info("Connection closed, trigger reconnect: {}", connectionId);
                reconnect();
            } else {
                throw new Exception("Connection closed");
            }
        }
    }

    @Override
    public void authenticate() throws Exception {
        if (!isConnected()) {
            throw new IllegalStateException("Connection not established");
        }
        try {
            log.info("Authenticating: {}", connectionId);
            status = ConnectionStatus.AUTHENTICATING;
            doAuthenticate();
            status = ConnectionStatus.AUTHENTICATED;
            metrics.setAuthTime(System.currentTimeMillis());
            metrics.setStatus(status);
            log.info("Authentication success: {}", connectionId);
        } catch (Exception e) {
            status = ConnectionStatus.ERROR;
            metrics.setLastError(e.getMessage());
            errors.incrementAndGet();
            log.error("Authentication failed: {}", connectionId, e);
            throw e;
        }
    }

    @Override
    public ConnectionStatus getStatus() {
        return status;
    }

    @Override
    public DeviceConnection getConnectionConfig() {
        return config;
    }

    @Override
    public DeviceInfo getDeviceInfo() {
        return deviceInfo;
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
        return deviceInfo != null ? deviceInfo.getDeviceId() : null;
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
        Map<String, Object> stats = new HashMap<>();
        stats.put("connectionId", connectionId);
        stats.put("status", status.name());
        stats.put("bytesSent", bytesSent.get());
        stats.put("bytesReceived", bytesReceived.get());
        stats.put("messagesSent", messagesSent.get());
        stats.put("messagesReceived", messagesReceived.get());
        stats.put("errors", errors.get());
        stats.put("heartbeats", heartbeats.get());
        stats.put("lastActivityTime", lastActivityTime);
        stats.put("connectionDuration", status == ConnectionStatus.CONNECTED ? System.currentTimeMillis() - metrics.getConnectTime() : 0);
        stats.put("reconnectAttempts", reconnectAttempts);
        stats.put("currentReconnectDelay", currentReconnectDelay);
        return stats;
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

    protected String generateConnectionId() {
        String timestamp = String.valueOf(System.currentTimeMillis());
        String random = String.valueOf((int) (Math.random() * 10000));
        return "CONN_" + (deviceInfo != null ? deviceInfo.getDeviceId() : "UNKNOWN") + "_" + timestamp + "_" + random;
    }

    protected void checkConnection() {
        if (!isConnected()) {
            throw new IllegalStateException("Connection not established or closed");
        }
    }

    protected void updateMetrics() {
        metrics.setBytesSent(bytesSent.get());
        metrics.setBytesReceived(bytesReceived.get());
        metrics.setMessagesSent(messagesSent.get());
        metrics.setMessagesReceived(messagesReceived.get());
        metrics.setErrors(errors.get());
        metrics.setHeartbeats(heartbeats.get());
        metrics.setLastActivityTime(lastActivityTime);
        metrics.setConnectionDuration(System.currentTimeMillis() - metrics.getConnectTime());
        long totalMessages = messagesSent.get() + messagesReceived.get();
        if (totalMessages > 0) {
            double successRate = (totalMessages - errors.get()) * 100.0 / totalMessages;
            metrics.setSuccessRate(successRate);
        }
    }

    protected String resolveHost() {
        if (config.getHost() != null && !config.getHost().isEmpty()) {
            return config.getHost();
        }
        return deviceInfo != null ? deviceInfo.getIpAddress() : null;
    }

    protected Integer resolvePort() {
        if (config.getPort() != null && config.getPort() > 0) {
            return config.getPort();
        }
        return deviceInfo != null ? deviceInfo.getPort() : null;
    }

    protected abstract void doConnect() throws Exception;

    protected abstract void doDisconnect() throws Exception;

    protected abstract void doHeartbeat() throws Exception;

    protected abstract void doAuthenticate() throws Exception;

    protected void doSend(byte[] data) {
        throw new UnsupportedOperationException("Raw send not supported");
    }

    protected byte[] doReceive() {
        throw new UnsupportedOperationException("Raw receive not supported");
    }

    protected byte[] doReceive(long timeout) {
        throw new UnsupportedOperationException("Raw receive not supported");
    }
}
