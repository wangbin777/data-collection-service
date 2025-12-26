package com.wangbin.collector.core.connection.manager;

import com.wangbin.collector.common.domain.enums.ConnectionStatus;
import com.wangbin.collector.common.exception.CollectorException;
import com.wangbin.collector.core.connection.adapter.ConnectionAdapter;
import com.wangbin.collector.core.connection.factory.ConnectionFactory;
import com.wangbin.collector.core.connection.model.ConnectionConfig;
import com.wangbin.collector.core.connection.model.ConnectionMetrics;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * 连接管理器
 */
@Slf4j
@Component
public class ConnectionManager {

    @Autowired
    private ConnectionFactory connectionFactory;

    // 连接存储：deviceId -> ConnectionAdapter
    private final Map<String, ConnectionAdapter> connections = new ConcurrentHashMap<>();

    // 连接分组：groupId -> List<deviceId>
    private final Map<String, List<String>> groupConnections = new ConcurrentHashMap<>();

    // 连接状态监听器
    private final List<ConnectionStateListener> stateListeners = new CopyOnWriteArrayList<>();

    // 连接事件处理器
    private final List<ConnectionEventHandler> eventHandlers = new CopyOnWriteArrayList<>();

    @PostConstruct
    public void init() {
        log.info("连接管理器初始化完成");
    }

    @PreDestroy
    public void destroy() {
        log.info("开始关闭所有连接...");
        closeAllConnections();
        log.info("所有连接已关闭");
    }

    /**
     * 创建连接
     */
    public ConnectionAdapter createConnection(ConnectionConfig config) {
        String deviceId = config.getDeviceId();

        synchronized (connections) {
            if (connections.containsKey(deviceId)) {
                ConnectionAdapter existing = connections.get(deviceId);
                if (existing.isConnected()) {
                    log.warn("设备 {} 的连接已存在且处于连接状态", deviceId);
                    return existing;
                }
                // 移除旧的连接
                removeConnection(deviceId);
            }

            try {
                // 创建新连接
                ConnectionAdapter connection = connectionFactory.createConnection(config);
                connections.put(deviceId, connection);

                // 添加到分组
                addToGroup(deviceId, deviceId);

                // 通知连接创建
                notifyConnectionCreated(connection);

                log.info("连接创建成功: {}", deviceId);
                return connection;
            } catch (Exception e) {
                log.error("创建连接失败: {}", deviceId, e);
                throw new CollectorException("创建连接失败", deviceId, null);
            }
        }
    }

    /**
     * 建立连接
     */
    public void connect(String deviceId) {
        ConnectionAdapter connection = getConnection(deviceId);
        if (connection == null) {
            throw new CollectorException("连接不存在", deviceId, null);
        }

        try {
            connection.connect();
            notifyConnectionStateChanged(connection, ConnectionStatus.CONNECTED);
            log.info("连接建立成功: {}", deviceId);
        } catch (Exception e) {
            log.error("连接建立失败: {}", deviceId, e);
            notifyConnectionError(connection, e);
            throw new CollectorException("连接建立失败", deviceId, null);
        }
    }

    /**
     * 断开连接
     */
    public void disconnect(String deviceId) {
        ConnectionAdapter connection = getConnection(deviceId);
        if (connection == null) {
            throw new CollectorException("连接不存在", deviceId, null);
        }

        try {
            connection.disconnect();
            notifyConnectionStateChanged(connection, ConnectionStatus.DISCONNECTED);
            log.info("连接断开成功: {}", deviceId);
        } catch (Exception e) {
            log.error("连接断开失败: {}", deviceId, e);
            notifyConnectionError(connection, e);
            throw new CollectorException("连接断开失败", deviceId, null);
        }
    }

    /**
     * 重新连接
     */
    public void reconnect(String deviceId) {
        ConnectionAdapter connection = getConnection(deviceId);
        if (connection == null) {
            throw new CollectorException("连接不存在", deviceId, null);
        }

        try {
            connection.reconnect();
            notifyConnectionStateChanged(connection, ConnectionStatus.CONNECTED);
            log.info("重新连接成功: {}", deviceId);
        } catch (Exception e) {
            log.error("重新连接失败: {}", deviceId, e);
            notifyConnectionError(connection, e);
            throw new CollectorException("重新连接失败", deviceId, null);
        }
    }

    /**
     * 发送数据
     */
    public void send(String deviceId, byte[] data) {
        ConnectionAdapter connection = getConnection(deviceId);
        if (connection == null) {
            throw new CollectorException("连接不存在", deviceId, null);
        }

        try {
            connection.send(data);
            log.debug("数据发送成功: {}, 大小: {} bytes", deviceId, data.length);
        } catch (Exception e) {
            log.error("数据发送失败: {}", deviceId, e);
            notifyConnectionError(connection, e);
            throw new CollectorException("数据发送失败", deviceId, null);
        }
    }

    /**
     * 接收数据
     */
    public byte[] receive(String deviceId) {
        ConnectionAdapter connection = getConnection(deviceId);
        if (connection == null) {
            throw new CollectorException("连接不存在", deviceId, null);
        }

        try {
            return connection.receive();
        } catch (Exception e) {
            log.error("数据接收失败: {}", deviceId, e);
            notifyConnectionError(connection, e);
            throw new CollectorException("数据接收失败", deviceId, null);
        }
    }

    /**
     * 获取连接
     */
    public ConnectionAdapter getConnection(String deviceId) {
        return connections.get(deviceId);
    }

    /**
     * 获取所有连接
     */
    public List<ConnectionAdapter> getAllConnections() {
        return new ArrayList<>(connections.values());
    }

    /**
     * 获取活跃连接
     */
    public List<ConnectionAdapter> getActiveConnections() {
        return connections.values().stream()
                .filter(ConnectionAdapter::isConnected)
                .collect(Collectors.toList());
    }

    /**
     * 获取分组连接
     */
    public List<ConnectionAdapter> getGroupConnections(String groupId) {
        List<String> deviceIds = groupConnections.get(groupId);
        if (deviceIds == null) {
            return Collections.emptyList();
        }

        return deviceIds.stream()
                .map(this::getConnection)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    /**
     * 获取连接指标
     */
    public ConnectionMetrics getConnectionMetrics(String deviceId) {
        ConnectionAdapter connection = getConnection(deviceId);
        return connection != null ? connection.getMetrics() : null;
    }

    /**
     * 获取所有连接指标
     */
    public Map<String, ConnectionMetrics> getAllConnectionMetrics() {
        Map<String, ConnectionMetrics> metrics = new HashMap<>();
        connections.forEach((deviceId, connection) -> {
            metrics.put(deviceId, connection.getMetrics());
        });
        return metrics;
    }

    /**
     * 移除连接
     */
    public void removeConnection(String deviceId) {
        ConnectionAdapter connection = connections.remove(deviceId);
        if (connection != null) {
            try {
                if (connection.isConnected()) {
                    connection.disconnect();
                }
            } catch (Exception e) {
                log.error("断开连接失败: {}", deviceId, e);
            }

            // 从分组中移除
            removeFromGroup(deviceId);

            // 通知连接移除
            notifyConnectionRemoved(connection);

            log.info("连接移除成功: {}", deviceId);
        }
    }

    /**
     * 关闭所有连接
     */
    public void closeAllConnections() {
        List<String> deviceIds = new ArrayList<>(connections.keySet());
        for (String deviceId : deviceIds) {
            try {
                removeConnection(deviceId);
            } catch (Exception e) {
                log.error("关闭连接失败: {}", deviceId, e);
            }
        }
        connections.clear();
        groupConnections.clear();
    }

    /**
     * 关闭分组连接
     */
    public void closeGroupConnections(String groupId) {
        List<ConnectionAdapter> groupConnections = getGroupConnections(groupId);
        for (ConnectionAdapter connection : groupConnections) {
            try {
                removeConnection(connection.getDeviceId());
            } catch (Exception e) {
                log.error("关闭分组连接失败: {}", connection.getDeviceId(), e);
            }
        }
    }

    /**
     * 心跳检查
     */
    @Scheduled(fixedDelay = 30000) // 每30秒检查一次
    public void startHeartbeatMonitor() {
        log.debug("开始心跳检查...");

        for (ConnectionAdapter connection : connections.values()) {
            if (connection.isConnected()) {
                try {
                    // 检查心跳超时
                    ConnectionMetrics metrics = connection.getMetrics();
                    long heartbeatInterval = connection.getConfig().getHeartbeatInterval();

                    if (metrics.isTimeout(heartbeatInterval)) {
                        log.warn("连接心跳超时: {}", connection.getDeviceId());
                        notifyHeartbeatTimeout(connection);

                        // 如果配置了自动重连，则尝试重连
                        if (connection.getConfig().isAutoReconnect()) {
                            reconnect(connection.getDeviceId());
                        }
                    } else {
                        // 发送心跳
                        connection.heartbeat();
                    }
                } catch (Exception e) {
                    log.error("心跳检查失败: {}", connection.getDeviceId(), e);
                    notifyConnectionError(connection, e);
                }
            }
        }
    }

    /**
     * 添加连接状态监听器
     */
    public void addStateListener(ConnectionStateListener listener) {
        stateListeners.add(listener);
    }

    /**
     * 移除连接状态监听器
     */
    public void removeStateListener(ConnectionStateListener listener) {
        stateListeners.remove(listener);
    }

    /**
     * 添加连接事件处理器
     */
    public void addEventHandler(ConnectionEventHandler handler) {
        eventHandlers.add(handler);
    }

    /**
     * 移除连接事件处理器
     */
    public void removeEventHandler(ConnectionEventHandler handler) {
        eventHandlers.remove(handler);
    }

    /**
     * 添加到分组
     */
    private void addToGroup(String groupId, String deviceId) {
        if (groupId != null && !groupId.isEmpty()) {
            groupConnections.computeIfAbsent(groupId, k -> new ArrayList<>()).add(deviceId);
        }
    }

    /**
     * 从分组中移除
     */
    private void removeFromGroup(String deviceId) {
        groupConnections.forEach((groupId, deviceIds) -> {
            deviceIds.remove(deviceId);
            if (deviceIds.isEmpty()) {
                groupConnections.remove(groupId);
            }
        });
    }

    /**
     * 通知连接创建
     */
    private void notifyConnectionCreated(ConnectionAdapter connection) {
        for (ConnectionEventHandler handler : eventHandlers) {
            try {
                handler.onConnectionCreated(connection);
            } catch (Exception e) {
                log.error("连接创建事件处理失败", e);
            }
        }
    }

    /**
     * 通知连接状态变化
     */
    private void notifyConnectionStateChanged(ConnectionAdapter connection, ConnectionStatus newStatus) {
        for (ConnectionStateListener listener : stateListeners) {
            try {
                listener.onStateChanged(connection, newStatus);
            } catch (Exception e) {
                log.error("连接状态变化监听失败", e);
            }
        }

        for (ConnectionEventHandler handler : eventHandlers) {
            try {
                handler.onConnectionStateChanged(connection, newStatus);
            } catch (Exception e) {
                log.error("连接状态变化事件处理失败", e);
            }
        }
    }

    /**
     * 通知连接错误
     */
    private void notifyConnectionError(ConnectionAdapter connection, Exception error) {
        for (ConnectionEventHandler handler : eventHandlers) {
            try {
                handler.onConnectionError(connection, error);
            } catch (Exception e) {
                log.error("连接错误事件处理失败", e);
            }
        }
    }

    /**
     * 通知心跳超时
     */
    private void notifyHeartbeatTimeout(ConnectionAdapter connection) {
        for (ConnectionEventHandler handler : eventHandlers) {
            try {
                handler.onHeartbeatTimeout(connection);
            } catch (Exception e) {
                log.error("心跳超时事件处理失败", e);
            }
        }
    }

    /**
     * 通知连接移除
     */
    private void notifyConnectionRemoved(ConnectionAdapter connection) {
        for (ConnectionEventHandler handler : eventHandlers) {
            try {
                handler.onConnectionRemoved(connection);
            } catch (Exception e) {
                log.error("连接移除事件处理失败", e);
            }
        }
    }

    /**
     * 连接状态监听器接口
     */
    public interface ConnectionStateListener {
        void onStateChanged(ConnectionAdapter connection, ConnectionStatus newStatus);
    }

    /**
     * 连接事件处理器接口
     */
    public interface ConnectionEventHandler {
        void onConnectionCreated(ConnectionAdapter connection);
        void onConnectionStateChanged(ConnectionAdapter connection, ConnectionStatus newStatus);
        void onConnectionError(ConnectionAdapter connection, Exception error);
        void onHeartbeatTimeout(ConnectionAdapter connection);
        void onConnectionRemoved(ConnectionAdapter connection);
    }
}
