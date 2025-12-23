package com.wangbin.collector.core.connection.factory;

import com.wangbin.collector.common.exception.CollectorException;
import com.wangbin.collector.core.connection.adapter.*;
import com.wangbin.collector.core.connection.model.ConnectionConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * 连接工厂
 */
@Slf4j
@Component
public class ConnectionFactory {

    /**
     * 创建连接适配器
     */
    public ConnectionAdapter createConnection(ConnectionConfig config) {
        if (config == null || !config.isValid()) {
            throw new IllegalArgumentException("连接配置无效");
        }

        String connectionType = config.getConnectionType().toUpperCase();

        return switch (connectionType) {
            case "TCP" -> createTcpConnection(config);
            case "HTTP" -> createHttpConnection(config);
            case "MQTT" -> createMqttConnection(config);
            case "WEBSOCKET" -> createWebSocketConnection(config);
            default -> throw new CollectorException(
                    String.format("不支持的连接类型: %s", connectionType),
                    config.getDeviceId(), null
            );
        };
    }

    /**
     * 创建TCP连接
     */
    private ConnectionAdapter createTcpConnection(ConnectionConfig config) {
        try {
            return new TcpConnectionAdapter(config);
        } catch (Exception e) {
            log.error("创建TCP连接失败: {}", config.getDeviceId(), e);
            throw new CollectorException("创建TCP连接失败", config.getDeviceId(), null);
        }
    }

    /**
     * 创建HTTP连接
     */
    private ConnectionAdapter createHttpConnection(ConnectionConfig config) {
        try {
            return new HttpConnectionAdapter(config);
        } catch (Exception e) {
            log.error("创建HTTP连接失败: {}", config.getDeviceId(), e);
            throw new CollectorException("创建HTTP连接失败", config.getDeviceId(), null);
        }
    }

    /**
     * 创建MQTT连接
     */
    private ConnectionAdapter createMqttConnection(ConnectionConfig config) {
        try {
            return new MqttConnectionAdapter(config);
        } catch (Exception e) {
            log.error("创建MQTT连接失败: {}", config.getDeviceId(), e);
            throw new CollectorException("创建MQTT连接失败", config.getDeviceId(), null);
        }
    }

    /**
     * 创建WebSocket连接
     */
    private ConnectionAdapter createWebSocketConnection(ConnectionConfig config) {
        try {
            return new WebSocketConnectionAdapter(config);
        } catch (Exception e) {
            log.error("创建WebSocket连接失败: {}", config.getDeviceId(), e);
            throw new CollectorException("创建WebSocket连接失败", config.getDeviceId(), null);
        }
    }

    /**
     * 根据协议类型创建连接
     */
    public ConnectionAdapter createConnectionByProtocol(ConnectionConfig config) {
        String protocolType = config.getProtocolType().toUpperCase();

        // 根据协议类型确定连接类型
        switch (protocolType) {
            case "MODBUS_TCP":
            case "IEC104":
            case "CUSTOM_TCP":
                config.setConnectionType("TCP");
                break;
            case "HTTP":
            case "HTTPS":
                config.setConnectionType("HTTP");
                break;
            case "MQTT":
            case "MQTT_SSL":
                config.setConnectionType("MQTT");
                break;
            case "WEBSOCKET":
            case "WEBSOCKET_SSL":
                config.setConnectionType("WEBSOCKET");
                break;
            default:
                throw new CollectorException(
                        String.format("不支持的协议类型: %s", protocolType),
                        config.getDeviceId(), null
                );
        }

        return createConnection(config);
    }
}
