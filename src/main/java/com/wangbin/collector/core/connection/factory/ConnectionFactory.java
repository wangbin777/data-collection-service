package com.wangbin.collector.core.connection.factory;

import com.wangbin.collector.common.domain.entity.DeviceConnection;
import com.wangbin.collector.common.domain.entity.DeviceInfo;
import com.wangbin.collector.common.exception.CollectorException;
import com.wangbin.collector.core.connection.adapter.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * 连接工厂
 */
@Slf4j
@Component
public class ConnectionFactory {

    public ConnectionAdapter<?> createConnection(DeviceInfo deviceInfo, DeviceConnection connectionConfig) {
        if (deviceInfo == null || deviceInfo.getDeviceId() == null || deviceInfo.getDeviceId().isBlank()) {
            throw new IllegalArgumentException("设备信息无效");
        }
        DeviceConnection cfg = connectionConfig != null ? connectionConfig : new DeviceConnection();
        String connectionType = resolveConnectionType(deviceInfo, cfg);
        return switch (connectionType) {
            case "TCP" -> createTcpConnection(deviceInfo, cfg);
            case "HTTP" -> createHttpConnection(deviceInfo, cfg);
            case "MQTT" -> createMqttConnection(deviceInfo, cfg);
            case "WEBSOCKET" -> createWebSocketConnection(deviceInfo, cfg);
            case "COAP" -> createCoapConnection(deviceInfo, cfg);
            case "MODBUS_TCP" -> createModbusTcpConnection(deviceInfo, cfg);
            case "MODBUS_RTU" -> createModbusRtuConnection(deviceInfo, cfg);
            case "SNMP" -> createSnmpConnection(deviceInfo, cfg);
            case "OPC_UA", "OPCUA" -> createOpcUaConnection(deviceInfo, cfg);
            default -> throw new CollectorException(
                    String.format("不支持的连接类型: %s", connectionType),
                    deviceInfo.getDeviceId(), null
            );
        };
    }

    private String resolveConnectionType(DeviceInfo deviceInfo, DeviceConnection cfg) {
        if (deviceInfo.getConnectionType() != null && !deviceInfo.getConnectionType().isBlank()) {
            return normalize(deviceInfo.getConnectionType());
        }
        if (deviceInfo.getProtocolType() != null && !deviceInfo.getProtocolType().isBlank()) {
            return normalize(deviceInfo.getProtocolType());
        }
        if (cfg != null && cfg.getConnectionType() != null && !cfg.getConnectionType().isBlank()) {
            return normalize(cfg.getConnectionType());
        }
        return "TCP";
    }

    private String normalize(String type) {
        return type.toUpperCase().replace("-", "_");
    }

    private ConnectionAdapter<?> createTcpConnection(DeviceInfo deviceInfo, DeviceConnection cfg) {
        try {
            return new TcpConnectionAdapter(deviceInfo, cfg);
        } catch (Exception e) {
            log.error("创建TCP连接失败: {}", deviceInfo.getDeviceId(), e);
            throw new CollectorException("创建TCP连接失败", deviceInfo.getDeviceId(), null);
        }
    }

    private ConnectionAdapter<?> createHttpConnection(DeviceInfo deviceInfo, DeviceConnection cfg) {
        try {
            return new HttpConnectionAdapter(deviceInfo, cfg);
        } catch (Exception e) {
            log.error("创建HTTP连接失败: {}", deviceInfo.getDeviceId(), e);
            throw new CollectorException("创建HTTP连接失败", deviceInfo.getDeviceId(), null);
        }
    }

    private ConnectionAdapter<?> createMqttConnection(DeviceInfo deviceInfo, DeviceConnection cfg) {
        try {
            return new MqttConnectionAdapter(deviceInfo, cfg);
        } catch (Exception e) {
            log.error("创建MQTT连接失败: {}", deviceInfo.getDeviceId(), e);
            throw new CollectorException("创建MQTT连接失败", deviceInfo.getDeviceId(), null);
        }
    }

    private ConnectionAdapter<?> createWebSocketConnection(DeviceInfo deviceInfo, DeviceConnection cfg) {
        try {
            return new WebSocketConnectionAdapter(deviceInfo, cfg);
        } catch (Exception e) {
            log.error("创建WebSocket连接失败: {}", deviceInfo.getDeviceId(), e);
            throw new CollectorException("创建WebSocket连接失败", deviceInfo.getDeviceId(), null);
        }
    }

    private ConnectionAdapter<?> createCoapConnection(DeviceInfo deviceInfo, DeviceConnection cfg) {
        try {
            return new CoapConnectionAdapter(deviceInfo, cfg);
        } catch (Exception e) {
            log.error("创建CoAP连接失败: {}", deviceInfo.getDeviceId(), e);
            throw new CollectorException("创建CoAP连接失败", deviceInfo.getDeviceId(), null);
        }
    }

    private ConnectionAdapter<?> createModbusTcpConnection(DeviceInfo deviceInfo, DeviceConnection cfg) {
        try {
            return new ModbusTcpConnectionAdapter(deviceInfo, cfg);
        } catch (Exception e) {
            log.error("创建Modbus TCP连接失败: {}", deviceInfo.getDeviceId(), e);
            throw new CollectorException("创建Modbus TCP连接失败", deviceInfo.getDeviceId(), null);
        }
    }

    private ConnectionAdapter<?> createModbusRtuConnection(DeviceInfo deviceInfo, DeviceConnection cfg) {
        try {
            return new ModbusRtuConnectionAdapter(deviceInfo, cfg);
        } catch (Exception e) {
            log.error("创建Modbus RTU连接失败: {}", deviceInfo.getDeviceId(), e);
            throw new CollectorException("创建Modbus RTU连接失败", deviceInfo.getDeviceId(), null);
        }
    }

    private ConnectionAdapter<?> createSnmpConnection(DeviceInfo deviceInfo, DeviceConnection cfg) {
        try {
            return new SnmpConnectionAdapter(deviceInfo, cfg);
        } catch (Exception e) {
            log.error("创建SNMP连接失败: {}", deviceInfo.getDeviceId(), e);
            throw new CollectorException("创建SNMP连接失败", deviceInfo.getDeviceId(), null);
        }
    }

    private ConnectionAdapter<?> createOpcUaConnection(DeviceInfo deviceInfo, DeviceConnection cfg) {
        try {
            return new OpcUaConnectionAdapter(deviceInfo, cfg);
        } catch (Exception e) {
            log.error("创建OPC UA连接失败: {}", deviceInfo.getDeviceId(), e);
            throw new CollectorException("创建OPC UA连接失败", deviceInfo.getDeviceId(), null);
        }
    }
}
