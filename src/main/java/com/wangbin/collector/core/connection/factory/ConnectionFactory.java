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

    public ConnectionAdapter<?> createConnection(DeviceInfo deviceInfo) {
        if (deviceInfo == null || deviceInfo.getDeviceId() == null || deviceInfo.getDeviceId().isBlank()) {
            throw new IllegalArgumentException("设备信息无效");
        }
        ensureConnectionConfig(deviceInfo);
        String connectionType = resolveConnectionType(deviceInfo);
        switch (connectionType) {
            case "TCP":
                return createTcpConnection(deviceInfo);
            case "HTTP":
                return createHttpConnection(deviceInfo);
            case "MQTT":
                return createMqttConnection(deviceInfo);
            case "WEBSOCKET":
                return createWebSocketConnection(deviceInfo);
            case "COAP":
                return createCoapConnection(deviceInfo);
            case "MODBUS_TCP":
                return createModbusTcpConnection(deviceInfo);
            case "MODBUS_RTU":
                return createModbusRtuConnection(deviceInfo);
            case "SNMP":
                return createSnmpConnection(deviceInfo);
            case "OPC_UA":
            case "OPCUA":
                return createOpcUaConnection(deviceInfo);
            default:
                throw new CollectorException(
                        String.format("不支持的连接类型: %s", connectionType),
                        deviceInfo.getDeviceId(), null
                );
        }
    }

    private void ensureConnectionConfig(DeviceInfo deviceInfo) {
        if (deviceInfo.getConnectionConfig() == null) {
            deviceInfo.setConnectionConfig(new DeviceConnection());
        }
    }

    private String resolveConnectionType(DeviceInfo deviceInfo) {
        if (deviceInfo.getConnectionType() != null && !deviceInfo.getConnectionType().isBlank()) {
            return normalize(deviceInfo.getConnectionType());
        }
        DeviceConnection cfg = deviceInfo.getConnectionConfig();
        if (cfg != null && cfg.getConnectionType() != null && !cfg.getConnectionType().isBlank()) {
            return normalize(cfg.getConnectionType());
        }
        if (deviceInfo.getProtocolType() != null && !deviceInfo.getProtocolType().isBlank()) {
            return normalize(deviceInfo.getProtocolType());
        }
        return "TCP";
    }

    private String normalize(String type) {
        return type.toUpperCase().replace("-", "_");
    }

    private ConnectionAdapter<?> createTcpConnection(DeviceInfo deviceInfo) {
        try {
            return new TcpConnectionAdapter(deviceInfo);
        } catch (Exception e) {
            log.error("创建TCP连接失败: {}", deviceInfo.getDeviceId(), e);
            throw new CollectorException("创建TCP连接失败", deviceInfo.getDeviceId(), null);
        }
    }

    private ConnectionAdapter<?> createHttpConnection(DeviceInfo deviceInfo) {
        try {
            return new HttpConnectionAdapter(deviceInfo);
        } catch (Exception e) {
            log.error("创建HTTP连接失败: {}", deviceInfo.getDeviceId(), e);
            throw new CollectorException("创建HTTP连接失败", deviceInfo.getDeviceId(), null);
        }
    }

    private ConnectionAdapter<?> createMqttConnection(DeviceInfo deviceInfo) {
        try {
            return new MqttConnectionAdapter(deviceInfo);
        } catch (Exception e) {
            log.error("创建MQTT连接失败: {}", deviceInfo.getDeviceId(), e);
            throw new CollectorException("创建MQTT连接失败", deviceInfo.getDeviceId(), null);
        }
    }

    private ConnectionAdapter<?> createWebSocketConnection(DeviceInfo deviceInfo) {
        try {
            return new WebSocketConnectionAdapter(deviceInfo);
        } catch (Exception e) {
            log.error("创建WebSocket连接失败: {}", deviceInfo.getDeviceId(), e);
            throw new CollectorException("创建WebSocket连接失败", deviceInfo.getDeviceId(), null);
        }
    }

    private ConnectionAdapter<?> createCoapConnection(DeviceInfo deviceInfo) {
        try {
            return new CoapConnectionAdapter(deviceInfo);
        } catch (Exception e) {
            log.error("创建CoAP连接失败: {}", deviceInfo.getDeviceId(), e);
            throw new CollectorException("创建CoAP连接失败", deviceInfo.getDeviceId(), null);
        }
    }

    private ConnectionAdapter<?> createModbusTcpConnection(DeviceInfo deviceInfo) {
        try {
            return new ModbusTcpConnectionAdapter(deviceInfo);
        } catch (Exception e) {
            log.error("创建Modbus TCP连接失败: {}", deviceInfo.getDeviceId(), e);
            throw new CollectorException("创建Modbus TCP连接失败", deviceInfo.getDeviceId(), null);
        }
    }

    private ConnectionAdapter<?> createModbusRtuConnection(DeviceInfo deviceInfo) {
        try {
            return new ModbusRtuConnectionAdapter(deviceInfo);
        } catch (Exception e) {
            log.error("创建Modbus RTU连接失败: {}", deviceInfo.getDeviceId(), e);
            throw new CollectorException("创建Modbus RTU连接失败", deviceInfo.getDeviceId(), null);
        }
    }

    private ConnectionAdapter<?> createSnmpConnection(DeviceInfo deviceInfo) {
        try {
            return new SnmpConnectionAdapter(deviceInfo);
        } catch (Exception e) {
            log.error("创建SNMP连接失败: {}", deviceInfo.getDeviceId(), e);
            throw new CollectorException("创建SNMP连接失败", deviceInfo.getDeviceId(), null);
        }
    }

    private ConnectionAdapter<?> createOpcUaConnection(DeviceInfo deviceInfo) {
        try {
            return new OpcUaConnectionAdapter(deviceInfo);
        } catch (Exception e) {
            log.error("创建OPC UA连接失败: {}", deviceInfo.getDeviceId(), e);
            throw new CollectorException("创建OPC UA连接失败", deviceInfo.getDeviceId(), null);
        }
    }
}
