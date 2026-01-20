package com.wangbin.collector.core.config.manager;

import com.wangbin.collector.common.domain.entity.DeviceConnection;
import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.common.domain.entity.DeviceConnection;
import com.wangbin.collector.common.utils.JsonDataPointLoader;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

@Slf4j
public class DataUtils {

    /**
     * 创建模拟数据�?
     */
    public static List<DataPoint> createMockDataPoints(String deviceId) {
        List<DataPoint> points = new ArrayList<>();

        // 根据设备类型生成不同的数据点
        switch (deviceId) {
            case "PLC001":
                points = DataUtils.createPLC001DataPoints();
                break;
            case "PLC002":
                points = DataUtils.createPLC002DataPoints();
                break;
            case "RTU_104_001":
                points = DataUtils.createRTU104001Points();
                break;
            case "OPCUA_DEVICE_001":
                points = DataUtils.createOPCUADEVICE001Points();
                break;
            default:
                points = DataUtils.createPLC001DataPoints();
        }

        log.info("为设�?{} 生成 {} 个模拟数据点", deviceId, points.size());
        return points;
    }


    /**
     * 创建PLC设备的模拟数据点
     */
    public static List<DataPoint> createPLC001DataPoints() {
        return JsonDataPointLoader.loadDataPointsFromJson("mock/modbus-PLC001-points.json");
    }
    /**
     * 创建PLC设备的模拟数据点
     */
    public static List<DataPoint> createPLC002DataPoints() {
        return JsonDataPointLoader.loadDataPointsFromJson("mock/modbus-PLC002-points.json");
    }

    /**
     * 创建104协议模拟数据�?
     * @return
     */
    private static List<DataPoint> createRTU104001Points() {
        return JsonDataPointLoader.loadDataPointsFromJson("mock/IEC104-RTU104001-points.json");
    }

    private static List<DataPoint> createOPCUADEVICE001Points() {
        return JsonDataPointLoader.loadDataPointsFromJson("mock/OPCUA-points.json");
    }



    /**
     * 创建模拟连接信息
     * 根据设备ID生成对应的连接配�?
     */
    public static DeviceConnection createMockDeviceConnection(String deviceId) {
        DeviceConnection connection = new DeviceConnection();
        Date now = new Date();

        // 基础信息
        connection.setId(System.currentTimeMillis());
        connection.setConnectionId("CONN_" + deviceId + "_" + System.currentTimeMillis());
        connection.setDeviceId(deviceId);

        // 根据设备类型设置不同的连接配�?
        switch (deviceId) {
            case "PLC001":
                setupPLCConnection(connection);
                break;
            case "DCS001":
                setupOPCUaConnection(connection);
                break;
            case "SWITCH001":
                setupSNMPConnection(connection);
                break;
            case "IOT001":
                setupMQTTConnection(connection);
                break;
            case "TEST001":
                setupTestConnection(connection);
                break;
            default:
                setupDefaultConnection(connection, deviceId);
        }

        // 状态信�?
        connection.setStatus("CONNECTED");
        connection.setConnectTime(new Date(now.getTime() - 3600000)); // 1小时前连�?
        connection.setLastHeartbeatTime(now);
        connection.setLastDataTime(now);
        connection.setRetryCount(0);
        connection.calculateDuration();

        // 系统时间
        connection.setCreateTime(now);
        connection.setUpdateTime(now);

        log.info("为设�?{} 生成模拟连接配置: {}", deviceId, connection.getConnectionId());
        return connection;
    }

    /**
     * 设置PLC（Modbus TCP）连接配�?
     */
    public static void setupPLCConnection(DeviceConnection connection) {
        connection.setDeviceName("西门子S7-1200 PLC");
        connection.setConnectionType("TCP");
        connection.setProtocolType("MODBUS_TCP");
        /*connection.setRemoteAddress("192.168.1.100");
        connection.setRemotePort(502);
        connection.setLocalAddress("192.168.1.10");
        connection.setLocalPort(52000);

        // Modbus 连接参数
        Map<String, Object> params = new HashMap<>();
        params.put("slaveId", 1);
        params.put("timeout", 3000);
        params.put("retries", 3);
        params.put("byteOrder", "ABCD");
        params.put("unitId", 1);
        connection.setConnectionParams(params);*/

        // 连接统计
        setupConnectionStats(connection);
    }

    /**
     * 设置OPC UA连接配置
     */
    public static void setupOPCUaConnection(DeviceConnection connection) {
        connection.setDeviceName("中控DCS系统");
        connection.setConnectionType("TCP");
        connection.setProtocolType("OPC_UA");
        /*connection.setRemoteAddress("192.168.1.101");
        connection.setRemotePort(4840);

        Map<String, Object> params = new HashMap<>();
        params.put("endpointUrl", "opc.tcp://192.168.1.101:4840");
        params.put("securityPolicy", "None");
        params.put("messageMode", "Binary");
        params.put("sessionTimeout", 60000);
        params.put("requestTimeout", 5000);
        params.put("username", "admin");
        params.put("password", "123456");
        connection.setConnectionParams(params);*/

        setupConnectionStats(connection);
    }

    /**
     * 设置SNMP连接配置
     */
    public static void setupSNMPConnection(DeviceConnection connection) {
        connection.setDeviceName("华为交换�?");
        connection.setConnectionType("UDP");
        connection.setProtocolType("SNMP");
        /*connection.setRemoteAddress("192.168.1.102");
        connection.setRemotePort(161);
        connection.setLocalPort(162);

        Map<String, Object> params = new HashMap<>();
        params.put("community", "public");
        params.put("version", "2c");
        params.put("timeout", 5000);
        params.put("retries", 2);
        params.put("maxRepetitions", 10);
        connection.setConnectionParams(params);*/

        setupConnectionStats(connection);
    }

    /**
     * 设置MQTT连接配置
     */
    public static void setupMQTTConnection(DeviceConnection connection) {
        connection.setDeviceName("温湿度传感器");
        connection.setConnectionType("TCP");
        connection.setProtocolType("MQTT");
        /*connection.setRemoteAddress("192.168.1.103");
        connection.setRemotePort(1883);

        Map<String, Object> params = new HashMap<>();
        params.put("clientId", "collector_" + System.currentTimeMillis());
        params.put("topic", "sensor/data");
        params.put("qos", 1);
        params.put("cleanSession", true);
        params.put("connectionTimeout", 30);
        params.put("keepAliveInterval", 60);
        params.put("username", "iot_user");
        params.put("password", "iot_password");
        connection.setConnectionParams(params);*/

        setupConnectionStats(connection);
    }

    /**
     * 设置测试连接配置
     */
    public static void setupTestConnection(DeviceConnection connection) {
        connection.setDeviceName("测试设备");
        connection.setConnectionType("TCP");
        connection.setProtocolType("MODBUS_TCP");
        /*connection.setRemoteAddress("192.168.1.199");
        connection.setRemotePort(502);

        Map<String, Object> params = new HashMap<>();
        params.put("slaveId", 2);
        params.put("timeout", 3000);
        params.put("retries", 2);
        connection.setConnectionParams(params);*/

        // 测试设备设置为断开状�?
        connection.setStatus("DISCONNECTED");
        connection.setDisconnectTime(new Date());
        connection.setLastError("连接超时");
        connection.setRetryCount(2);

        setupConnectionStats(connection);
    }

    /**
     * 设置默认连接配置
     */
    public static void setupDefaultConnection(DeviceConnection connection, String deviceId) {
        connection.setDeviceName("未知设备-" + deviceId);
        connection.setConnectionType("TCP");
        connection.setProtocolType("MODBUS_TCP");
        /*connection.setRemoteAddress("192.168.1.200");
        connection.setRemotePort(502);

        Map<String, Object> params = new HashMap<>();
        params.put("slaveId", 1);
        params.put("timeout", 3000);
        connection.setConnectionParams(params);*/

        setupConnectionStats(connection);
    }

    /**
     * 设置连接统计信息
     */
    public static void setupConnectionStats(DeviceConnection connection) {
        DeviceConnection.ConnectionStats stats = new DeviceConnection.ConnectionStats();

        // 模拟一些统计数�?
        Random random = new Random();
        stats.setTotalBytesSent(100000L + random.nextInt(900000));
        stats.setTotalBytesReceived(200000L + random.nextInt(800000));
        stats.setTotalMessagesSent(500L + random.nextInt(1500));
        stats.setTotalMessagesReceived(600L + random.nextInt(1400));
        stats.setTotalErrors((long) random.nextInt(50));
        stats.setAverageResponseTime(50.0 + random.nextDouble() * 100);
        stats.setMaxResponseTime(100.0 + random.nextDouble() * 200);
        stats.setLastResponseTime(new Date());

        Map<String, Object> statsMap = new HashMap<>();
        statsMap.put("bytesSent", stats.getTotalBytesSent());
        statsMap.put("bytesReceived", stats.getTotalBytesReceived());
        statsMap.put("messagesSent", stats.getTotalMessagesSent());
        statsMap.put("messagesReceived", stats.getTotalMessagesReceived());
        statsMap.put("errors", stats.getTotalErrors());
        statsMap.put("successRate", stats.getSuccessRate());
        statsMap.put("avgResponseTime", stats.getAverageResponseTime());
        statsMap.put("lastResponseTime", stats.getLastResponseTime());

        connection.setStats(statsMap);
    }

}
