package com.wangbin.collector.core.config.manager;

import com.wangbin.collector.common.domain.entity.CollectionConfig;
import com.wangbin.collector.common.domain.entity.ConnectionInfo;
import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.common.utils.JsonDataPointLoader;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

@Slf4j
public class DataUtils {

    /**
     * 创建模拟数据点
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
            case "DCS001":
                points = DataUtils.createDCSDataPoints();
                break;
            case "SWITCH001":
                points = DataUtils.createSwitchDataPoints();
                break;
            case "IOT001":
                points = DataUtils.createIoTDataPoints();
                break;
            case "TEST001":
                points = DataUtils.createTestDataPoints();
                break;
            default:
                points = DataUtils.createDefaultDataPoints(deviceId);
        }

        log.info("为设备 {} 生成 {} 个模拟数据点", deviceId, points.size());
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
     * 创建DCS设备的模拟数据点
     */
    public static List<DataPoint> createDCSDataPoints() {
        List<DataPoint> points = new ArrayList<>();

        DataPoint flow1 = new DataPoint();
        flow1.setId(4L);
        flow1.setPointId("FLOW001");
        flow1.setPointCode("FLOW001");
        flow1.setPointName("流量计1");
        flow1.setDeviceId("DCS001");
        flow1.setDeviceName("中控DCS系统");
        flow1.setAddress("ns=2;s=Flow1");
        flow1.setDataType("DOUBLE");
        flow1.setReadWrite("R");
        flow1.setUnit("m³/h");
        flow1.setCollectionMode("POLLING");
        flow1.setStatus(1);
        points.add(flow1);

        return points;
    }

    /**
     * 创建交换机设备的模拟数据点
     */
    public static List<DataPoint> createSwitchDataPoints() {
        List<DataPoint> points = new ArrayList<>();

        DataPoint cpuUsage = new DataPoint();
        cpuUsage.setId(5L);
        cpuUsage.setPointId("CPU_USAGE");
        cpuUsage.setPointCode("CPU_USAGE");
        cpuUsage.setPointName("CPU使用率");
        cpuUsage.setDeviceId("SWITCH001");
        cpuUsage.setDeviceName("华为交换机");
        cpuUsage.setAddress("1.3.6.1.4.1.2011.6.3.4.1.2.1.1.5.0");
        cpuUsage.setDataType("INT");
        cpuUsage.setReadWrite("R");
        cpuUsage.setUnit("%");
        cpuUsage.setCollectionMode("POLLING");
        cpuUsage.setStatus(1);
        points.add(cpuUsage);

        return points;
    }

    /**
     * 创建IoT设备的模拟数据点
     */
    public static List<DataPoint> createIoTDataPoints() {
        List<DataPoint> points = new ArrayList<>();

        DataPoint temp = new DataPoint();
        temp.setId(6L);
        temp.setPointId("IOT_TEMP");
        temp.setPointCode("IOT_TEMP");
        temp.setPointName("环境温度");
        temp.setDeviceId("IOT001");
        temp.setDeviceName("温湿度传感器");
        temp.setAddress("sensor/temperature");
        temp.setDataType("FLOAT");
        temp.setReadWrite("R");
        temp.setUnit("℃");
        temp.setCollectionMode("SUBSCRIPTION");
        temp.setStatus(1);
        points.add(temp);

        DataPoint humidity = new DataPoint();
        humidity.setId(7L);
        humidity.setPointId("IOT_HUM");
        humidity.setPointCode("IOT_HUM");
        humidity.setPointName("环境湿度");
        humidity.setDeviceId("IOT001");
        humidity.setDeviceName("温湿度传感器");
        humidity.setAddress("sensor/humidity");
        humidity.setDataType("FLOAT");
        humidity.setReadWrite("R");
        humidity.setUnit("%");
        humidity.setCollectionMode("SUBSCRIPTION");
        humidity.setStatus(1);
        points.add(humidity);

        return points;
    }

    /**
     * 创建测试设备的模拟数据点
     */
    public static List<DataPoint> createTestDataPoints() {
        List<DataPoint> points = new ArrayList<>();

        DataPoint test1 = new DataPoint();
        test1.setId(8L);
        test1.setPointId("TEST001");
        test1.setPointCode("TEST001");
        test1.setPointName("测试数据点");
        test1.setDeviceId("TEST001");
        test1.setDeviceName("测试设备");
        test1.setAddress("40001");
        test1.setDataType("FLOAT");
        test1.setReadWrite("R");
        test1.setStatus(1);
        points.add(test1);

        return points;
    }

    /**
     * 默认数据点
     */
    public static List<DataPoint> createDefaultDataPoints(String deviceId) {
        List<DataPoint> points = new ArrayList<>();

        DataPoint point = new DataPoint();
        point.setId(9L);
        point.setPointId(deviceId + "_001");
        point.setPointCode(deviceId + "_001");
        point.setPointName("默认数据点");
        point.setDeviceId(deviceId);
        point.setDeviceName("未知设备");
        point.setAddress("0");
        point.setDataType("FLOAT");
        point.setReadWrite("R");
        point.setStatus(1);
        points.add(point);

        return points;
    }




    /**
     * 创建模拟连接信息
     * 根据设备ID生成对应的连接配置
     */
    public static ConnectionInfo createMockConnectionInfo(String deviceId) {
        ConnectionInfo connection = new ConnectionInfo();
        Date now = new Date();

        // 基础信息
        connection.setId(System.currentTimeMillis());
        connection.setConnectionId("CONN_" + deviceId + "_" + System.currentTimeMillis());
        connection.setDeviceId(deviceId);

        // 根据设备类型设置不同的连接配置
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

        // 状态信息
        connection.setStatus("CONNECTED");
        connection.setConnectTime(new Date(now.getTime() - 3600000)); // 1小时前连接
        connection.setLastHeartbeatTime(now);
        connection.setLastDataTime(now);
        connection.setRetryCount(0);
        connection.calculateDuration();

        // 系统时间
        connection.setCreateTime(now);
        connection.setUpdateTime(now);

        log.info("为设备 {} 生成模拟连接配置: {}", deviceId, connection.getConnectionId());
        return connection;
    }

    /**
     * 设置PLC（Modbus TCP）连接配置
     */
    public static void setupPLCConnection(ConnectionInfo connection) {
        connection.setDeviceName("西门子S7-1200 PLC");
        connection.setConnectionType("TCP");
        connection.setProtocolType("MODBUS_TCP");
        connection.setRemoteAddress("192.168.1.100");
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
        connection.setConnectionParams(params);

        // 连接统计
        setupConnectionStats(connection);
    }

    /**
     * 设置OPC UA连接配置
     */
    public static void setupOPCUaConnection(ConnectionInfo connection) {
        connection.setDeviceName("中控DCS系统");
        connection.setConnectionType("TCP");
        connection.setProtocolType("OPC_UA");
        connection.setRemoteAddress("192.168.1.101");
        connection.setRemotePort(4840);

        Map<String, Object> params = new HashMap<>();
        params.put("endpointUrl", "opc.tcp://192.168.1.101:4840");
        params.put("securityPolicy", "None");
        params.put("messageMode", "Binary");
        params.put("sessionTimeout", 60000);
        params.put("requestTimeout", 5000);
        params.put("username", "admin");
        params.put("password", "123456");
        connection.setConnectionParams(params);

        setupConnectionStats(connection);
    }

    /**
     * 设置SNMP连接配置
     */
    public static void setupSNMPConnection(ConnectionInfo connection) {
        connection.setDeviceName("华为交换机");
        connection.setConnectionType("UDP");
        connection.setProtocolType("SNMP");
        connection.setRemoteAddress("192.168.1.102");
        connection.setRemotePort(161);
        connection.setLocalPort(162);

        Map<String, Object> params = new HashMap<>();
        params.put("community", "public");
        params.put("version", "2c");
        params.put("timeout", 5000);
        params.put("retries", 2);
        params.put("maxRepetitions", 10);
        connection.setConnectionParams(params);

        setupConnectionStats(connection);
    }

    /**
     * 设置MQTT连接配置
     */
    public static void setupMQTTConnection(ConnectionInfo connection) {
        connection.setDeviceName("温湿度传感器");
        connection.setConnectionType("TCP");
        connection.setProtocolType("MQTT");
        connection.setRemoteAddress("192.168.1.103");
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
        connection.setConnectionParams(params);

        setupConnectionStats(connection);
    }

    /**
     * 设置测试连接配置
     */
    public static void setupTestConnection(ConnectionInfo connection) {
        connection.setDeviceName("测试设备");
        connection.setConnectionType("TCP");
        connection.setProtocolType("MODBUS_TCP");
        connection.setRemoteAddress("192.168.1.199");
        connection.setRemotePort(502);

        Map<String, Object> params = new HashMap<>();
        params.put("slaveId", 2);
        params.put("timeout", 3000);
        params.put("retries", 2);
        connection.setConnectionParams(params);

        // 测试设备设置为断开状态
        connection.setStatus("DISCONNECTED");
        connection.setDisconnectTime(new Date());
        connection.setLastError("连接超时");
        connection.setRetryCount(2);

        setupConnectionStats(connection);
    }

    /**
     * 设置默认连接配置
     */
    public static void setupDefaultConnection(ConnectionInfo connection, String deviceId) {
        connection.setDeviceName("未知设备-" + deviceId);
        connection.setConnectionType("TCP");
        connection.setProtocolType("MODBUS_TCP");
        connection.setRemoteAddress("192.168.1.200");
        connection.setRemotePort(502);

        Map<String, Object> params = new HashMap<>();
        params.put("slaveId", 1);
        params.put("timeout", 3000);
        connection.setConnectionParams(params);

        setupConnectionStats(connection);
    }

    /**
     * 设置连接统计信息
     */
    public static void setupConnectionStats(ConnectionInfo connection) {
        ConnectionInfo.ConnectionStats stats = new ConnectionInfo.ConnectionStats();

        // 模拟一些统计数据
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







    /**
     * 创建模拟采集配置
     * 根据设备类型生成对应的采集配置
     */
    public static CollectionConfig createMockCollectionConfig(String deviceId) {
        CollectionConfig config = new CollectionConfig();
        Date now = new Date();

        // 基础信息
        config.setId(System.currentTimeMillis());
        config.setConfigId("COLL_CONFIG_" + deviceId + "_" + System.currentTimeMillis());
        config.setConfigName(getDeviceConfigName(deviceId));
        config.setConfigType("DEVICE");
        config.setTargetId(deviceId);

        // 根据设备类型设置不同的配置
        switch (deviceId) {
            case "PLC001":
                setupPLCCollectionConfig(config);
                break;
            case "DCS001":
                setupOPCUaCollectionConfig(config);
                break;
            case "SWITCH001":
                setupSNMPCollectionConfig(config);
                break;
            case "IOT001":
                setupMQTTCollectionConfig(config);
                break;
            case "TEST001":
                setupTestCollectionConfig(config);
                break;
            default:
                setupDefaultCollectionConfig(config, deviceId);
        }

        // 状态和版本信息
        config.setStatus(1); // 启用状态
        config.setEffectiveTime(new Date(now.getTime() - 86400000)); // 1天前生效
        config.setExpireTime(new Date(now.getTime() + 30L * 86400000)); // 30天后过期
        config.setVersion(1);
        config.setLastOperator("system_admin");

        // 系统时间
        config.setCreateTime(now);
        config.setUpdateTime(now);
        config.setRemark("模拟采集配置，用于测试");

        log.info("为设备 {} 生成模拟采集配置: {}", deviceId, config.getConfigId());
        return config;
    }

    /**
     * 获取设备配置名称
     */
    public static String getDeviceConfigName(String deviceId) {
        switch (deviceId) {
            case "PLC001": return "西门子PLC-高速采集配置";
            case "DCS001": return "OPC UA-批量采集配置";
            case "SWITCH001": return "SNMP-网络监控配置";
            case "IOT001": return "MQTT-物联网传感器配置";
            case "TEST001": return "测试设备-基础配置";
            default: return deviceId + "-默认采集配置";
        }
    }

    /**
     * 设置PLC设备的采集配置
     */
    public static void setupPLCCollectionConfig(CollectionConfig config) {
        // 设备基础信息
        config.setDeviceName("西门子PLC-S7-1500");
        config.setDeviceIp("192.168.1.100");
        config.setDevicePort(502);
        config.setDeviceProtocolType("MODBUS");
        config.setDeviceAlias("主生产线控制器");
        config.setCollectionInterval(1); // 1秒
        config.setDeviceLocation("生产车间-A区-01号位");
        config.setProductModel("S7-1500");
        config.setDepartment("生产部");

        // 协议配置
        config.setProtocolType("MODBUS");
        config.setConnectionType("TCP");

        // 协议参数
        Map<String, Object> protocolParams = new HashMap<>();
        protocolParams.put("slaveId", 1);
        protocolParams.put("functionCode", 3); // 读取保持寄存器
        protocolParams.put("registerType", "HOLDING_REGISTER");
        protocolParams.put("byteOrder", "ABCD");
        protocolParams.put("timeout", 3000);
        protocolParams.put("retryCount", 3);
        protocolParams.put("coilAddressStart", 0);
        protocolParams.put("coilAddressEnd", 100);
        config.setProtocolParams(protocolParams);

        // 连接参数
        Map<String, Object> connectionParams = new HashMap<>();
        connectionParams.put("host", "192.168.1.100");
        connectionParams.put("port", 502);
        connectionParams.put("reconnectInterval", 5000);
        connectionParams.put("maxRetries", 3);
        connectionParams.put("keepAlive", true);
        connectionParams.put("socketTimeout", 5000);
        config.setConnectionParams(connectionParams);

        // 采集参数
        Map<String, Object> collectionParams = new HashMap<>();
        collectionParams.put("collectionInterval", 1000); // 1秒
        collectionParams.put("batchSize", 10);
        collectionParams.put("enableBatchRead", true);
        collectionParams.put("readTimeout", 2000);
        collectionParams.put("maxPointsPerRequest", 125);
        collectionParams.put("enableOptimization", true);
        config.setCollectionParams(collectionParams);

        // 上报参数
        Map<String, Object> reportParams = new HashMap<>();
        reportParams.put("reportInterval", 5000); // 5秒上报一次
        reportParams.put("batchReportSize", 100);
        reportParams.put("reportMode", "BATCH");
        reportParams.put("enableCompression", true);
        reportParams.put("dataFormat", "JSON");
        reportParams.put("enableRealTimeAlert", true);
        config.setReportParams(reportParams);

        // 处理参数
        Map<String, Object> processParams = new HashMap<>();
        processParams.put("enableDataValidation", true);
        processParams.put("enableDeadband", true);
        processParams.put("deadbandValue", 0.5);
        processParams.put("enableDataCaching", true);
        processParams.put("dataCacheSize", 1000);
        processParams.put("enableHistoryStorage", true);
        config.setProcessParams(processParams);

        // 配置项
        List<CollectionConfig.ConfigItem> configItems = new ArrayList<>();

        // 采集间隔配置
        CollectionConfig.ConfigItem intervalItem = new CollectionConfig.ConfigItem();
        intervalItem.setKey("collection.interval");
        intervalItem.setName("采集间隔");
        intervalItem.setValue("1000");
        intervalItem.setDataType("INTEGER");
        intervalItem.setUnit("ms");
        intervalItem.setDescription("数据采集的时间间隔，单位毫秒");
        intervalItem.setEditable(1);
        intervalItem.setRequired(1);
        intervalItem.setValidationRule("^[1-9][0-9]*$");
        intervalItem.setDefaultValue("1000");
        configItems.add(intervalItem);

        // 批量大小配置
        CollectionConfig.ConfigItem batchItem = new CollectionConfig.ConfigItem();
        batchItem.setKey("batch.size");
        batchItem.setName("批量大小");
        batchItem.setValue("10");
        batchItem.setDataType("INTEGER");
        batchItem.setDescription("每次采集的数据点数量");
        intervalItem.setEditable(1);
        intervalItem.setRequired(0);
        intervalItem.setValidationRule("^[1-9][0-9]*$");
        intervalItem.setDefaultValue("10");
        configItems.add(batchItem);

        // 超时配置
        CollectionConfig.ConfigItem timeoutItem = new CollectionConfig.ConfigItem();
        timeoutItem.setKey("read.timeout");
        timeoutItem.setName("读取超时");
        timeoutItem.setValue("2000");
        timeoutItem.setDataType("INTEGER");
        timeoutItem.setUnit("ms");
        timeoutItem.setDescription("读取数据的超时时间");
        timeoutItem.setEditable(1);
        timeoutItem.setRequired(0);
        timeoutItem.setValidationRule("^[1-9][0-9]*$");
        timeoutItem.setDefaultValue("2000");
        configItems.add(timeoutItem);

        // 寄存器配置
        CollectionConfig.ConfigItem registerItem = new CollectionConfig.ConfigItem();
        registerItem.setKey("register.start");
        registerItem.setName("起始寄存器");
        registerItem.setValue("0");
        registerItem.setDataType("INTEGER");
        registerItem.setDescription("Modbus寄存器起始地址");
        registerItem.setEditable(1);
        registerItem.setRequired(1);
        registerItem.setValidationRule("^[0-9]+$");
        registerItem.setDefaultValue("0");
        configItems.add(registerItem);

        config.setConfigItems(configItems);
    }

    /**
     * 设置OPC UA设备的采集配置
     */
    public static void setupOPCUaCollectionConfig(CollectionConfig config) {
        // 设备基础信息
        config.setDeviceName("西门子DCS系统");
        config.setDeviceIp("192.168.1.101");
        config.setDevicePort(4840);
        config.setDeviceProtocolType("OPC_UA");
        config.setDeviceAlias("分布式控制系统");
        config.setCollectionInterval(2); // 2秒
        config.setDeviceLocation("控制中心-主控室");
        config.setProductModel("PCS7");
        config.setDepartment("自动化部");

        // 协议配置
        config.setProtocolType("OPC_UA");
        config.setConnectionType("TCP");

        // 协议参数
        Map<String, Object> protocolParams = new HashMap<>();
        protocolParams.put("endpointUrl", "opc.tcp://192.168.1.101:4840");
        protocolParams.put("securityPolicy", "None");
        protocolParams.put("messageMode", "Binary");
        protocolParams.put("sessionTimeout", 60000);
        protocolParams.put("requestTimeout", 5000);
        protocolParams.put("samplingInterval", 1000);
        protocolParams.put("queueSize", 10);
        protocolParams.put("publishingEnabled", true);
        config.setProtocolParams(protocolParams);

        // 连接参数
        Map<String, Object> connectionParams = new HashMap<>();
        connectionParams.put("host", "192.168.1.101");
        connectionParams.put("port", 4840);
        connectionParams.put("reconnectInterval", 10000);
        connectionParams.put("maxRetries", 5);
        connectionParams.put("securityMode", "None");
        connectionParams.put("userName", "admin");
        connectionParams.put("password", "password123");
        config.setConnectionParams(connectionParams);

        // 采集参数
        Map<String, Object> collectionParams = new HashMap<>();
        collectionParams.put("collectionInterval", 2000); // 2秒
        collectionParams.put("subscriptionInterval", 1000);
        collectionParams.put("queueSize", 100);
        collectionParams.put("maxNotificationsPerPublish", 1000);
        collectionParams.put("enableMonitoredItems", true);
        config.setCollectionParams(collectionParams);

        // 上报参数
        Map<String, Object> reportParams = new HashMap<>();
        reportParams.put("reportInterval", 10000); // 10秒上报一次
        reportParams.put("batchReportSize", 500);
        reportParams.put("reportMode", "REALTIME");
        reportParams.put("enableCompression", false);
        reportParams.put("dataFormat", "PROTOBUF");
        config.setReportParams(reportParams);

        // 处理参数
        Map<String, Object> processParams = new HashMap<>();
        processParams.put("enableDataValidation", true);
        processParams.put("enableQualityCheck", true);
        processParams.put("enableTimestampSync", true);
        processParams.put("enableDataCaching", true);
        processParams.put("dataCacheSize", 5000);
        config.setProcessParams(processParams);
    }

    /**
     * 设置SNMP设备的采集配置
     */
    public static void setupSNMPCollectionConfig(CollectionConfig config) {
        // 设备基础信息
        config.setDeviceName("华为交换机-S5720");
        config.setDeviceIp("192.168.1.102");
        config.setDevicePort(161);
        config.setDeviceProtocolType("SNMP");
        config.setDeviceAlias("核心网络交换机");
        config.setCollectionInterval(30); // 30秒
        config.setDeviceLocation("机房-机柜A-03");
        config.setProductModel("S5720-52X-PWR-SI");
        config.setDepartment("IT部");

        // 协议配置
        config.setProtocolType("SNMP");
        config.setConnectionType("UDP");

        // 协议参数
        Map<String, Object> protocolParams = new HashMap<>();
        protocolParams.put("version", "v2c");
        protocolParams.put("community", "public");
        protocolParams.put("timeout", 3000);
        protocolParams.put("retries", 2);
        protocolParams.put("maxRepetitions", 10);
        protocolParams.put("nonRepeaters", 0);
        config.setProtocolParams(protocolParams);

        // 连接参数
        Map<String, Object> connectionParams = new HashMap<>();
        connectionParams.put("host", "192.168.1.102");
        connectionParams.put("port", 161);
        connectionParams.put("localAddress", "0.0.0.0");
        connectionParams.put("localPort", 0);
        connectionParams.put("receiveBufferSize", 65535);
        config.setConnectionParams(connectionParams);

        // 采集参数
        Map<String, Object> collectionParams = new HashMap<>();
        collectionParams.put("collectionInterval", 30000); // 30秒
        collectionParams.put("oidList", Arrays.asList(
                "1.3.6.1.2.1.1.1.0", // sysDescr
                "1.3.6.1.2.1.1.3.0", // sysUpTime
                "1.3.6.1.2.1.2.2.1.10", // ifInOctets
                "1.3.6.1.2.1.2.2.1.16"  // ifOutOctets
        ));
        collectionParams.put("enableBulkWalk", true);
        collectionParams.put("walkTimeout", 10000);
        config.setCollectionParams(collectionParams);

        // 上报参数
        Map<String, Object> reportParams = new HashMap<>();
        reportParams.put("reportInterval", 60000); // 60秒上报一次
        reportParams.put("enableTrap", true);
        reportParams.put("trapReceiver", "192.168.1.10");
        reportParams.put("trapPort", 162);
        reportParams.put("enableThresholdAlert", true);
        config.setReportParams(reportParams);
    }

    /**
     * 设置MQTT设备的采集配置
     */
    public static void setupMQTTCollectionConfig(CollectionConfig config) {
        // 设备基础信息
        config.setDeviceName("温湿度传感器-IOT");
        config.setDeviceIp("192.168.1.103");
        config.setDevicePort(1883);
        config.setDeviceProtocolType("MQTT");
        config.setDeviceAlias("环境监测传感器");
        config.setCollectionInterval(5); // 5秒
        config.setDeviceLocation("办公室-三楼-305室");
        config.setProductModel("THS-01");
        config.setDepartment("后勤部");

        // 协议配置
        config.setProtocolType("MQTT");
        config.setConnectionType("TCP");

        // 协议参数
        Map<String, Object> protocolParams = new HashMap<>();
        protocolParams.put("clientId", "collector_client_" + config.getConfigId());
        protocolParams.put("cleanSession", true);
        protocolParams.put("keepAliveInterval", 60);
        protocolParams.put("connectionTimeout", 10);
        protocolParams.put("qos", 1);
        protocolParams.put("retain", false);
        config.setProtocolParams(protocolParams);

        // 连接参数
        Map<String, Object> connectionParams = new HashMap<>();
        connectionParams.put("brokerUrl", "tcp://192.168.1.103:1883");
        connectionParams.put("username", "iot_user");
        connectionParams.put("password", "iot_pass");
        connectionParams.put("sslEnabled", false);
        connectionParams.put("autoReconnect", true);
        config.setConnectionParams(connectionParams);

        // 采集参数
        Map<String, Object> collectionParams = new HashMap<>();
        collectionParams.put("collectionInterval", 5000); // 5秒
        collectionParams.put("topics", Arrays.asList(
                "/sensor/temperature",
                "/sensor/humidity",
                "/sensor/pressure"
        ));
        collectionParams.put("enableSubscribe", true);
        collectionParams.put("messageQueueSize", 100);
        config.setCollectionParams(collectionParams);

        // 上报参数
        Map<String, Object> reportParams = new HashMap<>();
        reportParams.put("reportInterval", 30000); // 30秒上报一次
        reportParams.put("batchReportSize", 50);
        reportParams.put("reportTopic", "/collector/data");
        reportParams.put("qos", 1);
        reportParams.put("retain", false);
        config.setReportParams(reportParams);
    }

    /**
     * 设置测试设备的采集配置
     */
    public static void setupTestCollectionConfig(CollectionConfig config) {
        // 设备基础信息
        config.setDeviceName("测试设备");
        config.setDeviceIp("127.0.0.1");
        config.setDevicePort(8080);
        config.setDeviceProtocolType("HTTP");
        config.setDeviceAlias("开发测试设备");
        config.setCollectionInterval(10); // 10秒
        config.setDeviceLocation("研发办公室");
        config.setProductModel("TEST-001");
        config.setDepartment("研发部");

        // 协议配置
        config.setProtocolType("HTTP");
        config.setConnectionType("TCP");

        // 协议参数
        Map<String, Object> protocolParams = new HashMap<>();
        protocolParams.put("method", "GET");
        protocolParams.put("contentType", "application/json");
        protocolParams.put("charset", "UTF-8");
        protocolParams.put("timeout", 5000);
        protocolParams.put("followRedirects", true);
        config.setProtocolParams(protocolParams);

        // 连接参数
        Map<String, Object> connectionParams = new HashMap<>();
        connectionParams.put("url", "http://127.0.0.1:8080/api/data");
        connectionParams.put("headers", Map.of(
                "User-Agent", "DataCollector/1.0",
                "Accept", "application/json"
        ));
        connectionParams.put("sslVerify", false);
        config.setConnectionParams(connectionParams);

        // 采集参数
        Map<String, Object> collectionParams = new HashMap<>();
        collectionParams.put("collectionInterval", 10000); // 10秒
        collectionParams.put("retryCount", 2);
        collectionParams.put("enableMockData", true);
        config.setCollectionParams(collectionParams);
    }

    /**
     * 设置默认采集配置
     */
    public static void setupDefaultCollectionConfig(CollectionConfig config, String deviceId) {
        // 设备基础信息
        config.setDeviceName("通用设备-" + deviceId);
        config.setDeviceIp("192.168.1.200");
        config.setDevicePort(80);
        config.setDeviceProtocolType("HTTP");
        config.setDeviceAlias(deviceId + "-设备");
        config.setCollectionInterval(15); // 15秒
        config.setDeviceLocation("未指定位置");
        config.setProductModel("GENERIC");
        config.setDepartment("其他部门");

        // 协议配置
        config.setProtocolType("HTTP");
        config.setConnectionType("TCP");

        // 协议参数
        Map<String, Object> protocolParams = new HashMap<>();
        protocolParams.put("method", "GET");
        protocolParams.put("timeout", 3000);
        config.setProtocolParams(protocolParams);

        // 连接参数
        Map<String, Object> connectionParams = new HashMap<>();
        connectionParams.put("host", "192.168.1.200");
        connectionParams.put("port", 80);
        connectionParams.put("path", "/api/collect");
        config.setConnectionParams(connectionParams);

        // 采集参数
        Map<String, Object> collectionParams = new HashMap<>();
        collectionParams.put("collectionInterval", 15000); // 15秒
        collectionParams.put("retryCount", 3);
        config.setCollectionParams(collectionParams);

        // 上报参数
        Map<String, Object> reportParams = new HashMap<>();
        reportParams.put("reportInterval", 60000); // 60秒
        reportParams.put("batchReportSize", 50);
        config.setReportParams(reportParams);
    }

}
