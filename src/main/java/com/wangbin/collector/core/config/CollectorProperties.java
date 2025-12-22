package com.wangbin.collector.core.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * 采集器配置类
 */
@Data
@Component
@ConfigurationProperties(prefix = "collector")
public class CollectorProperties {

    /**
     * SNMP配置
     */
    private SnmpConfig snmp = new SnmpConfig();

    /**
     * OPC UA配置
     */
    private OpcUaConfig opcUa = new OpcUaConfig();

    /**
     * MQTT配置
     */
    private MqttConfig mqtt = new MqttConfig();

    /**
     * Modbus配置
     */
    private ModbusConfig modbus = new ModbusConfig();

    /**
     * CoAP配置
     */
    private CoapConfig coap = new CoapConfig();

    /**
     * IEC104配置
     */
    private Iec104Config iec104 = new Iec104Config();

    /**
     * IEC61850配置
     */
    private Iec61850Config iec61850 = new Iec61850Config();

    /**
     * 通用配置
     */
    private CommonConfig common = new CommonConfig();

    // =============== 配置类定义 ===============

    @Data
    public static class SnmpConfig {
        private String community = "public";
        private int timeout = 3000;
        private int retries = 2;
        private String version = "2c";
        private int pollingInterval = 5000;
        private Map<String, String> devices;
    }

    @Data
    public static class OpcUaConfig {
        private String securityPolicy = "None";
        private String messageMode = "Binary";
        private int requestTimeout = 5000;
        private int subscriptionInterval = 1000;
        private Map<String, String> endpoints;
    }

    @Data
    public static class MqttConfig {
        private String brokerUrl = "tcp://localhost:1883";
        private String clientId;
        private String username;
        private String password;
        private int qos = 1;
        private boolean cleanSession = true;
        private int connectionTimeout = 30;
        private int keepAliveInterval = 60;
    }

    @Data
    public static class ModbusConfig {
        private int timeout = 3000;
        private int retries = 2;
        private int poolSize = 10;
        private Map<String, String> connections;
    }

    @Data
    public static class CoapConfig {
        private int timeout = 3000;
        private int retries = 2;
        private String scheme = "coap";
        private Map<String, String> servers;
    }

    @Data
    public static class Iec104Config {
        private int timeout = 5000;
        private int retries = 3;
        private int commonAddress = 1;
        private boolean generalInterrogationOnConnect = true;
        private long generalInterrogationInterval = 600000;
        private boolean singleInterrogationOnReadMiss = true;
        private long cacheTtl = 5000;
        private String singleInterrogationGroupField = "groupId";
        private Map<String, String> stations;
    }

    @Data
    public static class Iec61850Config {
        private int timeout = 5000;
        private int retries = 3;
        private String securityPolicy = "None";
        private Map<String, String> servers;
    }

    @Data
    public static class CommonConfig {
        private int heartbeatInterval = 30000;
        private int reconnectInterval = 5000;
        private int maxReconnectTimes = 3;
        private int dataCacheSize = 10000;
        private boolean enableMonitor = true;
        private boolean enableAlert = true;
    }
}
