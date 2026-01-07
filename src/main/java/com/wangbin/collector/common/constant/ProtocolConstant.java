package com.wangbin.collector.common.constant;

public class ProtocolConstant {

    // 协议类型
    public static final String PROTOCOL_MODBUS_TCP = "MODBUS_TCP";
    public static final String PROTOCOL_MODBUS_RTU = "MODBUS_RTU";
    public static final String PROTOCOL_OPC_DA = "OPC_DA";
    public static final String PROTOCOL_OPC_UA = "OPC_UA";
    public static final String PROTOCOL_SNMP = "SNMP";
    public static final String PROTOCOL_MQTT = "MQTT";
    public static final String PROTOCOL_COAP = "COAP";
    public static final String PROTOCOL_IEC104 = "IEC104";
    public static final String PROTOCOL_IEC61850 = "IEC61850";
    public static final String PROTOCOL_HTTP = "HTTP";
    public static final String PROTOCOL_TCP = "TCP";
    public static final String PROTOCOL_WEBSOCKET = "WEBSOCKET";

    // 默认端口
    public static final int DEFAULT_MODBUS_TCP_PORT = 502;
    public static final int DEFAULT_OPC_UA_PORT = 4840;
    public static final int DEFAULT_SNMP_PORT = 161;
    public static final int DEFAULT_MQTT_PORT = 1883;
    public static final int DEFAULT_COAP_PORT = 5683;
    public static final int DEFAULT_IEC104_PORT = 2404;
    public static final int DEFAULT_HTTP_PORT = 80;
    public static final int DEFAULT_HTTPS_PORT = 443;

    // TCP物联网协议默认端口
    public static final int DEFAULT_IOT_TCP_PORT = 1883;

    // 采集模式
    public static final String COLLECT_MODE_POLLING = "POLLING";
    public static final String COLLECT_MODE_SUBSCRIPTION = "SUBSCRIPTION";
    public static final String COLLECT_MODE_EVENT = "EVENT";

    // 数据上报模式
    public static final String REPORT_MODE_REALTIME = "REALTIME";
    public static final String REPORT_MODE_BATCH = "BATCH";
    public static final String REPORT_MODE_COMPRESSED = "COMPRESSED";

    // ==================== 配置相关常量 ====================

    // 超时配置
    public static final int DEFAULT_CONNECT_TIMEOUT_MS = 5000;    // 连接超时默认5秒
    public static final int DEFAULT_READ_TIMEOUT_MS = 10000;      // 读取超时默认10秒
    public static final int DEFAULT_MAX_RETRY_COUNT = 3;          // 默认最大重试次数
    public static final long DEFAULT_RETRY_INTERVAL_MS = 1000L;   // 默认重试间隔1秒

    // MQTT配置常量
    public static final String MQTT_PARAM_USERNAME = "username";
    public static final String MQTT_PARAM_PASSWORD = "password";
    public static final String MQTT_PARAM_CLIENT_ID = "clientId";
    public static final String MQTT_PARAM_CLEAN_SESSION = "cleanSession";
    public static final String MQTT_PARAM_KEEP_ALIVE = "keepAliveInterval";
    public static final String MQTT_PARAM_SSL_ENABLED = "sslEnabled";
    public static final String MQTT_PARAM_WILL_TOPIC = "willTopic";
    public static final String MQTT_PARAM_WILL_MESSAGE = "willMessage";
    public static final String MQTT_PARAM_WILL_QOS = "willQos";
    public static final String MQTT_PARAM_WILL_RETAINED = "willRetained";
    public static final String MQTT_PARAM_PUBLISH_TOPIC = "publishTopic";
    public static final String MQTT_PARAM_SUBSCRIBE_TOPICS = "subscribeTopics";
    public static final String MQTT_PARAM_ACK_TOPIC_TEMPLATE = "ackTopicTemplate";
    public static final String MQTT_PARAM_ACK_TIMEOUT = "ackTimeoutMs";
    public static final int DEFAULT_MQTT_ACK_TIMEOUT_MS = 5000;

    // TCP物联网协议配置常量
    public static final String TCP_PARAM_FORMAT = "format";           // json 或 binary
    public static final String TCP_PARAM_MESSAGE_TYPE = "messageType"; // 消息类型
    public static final String TCP_PARAM_PRODUCT_KEY = "productKey";
    public static final String TCP_PARAM_DEVICE_NAME = "deviceName";

    // HTTP配置常量
    public static final String HTTP_PARAM_METHOD = "method";          // GET, POST等
    public static final String HTTP_PARAM_HEADERS = "headers";
    public static final String HTTP_PARAM_DATA_FORMAT = "dataFormat"; // JSON, FORM, XML

    // 通用配置常量
    public static final String CONFIG_PARAM_QOS = "qos";              // QoS等级
    public static final String CONFIG_PARAM_RETAINED = "retained";    // 是否保留消息
    public static final String CONFIG_PARAM_BATCH_SIZE = "batchSize"; // 批量大小
}
