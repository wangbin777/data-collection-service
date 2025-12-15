package com.wangbin.collector.common.domain.enums;

/**
 * 协议类型枚举
 */
public enum ProtocolType {

    // Modbus协议
    MODBUS_TCP("MODBUS_TCP", "Modbus TCP协议", 502),
    MODBUS_RTU("MODBUS_RTU", "Modbus RTU协议", null),
    MODBUS_ASCII("MODBUS_ASCII", "Modbus ASCII协议", null),

    // OPC协议
    OPC_DA("OPC_DA", "OPC DA协议", null),
    OPC_UA("OPC_UA", "OPC UA协议", 4840),

    // SNMP协议
    SNMP_V1("SNMP_V1", "SNMP v1协议", 161),
    SNMP_V2C("SNMP_V2C", "SNMP v2c协议", 161),
    SNMP_V3("SNMP_V3", "SNMP v3协议", 161),

    // MQTT协议
    MQTT("MQTT", "MQTT协议", 1883),
    MQTT_SSL("MQTT_SSL", "MQTT SSL协议", 8883),

    // CoAP协议
    COAP("COAP", "CoAP协议", 5683),
    COAP_SSL("COAP_SSL", "CoAP SSL协议", 5684),

    // IEC协议
    IEC104("IEC104", "IEC 104协议", 2404),
    IEC61850("IEC61850", "IEC 61850协议", 102),

    // HTTP协议
    HTTP("HTTP", "HTTP协议", 80),
    HTTPS("HTTPS", "HTTPS协议", 443),

    // WebSocket协议
    WEBSOCKET("WEBSOCKET", "WebSocket协议", 80),
    WEBSOCKET_SSL("WEBSOCKET_SSL", "WebSocket SSL协议", 443),

    // 自定义协议
    CUSTOM_TCP("CUSTOM_TCP", "自定义TCP协议", null),
    CUSTOM_UDP("CUSTOM_UDP", "自定义UDP协议", null);

    private final String code;
    private final String description;
    private final Integer defaultPort;

    ProtocolType(String code, String description, Integer defaultPort) {
        this.code = code;
        this.description = description;
        this.defaultPort = defaultPort;
    }

    public String getCode() {
        return code;
    }

    public String getDescription() {
        return description;
    }

    public Integer getDefaultPort() {
        return defaultPort;
    }

    // 根据code获取枚举
    public static ProtocolType fromCode(String code) {
        for (ProtocolType type : values()) {
            if (type.getCode().equals(code)) {
                return type;
            }
        }
        return null;
    }

    // 判断是否为TCP协议
    public boolean isTcpProtocol() {
        return this == MODBUS_TCP || this == OPC_UA || this == IEC104 ||
                this == HTTP || this == HTTPS || this == WEBSOCKET ||
                this == WEBSOCKET_SSL || this == CUSTOM_TCP;
    }

    // 判断是否为串口协议
    public boolean isSerialProtocol() {
        return this == MODBUS_RTU || this == MODBUS_ASCII;
    }

    // 判断是否需要加密
    public boolean needEncryption() {
        return this == HTTPS || this == MQTT_SSL ||
                this == COAP_SSL || this == WEBSOCKET_SSL;
    }

    // 获取建议的超时时间
    public int getDefaultTimeout() {
        switch (this) {
            case MODBUS_TCP:
            case MODBUS_RTU:
                return 3000;
            case OPC_UA:
                return 10000;
            case SNMP_V1:
            case SNMP_V2C:
            case SNMP_V3:
                return 5000;
            case MQTT:
            case MQTT_SSL:
                return 10000;
            case IEC104:
                return 15000;
            default:
                return 5000;
        }
    }
}
