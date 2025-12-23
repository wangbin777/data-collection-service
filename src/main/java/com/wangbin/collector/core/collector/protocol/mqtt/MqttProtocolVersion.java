package com.wangbin.collector.core.collector.protocol.mqtt;

/**
 * 支持的 MQTT 协议版本。
 */
public enum MqttProtocolVersion {
    V3,
    V5;

    public static MqttProtocolVersion fromText(String text) {
        if (text == null) {
            return V5;
        }
        String normalized = text.trim().toLowerCase();
        if ("v3".equals(normalized) || "3.1.1".equals(normalized) || "mqtt3".equals(normalized)) {
            return V3;
        }
        return V5;
    }
}
