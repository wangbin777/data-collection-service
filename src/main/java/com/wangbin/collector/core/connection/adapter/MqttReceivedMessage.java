package com.wangbin.collector.core.connection.adapter;

import lombok.Getter;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

/**
 * MQTT消息封装，包含主题、负载及属性。
 */
public class MqttReceivedMessage {

    @Getter
    private final String topic;
    private final byte[] payload;
    @Getter
    private final int qos;
    @Getter
    private final boolean retained;
    @Getter
    private final Map<String, String> userProperties;
    private final long receivedTime;

    public MqttReceivedMessage(String topic,
                               byte[] payload,
                               int qos,
                               boolean retained,
                               Map<String, String> userProperties) {
        this(topic, payload, qos, retained, userProperties, System.currentTimeMillis());
    }

    public MqttReceivedMessage(String topic,
                               byte[] payload,
                               int qos,
                               boolean retained,
                               Map<String, String> userProperties,
                               long receivedTime) {
        this.topic = topic;
        this.payload = payload != null ? Arrays.copyOf(payload, payload.length) : new byte[0];
        this.qos = qos;
        this.retained = retained;
        this.userProperties = userProperties != null
                ? Collections.unmodifiableMap(userProperties)
                : Collections.emptyMap();
        this.receivedTime = receivedTime;
    }

    public byte[] getPayload() {
        return Arrays.copyOf(payload, payload.length);
    }

    public long getReceivedTime() {
        return receivedTime;
    }
}
