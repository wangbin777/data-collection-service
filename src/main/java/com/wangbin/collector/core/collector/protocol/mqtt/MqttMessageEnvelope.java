package com.wangbin.collector.core.collector.protocol.mqtt;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Map;

@Getter
@RequiredArgsConstructor
public class MqttMessageEnvelope {
    private final byte[] payload;
    private final int qos;
    private final boolean retained;
    private final Map<String, String> properties;
}
