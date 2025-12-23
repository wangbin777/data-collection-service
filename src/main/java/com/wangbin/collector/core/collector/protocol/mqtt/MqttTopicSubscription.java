package com.wangbin.collector.core.collector.protocol.mqtt;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class MqttTopicSubscription {
    private final String topic;
    private final int qos;
}
