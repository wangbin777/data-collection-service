package com.wangbin.collector.core.collector.protocol.mqtt;

@FunctionalInterface
public interface IncomingMessageHandler {
    void handle(String topic, MqttMessageEnvelope envelope);
}
