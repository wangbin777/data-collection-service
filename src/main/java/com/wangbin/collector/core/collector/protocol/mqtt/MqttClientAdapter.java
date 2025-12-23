package com.wangbin.collector.core.collector.protocol.mqtt;

public interface MqttClientAdapter extends AutoCloseable {

    void connect() throws Exception;

    void disconnect() throws Exception;

    void publish(String topic, byte[] payload, int qos, boolean retained) throws Exception;

    void subscribe(String topic, int qos) throws Exception;

    void unsubscribe(String topic) throws Exception;

    boolean isConnected();

    @Override
    default void close() throws Exception {
        disconnect();
    }
}
