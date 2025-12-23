package com.wangbin.collector.core.collector.protocol.mqtt;

import org.eclipse.paho.mqttv5.client.IMqttMessageListener;
import org.eclipse.paho.mqttv5.client.MqttAsyncClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class PahoV5ClientAdapter implements MqttClientAdapter {

    private final MqttConnectionConfig config;
    private final IncomingMessageHandler handler;
    private final MqttAsyncClient client;
    private final MqttConnectionOptions options;

    PahoV5ClientAdapter(MqttConnectionConfig config, IncomingMessageHandler handler) throws MqttException {
        this.config = config;
        this.handler = handler;
        this.options = new MqttConnectionOptions();
        options.setAutomaticReconnect(config.isAutomaticReconnect());
        options.setCleanStart(config.isCleanSession());
        options.setConnectionTimeout(config.getConnectionTimeoutSeconds());
        options.setKeepAliveInterval(config.getKeepAliveIntervalSeconds());
        if (config.getUsername() != null && !config.getUsername().isBlank()) {
            options.setUserName(config.getUsername());
        }
        if (config.getPassword() != null) {
            options.setPassword(config.getPassword().getBytes(StandardCharsets.UTF_8));
        }
        this.client = new MqttAsyncClient(config.getBrokerUrl(), config.getClientId());
    }

    @Override
    public void connect() throws Exception {
        client.connect(options).waitForCompletion(config.getConnectionTimeoutSeconds() * 1000L);
        if (!client.isConnected()) {
            throw new IllegalStateException("MQTT v5 client failed to connect");
        }
    }

    @Override
    public void disconnect() throws Exception {
        if (client.isConnected()) {
            client.disconnect().waitForCompletion(5000);
        }
        client.close();
    }

    @Override
    public void publish(String topic, byte[] payload, int qos, boolean retained) throws Exception {
        client.publish(topic, payload != null ? payload : new byte[0], qos, retained).waitForCompletion(5000);
    }

    @Override
    public void subscribe(String topic, int qos) throws Exception {
        IMqttMessageListener listener = (receivedTopic, message) -> {
            Map<String, String> props = extractProperties(message.getProperties());
            handler.handle(receivedTopic,
                    new MqttMessageEnvelope(message.getPayload(), message.getQos(), message.isRetained(), props));
        };
        client.subscribe(new String[]{topic}, new int[]{qos}, listener, null).waitForCompletion(5000);
    }

    @Override
    public void unsubscribe(String topic) throws Exception {
        client.unsubscribe(topic).waitForCompletion(5000);
    }

    @Override
    public boolean isConnected() {
        return client.isConnected();
    }

    private Map<String, String> extractProperties(MqttProperties properties) {
        if (properties == null || properties.getUserProperties().isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, String> result = new ConcurrentHashMap<>();
        properties.getUserProperties()
                .forEach(prop -> result.put(prop.getKey(), prop.getValue()));
        return result;
    }
}
