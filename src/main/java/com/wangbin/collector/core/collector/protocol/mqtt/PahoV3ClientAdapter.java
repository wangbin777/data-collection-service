package com.wangbin.collector.core.collector.protocol.mqtt;

import org.eclipse.paho.client.mqttv3.IMqttAsyncClient;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.util.Collections;

class PahoV3ClientAdapter implements MqttClientAdapter {

    private final MqttConnectionConfig config;
    private final IncomingMessageHandler handler;
    private final IMqttAsyncClient client;
    private final MqttConnectOptions options;

    PahoV3ClientAdapter(MqttConnectionConfig config, IncomingMessageHandler handler) throws MqttException {
        this.config = config;
        this.handler = handler;
        this.options = new MqttConnectOptions();
        options.setAutomaticReconnect(config.isAutomaticReconnect());
        options.setCleanSession(config.isCleanSession());
        options.setConnectionTimeout(config.getConnectionTimeoutSeconds());
        options.setKeepAliveInterval(config.getKeepAliveIntervalSeconds());
        if (config.getUsername() != null && !config.getUsername().isBlank()) {
            options.setUserName(config.getUsername());
        }
        if (config.getPassword() != null) {
            options.setPassword(config.getPassword().toCharArray());
        }
        this.client = new MqttAsyncClient(config.getBrokerUrl(), config.getClientId());
    }

    @Override
    public void connect() throws Exception {
        IMqttToken token = client.connect(options);
        token.waitForCompletion(config.getConnectionTimeoutSeconds() * 1000L);
        if (!client.isConnected()) {
            throw new IllegalStateException("MQTT v3 client failed to connect");
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
        MqttMessage message = new MqttMessage(payload != null ? payload : new byte[0]);
        message.setQos(qos);
        message.setRetained(retained);
        client.publish(topic, message).waitForCompletion(5000);
    }

    @Override
    public void subscribe(String topic, int qos) throws Exception {
        IMqttToken token = client.subscribe(topic, qos, (receivedTopic, message) -> {
            handler.handle(receivedTopic,
                    new MqttMessageEnvelope(message.getPayload(), message.getQos(), message.isRetained(), Collections.emptyMap()));
        });
        token.waitForCompletion(5000);
    }

    @Override
    public void unsubscribe(String topic) throws Exception {
        client.unsubscribe(topic).waitForCompletion(5000);
    }

    @Override
    public boolean isConnected() {
        return client.isConnected();
    }
}
