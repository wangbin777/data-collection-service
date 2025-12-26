package com.wangbin.collector.core.connection.adapter;

import com.wangbin.collector.common.domain.enums.ConnectionStatus;
import com.wangbin.collector.core.connection.dispatch.MessageBatchDispatcher;
import com.wangbin.collector.core.connection.dispatch.OverflowStrategy;
import com.wangbin.collector.core.connection.model.ConnectionConfig;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.mqttv5.client.IMqttToken;
import org.eclipse.paho.mqttv5.client.MqttAsyncClient;
import org.eclipse.paho.mqttv5.client.MqttCallback;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.MqttDisconnectResponse;
import org.eclipse.paho.mqttv5.client.persist.MemoryPersistence;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;
import org.eclipse.paho.mqttv5.common.packet.UserProperty;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * MQTT连接适配器，同时支持 Paho v3 与 v5 客户端，并提供统一的消息监听机制。
 */
@Slf4j
public class MqttConnectionAdapter extends AbstractConnectionAdapter<Object>
        implements MqttCallback, MqttCallbackExtended {

    private MqttAsyncClient mqttClientV5;
    private org.eclipse.paho.client.mqttv3.MqttAsyncClient mqttClientV3;
    private MemoryPersistence persistenceV5;
    private org.eclipse.paho.client.mqttv3.persist.MemoryPersistence persistenceV3;
    private MqttConnectionOptions connectionOptionsV5;
    private MqttConnectOptions connectionOptionsV3;
    private MessageBatchDispatcher<MqttReceivedMessage> messageDispatcher;
    private BlockingQueue<MqttReceivedMessage> receiveBuffer;
    private final Map<String, Integer> subscribedTopics = new ConcurrentHashMap<>();
    private final CopyOnWriteArrayList<Consumer<MqttReceivedMessage>> messageListeners = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Consumer<List<MqttReceivedMessage>>> batchListeners = new CopyOnWriteArrayList<>();
    private OverflowStrategy overflowStrategy = OverflowStrategy.BLOCK;
    private String clientId;
    private boolean useMqttV5;

    public MqttConnectionAdapter(ConnectionConfig config) {
        super(config);
        initialize();
    }

    private void initialize() {
        this.clientId = generateClientId();
        String version = config.getStringConfig("version", "v5");
        this.useMqttV5 = !"v3".equalsIgnoreCase(version) && !"3".equals(version);
        ensureDispatcher();
        String serverUri = buildServerUri();

        try {
            if (useMqttV5) {
                this.persistenceV5 = new MemoryPersistence();
                this.mqttClientV5 = new MqttAsyncClient(serverUri, clientId, persistenceV5);
                this.mqttClientV5.setCallback(this);
                configureV5Options();
            } else {
                this.persistenceV3 = new org.eclipse.paho.client.mqttv3.persist.MemoryPersistence();
                this.mqttClientV3 = new org.eclipse.paho.client.mqttv3.MqttAsyncClient(serverUri, clientId, persistenceV3);
                this.mqttClientV3.setCallback(this);
                configureV3Options();
            }
        } catch (Exception e) {
            log.error("MQTT客户端初始化失败", e);
            throw new RuntimeException("MQTT客户端初始化失败", e);
        }
    }

    private String generateClientId() {
        if (config.getClientId() != null && !config.getClientId().isEmpty()) {
            return config.getClientId();
        }
        String prefix = config.getDeviceId() != null ? config.getDeviceId() : "collector";
        return prefix + "_" + UUID.randomUUID().toString().substring(0, 8);
    }

    private String buildServerUri() {
        if (config.getUrl() != null && !config.getUrl().isEmpty()) {
            return config.getUrl();
        }
        String protocol = Boolean.TRUE.equals(config.getSslEnabled()) ? "ssl" : "tcp";
        return String.format("%s://%s:%d",
                protocol,
                config.getHost(),
                config.getPort());
    }

    private void configureV5Options() {
        this.connectionOptionsV5 = new MqttConnectionOptions();
        connectionOptionsV5.setCleanStart(config.getBoolConfig("cleanSession", true));
        connectionOptionsV5.setConnectionTimeout(Math.max(1, config.getConnectTimeout() / 1000));
        connectionOptionsV5.setKeepAliveInterval(Math.max(1, config.getHeartbeatInterval() / 1000));
        connectionOptionsV5.setAutomaticReconnect(config.getBoolConfig("autoReconnect", true));
        connectionOptionsV5.setMaxReconnectDelay(config.getReconnectDelay());
        if (config.getUsername() != null) {
            connectionOptionsV5.setUserName(config.getUsername());
        }
        if (config.getPassword() != null) {
            connectionOptionsV5.setPassword(config.getPassword().getBytes(StandardCharsets.UTF_8));
        }
        configureWillMessageV5();
        connectionOptionsV5.setSessionExpiryInterval(config.getLongConfig("sessionExpiryInterval", 86400L));
        connectionOptionsV5.setReceiveMaximum(config.getIntConfig("receiveMaximum", 65535));
        configureSSL();
    }

    private void configureV3Options() {
        this.connectionOptionsV3 = new MqttConnectOptions();
        connectionOptionsV3.setAutomaticReconnect(config.getBoolConfig("autoReconnect", true));
        connectionOptionsV3.setCleanSession(config.getBoolConfig("cleanSession", true));
        connectionOptionsV3.setConnectionTimeout(Math.max(1, config.getConnectTimeout() / 1000));
        connectionOptionsV3.setKeepAliveInterval(Math.max(1, config.getHeartbeatInterval() / 1000));
        if (config.getUsername() != null) {
            connectionOptionsV3.setUserName(config.getUsername());
        }
        if (config.getPassword() != null) {
            connectionOptionsV3.setPassword(config.getPassword().toCharArray());
        }
        configureWillMessageV3();
        configureSSL();
    }

    private void configureWillMessageV5() {
        String willTopic = config.getStringConfig("willTopic", null);
        if (willTopic != null) {
            String willMessage = config.getStringConfig("willMessage", "");
            int willQos = config.getIntConfig("willQos", 0);
            boolean willRetained = config.getBoolConfig("willRetained", false);
            MqttMessage willMsg = new MqttMessage(willMessage.getBytes(StandardCharsets.UTF_8));
            willMsg.setQos(willQos);
            willMsg.setRetained(willRetained);
            connectionOptionsV5.setWill(willTopic, willMsg);
        }
    }

    private void configureWillMessageV3() {
        String willTopic = config.getStringConfig("willTopic", null);
        if (willTopic != null) {
            String willMessage = config.getStringConfig("willMessage", "");
            int willQos = config.getIntConfig("willQos", 0);
            boolean willRetained = config.getBoolConfig("willRetained", false);
            connectionOptionsV3.setWill(willTopic,
                    willMessage.getBytes(StandardCharsets.UTF_8),
                    willQos,
                    willRetained);
        }
    }

    private void configureSSL() {
        if (!Boolean.TRUE.equals(config.getSslEnabled())) {
            return;
        }
        try {
            TrustManager[] trustAllCerts = new TrustManager[]{
                    new X509TrustManager() {
                        public X509Certificate[] getAcceptedIssuers() {
                            return new X509Certificate[0];
                        }

                        public void checkClientTrusted(X509Certificate[] certs, String authType) {
                        }

                        public void checkServerTrusted(X509Certificate[] certs, String authType) {
                        }
                    }
            };
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, trustAllCerts, new SecureRandom());
            if (connectionOptionsV5 != null) {
                connectionOptionsV5.setSocketFactory(sslContext.getSocketFactory());
            }
            if (connectionOptionsV3 != null) {
                connectionOptionsV3.setSocketFactory(sslContext.getSocketFactory());
            }
        } catch (Exception e) {
            log.error("配置MQTT SSL失败", e);
        }
    }

    private void ensureDispatcher() {
        if (messageDispatcher != null) {
            return;
        }
        int capacity = Math.max(1, config.getMaxPendingMessages() != null ? config.getMaxPendingMessages() : 5000);
        int batchSize = Math.max(1, config.getDispatchBatchSize() != null ? config.getDispatchBatchSize() : 1);
        long flushInterval = config.getDispatchFlushInterval() != null
                ? Math.max(0L, config.getDispatchFlushInterval())
                : 0L;
        this.overflowStrategy = OverflowStrategy.from(config.getOverflowStrategy());
        this.receiveBuffer = new LinkedBlockingQueue<>(capacity);
        this.messageDispatcher = new MessageBatchDispatcher<>(capacity, batchSize, flushInterval, overflowStrategy);
        this.messageDispatcher.addListener(this::handleDispatchedBatch);
        this.messageDispatcher.start();
    }

    private void shutdownDispatcher() {
        if (messageDispatcher != null) {
            messageDispatcher.stop();
            messageDispatcher = null;
        }
        if (receiveBuffer != null) {
            receiveBuffer.clear();
            receiveBuffer = null;
        }
    }

    private void handleDispatchedBatch(List<MqttReceivedMessage> batch) {
        if (batch == null || batch.isEmpty()) {
            return;
        }
        for (MqttReceivedMessage message : batch) {
            offerReceiveBuffer(message);
        }
        if (!batchListeners.isEmpty()) {
            for (Consumer<List<MqttReceivedMessage>> listener : batchListeners) {
                try {
                    listener.accept(batch);
                } catch (Exception e) {
                    log.warn("MQTT批量监听处理失败", e);
                }
            }
        }
        if (!messageListeners.isEmpty()) {
            for (MqttReceivedMessage message : batch) {
                for (Consumer<MqttReceivedMessage> listener : messageListeners) {
                    try {
                        listener.accept(message);
                    } catch (Exception e) {
                        log.warn("MQTT消息监听处理失败", e);
                    }
                }
            }
        }
    }

    private void offerReceiveBuffer(MqttReceivedMessage message) {
        if (receiveBuffer == null || message == null) {
            return;
        }
        switch (overflowStrategy) {
            case DROP_LATEST:
                receiveBuffer.offer(message);
                break;
            case DROP_OLDEST:
                while (!receiveBuffer.offer(message)) {
                    receiveBuffer.poll();
                }
                break;
            case BLOCK:
            default:
                try {
                    receiveBuffer.put(message);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                break;
        }
    }

    @Override
    protected void doConnect() throws Exception {
        ensureDispatcher();
        try {
            log.info("开始连接MQTT: {} (protocol={})", clientId, useMqttV5 ? "v5" : "v3");
            if (useMqttV5) {
                IMqttToken connectToken = mqttClientV5.connect(connectionOptionsV5);
                connectToken.waitForCompletion(config.getConnectTimeout());
                if (!mqttClientV5.isConnected()) {
                    throw new Exception("MQTT v5 连接未完成");
                }
            } else {
                org.eclipse.paho.client.mqttv3.IMqttToken token = mqttClientV3.connect(connectionOptionsV3);
                token.waitForCompletion(config.getConnectTimeout());
                if (!mqttClientV3.isConnected()) {
                    throw new Exception("MQTT v3 连接未完成");
                }
            }
            if (subscribedTopics.isEmpty()) {
                subscribeConfiguredTopics();
            } else {
                resubscribeAllTopics();
            }
            log.info("MQTT连接建立成功: {}", clientId);
        } catch (Exception e) {
            throw new Exception("MQTT连接失败: " + e.getMessage(), e);
        }
    }

    private void subscribeConfiguredTopics() throws Exception {
        Object topicsObj = config.getConfigValue("subscribeTopics");
        int qos = config.getIntConfig("subscribeQos", 1);
        int subscribed = 0;
        if (topicsObj instanceof List<?> topics) {
            for (Object obj : topics) {
                String topic = Objects.toString(obj, "").trim();
                if (!topic.isEmpty()) {
                    subscribeTopic(topic, qos, true);
                    subscribed++;
                }
            }
        } else if (topicsObj instanceof String topics) {
            String[] arr = topics.split(",");
            for (String topic : arr) {
                String trimmed = topic.trim();
                if (!trimmed.isEmpty()) {
                    subscribeTopic(trimmed, qos, true);
                    subscribed++;
                }
            }
        }
        if (subscribed == 0) {
            String defaultTopic = String.format("devices/%s/#", config.getDeviceId());
            subscribeTopic(defaultTopic, 1, true);
        }
    }

    private void subscribeTopic(String topic, int qos, boolean track) throws Exception {
        if (topic == null || topic.isEmpty()) {
            return;
        }
        if (track) {
            subscribedTopics.put(topic, qos);
        }
        if (useMqttV5) {
            IMqttToken subscribeToken = mqttClientV5.subscribe(topic, qos);
            subscribeToken.waitForCompletion(5000);
        } else {
            org.eclipse.paho.client.mqttv3.IMqttToken subscribeToken = mqttClientV3.subscribe(topic, qos);
            subscribeToken.waitForCompletion(5000);
        }
        log.info("MQTT订阅成功 - Topic: {}, QoS: {}", topic, qos);
    }

    private void resubscribeAllTopics() {
        if (subscribedTopics.isEmpty()) {
            return;
        }
        subscribedTopics.forEach((topic, qos) -> {
            try {
                subscribeTopic(topic, qos, false);
            } catch (Exception e) {
                log.warn("MQTT重订阅失败 - Topic: {}", topic, e);
            }
        });
    }

    @Override
    protected void doDisconnect() throws Exception {
        try {
            unsubscribeAllTopics();
            if (useMqttV5 && mqttClientV5 != null && mqttClientV5.isConnected()) {
                mqttClientV5.disconnect().waitForCompletion(5000);
                mqttClientV5.close();
            }
            if (!useMqttV5 && mqttClientV3 != null && mqttClientV3.isConnected()) {
                mqttClientV3.disconnect().waitForCompletion(5000);
                mqttClientV3.close();
            }
            log.info("MQTT连接断开成功: {}", clientId);
        } catch (Exception e) {
            log.warn("MQTT断开连接异常", e);
            throw e;
        } finally {
            shutdownDispatcher();
        }
    }

    private void unsubscribeAllTopics() throws Exception {
        if (subscribedTopics.isEmpty()) {
            return;
        }
        String[] topics = subscribedTopics.keySet().toArray(new String[0]);
        if (useMqttV5 && mqttClientV5 != null && mqttClientV5.isConnected()) {
            mqttClientV5.unsubscribe(topics).waitForCompletion(5000);
        } else if (mqttClientV3 != null && mqttClientV3.isConnected()) {
            mqttClientV3.unsubscribe(topics).waitForCompletion(5000);
        }
        subscribedTopics.clear();
    }

    @Override
    protected void doSend(byte[] data) throws UnsupportedOperationException {
        try {
            publishInternal(getPublishTopic(), data,
                    config.getIntConfig("publishQos", 1),
                    config.getBoolConfig("retained", false));
        } catch (Exception e) {
            throw new UnsupportedOperationException("MQTT发送操作失败", e);
        }
    }

    private String getPublishTopic() {
        String topic = config.getStringConfig("publishTopic", null);
        if (topic != null && !topic.isEmpty()) {
            return topic;
        }
        return String.format("devices/%s/data", config.getDeviceId());
    }

    private void publishInternal(String topic, byte[] payload, int qos, boolean retained) throws Exception {
        if ((useMqttV5 && (mqttClientV5 == null || !mqttClientV5.isConnected())) ||
                (!useMqttV5 && (mqttClientV3 == null || !mqttClientV3.isConnected()))) {
            throw new IllegalStateException("MQTT连接未激活");
        }
        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("MQTT topic 不能为空");
        }
        byte[] body = payload != null ? payload : new byte[0];
        if (useMqttV5) {
            MqttMessage message = new MqttMessage(body);
            message.setQos(qos);
            message.setRetained(retained);
            MqttProperties properties = new MqttProperties();
            properties.setContentType("application/json");
            Map<String, Object> messageProps = config.getMapConfig("messageProperties");
            if (messageProps != null) {
                for (Map.Entry<String, Object> entry : messageProps.entrySet()) {
                    properties.getUserProperties().add(new UserProperty(entry.getKey(), entry.getValue().toString()));
                }
            }
            message.setProperties(properties);
            mqttClientV5.publish(topic, message).waitForCompletion(5000);
        } else {
            org.eclipse.paho.client.mqttv3.MqttMessage message = new org.eclipse.paho.client.mqttv3.MqttMessage(body);
            message.setQos(qos);
            message.setRetained(retained);
            mqttClientV3.publish(topic, message).waitForCompletion(5000);
        }
        log.debug("MQTT消息发送成功 - Topic: {}, Qos: {}, Size: {} bytes", topic, qos, body.length);
    }

    @Override
    protected byte[] doReceive() throws UnsupportedOperationException {
        try {
            return doReceive(config.getReadTimeout());
        } catch (Exception e) {
            throw new UnsupportedOperationException("MQTT接收操作失败", e);
        }
    }

    @Override
    protected byte[] doReceive(long timeout) throws UnsupportedOperationException {
        if (receiveBuffer == null) {
            return null;
        }
        try {
            MqttReceivedMessage message = timeout > 0
                    ? receiveBuffer.poll(timeout, TimeUnit.MILLISECONDS)
                    : receiveBuffer.take();
            return message != null ? message.getPayload() : null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new UnsupportedOperationException("MQTT接收消息被中断", e);
        } catch (Exception e) {
            throw new UnsupportedOperationException("MQTT接收操作失败", e);
        }
    }

    @Override
    protected void doHeartbeat() throws Exception {
        String pingTopic = String.format("$SYS/%s/ping", config.getDeviceId());
        byte[] payload = "PING".getBytes(StandardCharsets.UTF_8);
        if (useMqttV5) {
            mqttClientV5.publish(pingTopic, payload, 0, false);
        } else {
            mqttClientV3.publish(pingTopic, payload, 0, false);
        }
        log.debug("MQTT心跳消息发送: {}", pingTopic);
    }

    @Override
    protected void doAuthenticate() throws Exception {
        String authTopic = config.getStringConfig("authTopic", null);
        if (authTopic == null) {
            log.info("MQTT认证已在连接过程中完成");
            return;
        }
        String authMessage = buildAuthMessage();
        byte[] payload = authMessage.getBytes(StandardCharsets.UTF_8);
        if (useMqttV5) {
            mqttClientV5.publish(authTopic, payload, 1, false).waitForCompletion(5000);
        } else {
            mqttClientV3.publish(authTopic, payload, 1, false).waitForCompletion(5000);
        }
        log.info("MQTT额外认证消息已发送 {}", config.getDeviceId());
    }

    private String buildAuthMessage() {
        Map<String, Object> authData = new java.util.HashMap<>();
        authData.put("deviceId", config.getDeviceId());
        authData.put("timestamp", System.currentTimeMillis());
        if (config.getProductKey() != null) {
            authData.put("productKey", config.getProductKey());
        }
        if (config.getDeviceSecret() != null) {
            authData.put("deviceSecret", config.getDeviceSecret());
        }
        if (config.getAuthParams() != null) {
            authData.putAll(config.getAuthParams());
        }
        return com.alibaba.fastjson2.JSON.toJSONString(authData);
    }

    @Override
    public void disconnected(MqttDisconnectResponse disconnectResponse) {
        log.error("MQTT v5 连接断开: {}, 原因: {}", clientId,
                disconnectResponse != null ? disconnectResponse.getReasonString() : "未知");
        status = ConnectionStatus.DISCONNECTED;
        if (config.getBoolConfig("autoReconnect", true)) {
            try {
                reconnect();
            } catch (Exception e) {
                log.error("MQTT自动重连失败", e);
            }
        }
    }

    @Override
    public void mqttErrorOccurred(MqttException exception) {
        log.error("MQTT发生错误: {}", clientId, exception);
        errors.incrementAndGet();
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) {
        Map<String, String> props = extractProperties(message.getProperties());
        dispatchInboundMessage(topic, message.getPayload(), message.getQos(), message.isRetained(), props);
    }

    @Override
    public void messageArrived(String topic, org.eclipse.paho.client.mqttv3.MqttMessage message) {
        dispatchInboundMessage(topic, message.getPayload(), message.getQos(), message.isRetained(), Collections.emptyMap());
    }

    private void dispatchInboundMessage(String topic, byte[] payload, int qos, boolean retained, Map<String, String> props) {
        MqttReceivedMessage inbound = new MqttReceivedMessage(topic, payload, qos, retained, props);
        if (messageDispatcher == null) {
            log.warn("MQTT消息调度器未初始化，直接丢弃消息 topic={}", topic);
            return;
        }
        try {
            messageDispatcher.enqueue(inbound);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("MQTT消息入队被中断 topic={}", topic);
        }
    }

    @Override
    public void deliveryComplete(IMqttToken token) {
        log.debug("MQTT v5 消息投递完成 {}", token.getMessageId());
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        log.debug("MQTT v3 消息投递完成 {}", token.getMessageId());
    }

    @Override
    public void connectComplete(boolean reconnect, String serverURI) {
        log.info("MQTT连接完成 - 重连: {}, 服务端: {}", reconnect, serverURI);
        status = ConnectionStatus.CONNECTED;
        if (reconnect) {
            resubscribeAllTopics();
        }
    }

    @Override
    public void authPacketArrived(int reasonCode, MqttProperties properties) {
        log.debug("MQTT认证包到达 reasonCode={}", reasonCode);
    }

    @Override
    public void connectionLost(Throwable cause) {
        log.error("MQTT v3 连接丢失: {}", clientId, cause);
        status = ConnectionStatus.DISCONNECTED;
        if (config.getBoolConfig("autoReconnect", true)) {
            try {
                reconnect();
            } catch (Exception e) {
                log.error("MQTT自动重连失败", e);
            }
        }
    }

    // ========== 对外方法 ==========

    public void subscribe(String topic, int qos) throws Exception {
        subscribeTopic(topic, qos, true);
    }

    public void unsubscribe(String topic) throws Exception {
        if (topic == null || topic.isEmpty()) {
            return;
        }
        subscribedTopics.remove(topic);
        if (useMqttV5 && mqttClientV5 != null && mqttClientV5.isConnected()) {
            mqttClientV5.unsubscribe(topic).waitForCompletion(5000);
        } else if (mqttClientV3 != null && mqttClientV3.isConnected()) {
            mqttClientV3.unsubscribe(topic).waitForCompletion(5000);
        }
        log.info("取消订阅MQTT主题: {}", topic);
    }

    public void publish(String topic, byte[] payload, int qos, boolean retained) throws Exception {
        publishInternal(topic, payload, qos, retained);
    }

    public List<String> getSubscribedTopics() {
        return new ArrayList<>(subscribedTopics.keySet());
    }

    public boolean isSubscribed(String topic) {
        return subscribedTopics.containsKey(topic);
    }

    public void addMessageListener(Consumer<MqttReceivedMessage> listener) {
        if (listener != null) {
            messageListeners.add(listener);
        }
    }

    public void removeMessageListener(Consumer<MqttReceivedMessage> listener) {
        if (listener != null) {
            messageListeners.remove(listener);
        }
    }

    public void addBatchMessageListener(Consumer<List<MqttReceivedMessage>> listener) {
        if (listener != null) {
            batchListeners.add(listener);
        }
    }

    public void removeBatchMessageListener(Consumer<List<MqttReceivedMessage>> listener) {
        if (listener != null) {
            batchListeners.remove(listener);
        }
    }

    private Map<String, String> extractProperties(MqttProperties properties) {
        if (properties == null || properties.getUserProperties().isEmpty()) {
            return java.util.Collections.emptyMap();
        }
        Map<String, String> result = new ConcurrentHashMap<>();
        properties.getUserProperties()
                .forEach(prop -> result.put(prop.getKey(), prop.getValue()));
        return result;
    }

    @Override
    public Object getClient() {
        return useMqttV5 ? mqttClientV5 : mqttClientV3;
    }
}