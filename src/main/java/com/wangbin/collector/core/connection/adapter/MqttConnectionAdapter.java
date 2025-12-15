package com.wangbin.collector.core.connection.adapter;

import com.wangbin.collector.common.domain.enums.ConnectionStatus;
import com.wangbin.collector.core.connection.model.ConnectionConfig;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.mqttv5.client.*;
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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * MQTT连接适配器
 */
@Slf4j
public class MqttConnectionAdapter extends AbstractConnectionAdapter implements MqttCallback {

    private IMqttAsyncClient mqttClient;
    private MemoryPersistence persistence;
    private MqttConnectionOptions connectionOptions;
    private BlockingQueue<MqttMessage> messageQueue;
    private String clientId;

    // 订阅主题列表
    private List<String> subscribedTopics = new ArrayList<>();

    public MqttConnectionAdapter(ConnectionConfig config) {
        super(config);
        initialize();
    }

    private void initialize() {
        this.persistence = new MemoryPersistence();
        this.messageQueue = new LinkedBlockingQueue<>();

        // 生成客户端ID
        this.clientId = generateClientId();

        // 构建服务器URI
        String serverUri = buildServerUri();

        try {
            this.mqttClient = new MqttAsyncClient(serverUri, clientId, persistence);
            this.mqttClient.setCallback(this);

            // 配置连接选项
            configureConnectionOptions();

        } catch (MqttException e) {
            log.error("MQTT客户端初始化失败", e);
            throw new RuntimeException("MQTT客户端初始化失败", e);
        }
    }

    private String generateClientId() {
        // 优先使用配置的clientId
        if (config.getClientId() != null && !config.getClientId().isEmpty()) {
            return config.getClientId();
        }

        // 否则使用deviceId + 随机数
        String prefix = config.getDeviceId() != null ? config.getDeviceId() : "collector";
        return prefix + "_" + UUID.randomUUID().toString().substring(0, 8);
    }

    private String buildServerUri() {
        // 优先使用url字段
        if (config.getUrl() != null && !config.getUrl().isEmpty()) {
            return config.getUrl();
        }

        // 否则使用host和port构建
        String protocol = Boolean.TRUE.equals(config.getSslEnabled()) ? "ssl" : "tcp";
        return String.format("%s://%s:%d",
                protocol,
                config.getHost(),
                config.getPort());
    }

    private void configureConnectionOptions() {
        this.connectionOptions = new MqttConnectionOptions();

        // 基础配置
        connectionOptions.setCleanStart(config.getBoolConfig("cleanSession", true));
        connectionOptions.setConnectionTimeout(config.getConnectTimeout() / 1000);
        connectionOptions.setKeepAliveInterval(config.getHeartbeatInterval() / 1000);
        connectionOptions.setAutomaticReconnect(config.getBoolConfig("autoReconnect", true));
        connectionOptions.setMaxReconnectDelay(config.getReconnectDelay());

        // 认证配置
        if (config.getUsername() != null) {
            connectionOptions.setUserName(config.getUsername());
        }

        if (config.getPassword() != null) {
            connectionOptions.setPassword(config.getPassword().getBytes(StandardCharsets.UTF_8));
        }

        // 配置遗言
        configureWillMessage();

        // MQTTv5特定配置
        connectionOptions.setSessionExpiryInterval(config.getLongConfig("sessionExpiryInterval", 86400L)); // 默认24小时
        connectionOptions.setReceiveMaximum(config.getIntConfig("receiveMaximum", 65535));

        // SSL配置
        if (Boolean.TRUE.equals(config.getSslEnabled())) {
            configureSSL();
        }

        // 自定义属性
        configureCustomProperties();
    }

    private void configureWillMessage() {
        String willTopic = config.getStringConfig("willTopic", null);
        if (willTopic != null) {
            String willMessage = config.getStringConfig("willMessage", "");
            int willQos = config.getIntConfig("willQos", 0);
            boolean willRetained = config.getBoolConfig("willRetained", false);

            MqttMessage willMsg = new MqttMessage(willMessage.getBytes(StandardCharsets.UTF_8));
            willMsg.setQos(willQos);
            willMsg.setRetained(willRetained);

            connectionOptions.setWill(willTopic, willMsg);
        }
    }

    private void configureSSL() {
        try {
            // 创建信任所有证书的SSLContext
            TrustManager[] trustAllCerts = new TrustManager[] {
                    new X509TrustManager() {
                        public X509Certificate[] getAcceptedIssuers() { return null; }
                        public void checkClientTrusted(X509Certificate[] certs, String authType) { }
                        public void checkServerTrusted(X509Certificate[] certs, String authType) { }
                    }
            };

            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, trustAllCerts, new SecureRandom());

            connectionOptions.setSocketFactory(sslContext.getSocketFactory());

        } catch (Exception e) {
            log.error("配置MQTT SSL失败", e);
        }
    }

    private void configureCustomProperties() {
        // 获取MQTT属性
        MqttProperties properties = connectionOptions.getConnectionProperties();
        if (properties == null) {
            properties = new MqttProperties();
        }

        // 设置用户属性
        Map<String, Object> userProps = config.getMapConfig("userProperties");
        if (userProps != null) {
            for (Map.Entry<String, Object> entry : userProps.entrySet()) {
                properties.getUserProperties().add(new UserProperty(entry.getKey(), entry.getValue().toString()));
            }
        }

        // 设置响应信息（如果有）
        String responseTopic = config.getStringConfig("responseTopic", null);
        if (responseTopic != null) {
            properties.setResponseTopic(responseTopic);
        }

        // 设置相关数据（如果有）
        String correlationData = config.getStringConfig("correlationData", null);
        if (correlationData != null) {
            properties.setCorrelationData(correlationData.getBytes(StandardCharsets.UTF_8));
        }

        // 设置内容类型（如果有）
        String contentType = config.getStringConfig("contentType", null);
        if (contentType != null) {
            properties.setContentType(contentType);
        }
    }

    @Override
    protected void doConnect() throws Exception {
        try {
            log.info("开始连接MQTT服务器: {}", clientId);

            // 连接到服务器
            IMqttToken connectToken = mqttClient.connect(connectionOptions);
            connectToken.waitForCompletion(config.getConnectTimeout());

            if (!connectToken.isComplete() || !mqttClient.isConnected()) {
                throw new Exception("MQTT连接未完成");
            }

            // 订阅配置的主题
            subscribeConfiguredTopics();

            log.info("MQTT连接建立成功: {}", clientId);

        } catch (MqttException e) {
            throw new Exception("MQTT连接失败: " + e.getMessage(), e);
        }
    }

    private void subscribeConfiguredTopics() throws MqttException {
        // 获取配置的订阅主题
        Object topicsObj = config.getConfigValue("subscribeTopics");
        if (topicsObj instanceof List) {
            List<?> topicsList = (List<?>) topicsObj;
            for (Object topicObj : topicsList) {
                if (topicObj instanceof String) {
                    String topic = (String) topicObj;
                    int qos = config.getIntConfig("subscribeQos", 1);
                    subscribeTopic(topic, qos);
                }
            }
        } else if (topicsObj instanceof String) {
            String topicsStr = (String) topicsObj;
            String[] topics = topicsStr.split(",");
            int qos = config.getIntConfig("subscribeQos", 1);
            for (String topic : topics) {
                subscribeTopic(topic.trim(), qos);
            }
        }

        // 如果没有配置主题，订阅默认主题
        if (subscribedTopics.isEmpty()) {
            String defaultTopic = String.format("devices/%s/#", config.getDeviceId());
            subscribeTopic(defaultTopic, 1);
        }
    }

    private void subscribeTopic(String topic, int qos) throws MqttException {
        IMqttToken subscribeToken = mqttClient.subscribe(topic, qos);
        subscribeToken.waitForCompletion(5000);

        subscribedTopics.add(topic);
        log.info("MQTT订阅成功 - Topic: {}, Qos: {}", topic, qos);
    }

    @Override
    protected void doDisconnect() throws Exception {
        try {
            if (mqttClient != null && mqttClient.isConnected()) {
                // 取消订阅所有主题
                unsubscribeAllTopics();

                // 断开连接
                mqttClient.disconnect();
                mqttClient.close();

                log.info("MQTT连接断开成功: {}", clientId);
            }
        } catch (MqttException e) {
            log.warn("MQTT断开连接异常", e);
            throw e;
        } finally {
            mqttClient = null;
            subscribedTopics.clear();
        }
    }

    private void unsubscribeAllTopics() throws MqttException {
        if (!subscribedTopics.isEmpty()) {
            String[] topics = subscribedTopics.toArray(new String[0]);
            mqttClient.unsubscribe(topics);
            subscribedTopics.clear();
        }
    }

    @Override
    protected void doSend(byte[] data) throws Exception {
        if (mqttClient == null || !mqttClient.isConnected()) {
            throw new IllegalStateException("MQTT连接未激活");
        }

        try {
            String topic = getPublishTopic();
            int qos = config.getIntConfig("publishQos", 1);
            boolean retained = config.getBoolConfig("retained", false);

            MqttMessage message = new MqttMessage(data);
            message.setQos(qos);
            message.setRetained(retained);

            // 设置消息属性
            MqttProperties properties = new MqttProperties();
            properties.setContentType("application/json");

            // 添加用户属性
            Map<String, Object> messageProps = config.getMapConfig("messageProperties");
            if (messageProps != null) {
                for (Map.Entry<String, Object> entry : messageProps.entrySet()) {
                    properties.getUserProperties().add(new UserProperty(entry.getKey(), entry.getValue().toString()));
                }
            }

            message.setProperties(properties);

            IMqttToken publishToken = mqttClient.publish(topic, message);
            publishToken.waitForCompletion(5000);

            log.debug("MQTT消息发布成功 - Topic: {}, Qos: {}, Size: {} bytes",
                    topic, qos, data.length);

        } catch (MqttException e) {
            throw new Exception("MQTT消息发送失败: " + e.getMessage(), e);
        }
    }

    private String getPublishTopic() {
        // 优先使用配置的发布主题
        String topic = config.getStringConfig("publishTopic", null);
        if (topic != null && !topic.isEmpty()) {
            return topic;
        }

        // 否则使用设备ID构建主题
        return String.format("devices/%s/data", config.getDeviceId());
    }

    @Override
    protected byte[] doReceive() throws Exception {
        return doReceive(config.getReadTimeout());
    }

    @Override
    protected byte[] doReceive(long timeout) throws Exception {
        try {
            MqttMessage message = messageQueue.poll(timeout, TimeUnit.MILLISECONDS);
            return message != null ? message.getPayload() : null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new Exception("MQTT接收消息被中断", e);
        }
    }

    @Override
    protected void doHeartbeat() throws Exception {
        // MQTT有自动心跳机制，这里可以发送一个ping消息
        String pingTopic = String.format("$SYS/%s/ping", config.getDeviceId());
        MqttMessage pingMessage = new MqttMessage("PING".getBytes(StandardCharsets.UTF_8));
        pingMessage.setQos(0);
        pingMessage.setRetained(false);

        try {
            mqttClient.publish(pingTopic, pingMessage);
            log.debug("MQTT心跳消息发送: {}", pingTopic);
        } catch (Exception e) {
            log.warn("MQTT心跳发送失败", e);
        }
    }

    @Override
    protected void doAuthenticate() throws Exception {
        // MQTT认证在连接时已经通过用户名/密码处理
        // 如果需要额外的认证，可以发送认证消息
        String authTopic = config.getStringConfig("authTopic", null);
        if (authTopic != null) {
            // 构建认证消息
            String authMessage = buildAuthMessage();
            MqttMessage message = new MqttMessage(authMessage.getBytes(StandardCharsets.UTF_8));
            message.setQos(1);

            mqttClient.publish(authTopic, message).waitForCompletion(5000);

            log.info("MQTT额外认证消息已发送: {}", config.getDeviceId());
        } else {
            log.info("MQTT认证已在连接过程中完成");
        }
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

        // 添加额外的认证参数
        if (config.getAuthParams() != null) {
            authData.putAll(config.getAuthParams());
        }

        return com.alibaba.fastjson2.JSON.toJSONString(authData);
    }

    // ========== MQTT回调方法 ==========

    @Override
    public void disconnected(MqttDisconnectResponse disconnectResponse) {
        log.error("MQTT连接断开: {}, 原因: {}", clientId,
                disconnectResponse != null ? disconnectResponse.getReasonString() : "未知");

        status = ConnectionStatus.DISCONNECTED;
        subscribedTopics.clear();

        // 自动重连
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
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        log.debug("收到MQTT消息 - Topic: {}, Qos: {}, Size: {} bytes",
                topic, message.getQos(), message.getPayload().length);

        // 添加到消息队列
        messageQueue.offer(message);
    }

    @Override
    public void deliveryComplete(IMqttToken token) {
        log.debug("MQTT消息投递完成: {}", token.getMessageId());
    }

    @Override
    public void connectComplete(boolean reconnect, String serverURI) {
        log.info("MQTT连接完成 - 重连: {}, 服务器: {}", reconnect, serverURI);
        status = ConnectionStatus.CONNECTED;

        // 如果是重连，重新订阅主题
        if (reconnect) {
            try {
                subscribeConfiguredTopics();
            } catch (MqttException e) {
                log.error("MQTT重连后订阅失败", e);
            }
        }
    }

    @Override
    public void authPacketArrived(int reasonCode, MqttProperties properties) {
        log.debug("MQTT认证包到达: reasonCode={}", reasonCode);
    }

    // ========== 公共方法 ==========

    /**
     * 订阅主题
     */
    public void subscribe(String topic, int qos) throws Exception {
        try {
            subscribeTopic(topic, qos);
        } catch (MqttException e) {
            throw new Exception("订阅主题失败: " + e.getMessage(), e);
        }
    }

    /**
     * 取消订阅
     */
    public void unsubscribe(String topic) throws Exception {
        try {
            mqttClient.unsubscribe(topic);
            subscribedTopics.remove(topic);
            log.info("取消订阅主题: {}", topic);
        } catch (MqttException e) {
            throw new Exception("取消订阅失败: " + e.getMessage(), e);
        }
    }

    /**
     * 获取所有订阅的主题
     */
    public List<String> getSubscribedTopics() {
        return new ArrayList<>(subscribedTopics);
    }

    /**
     * 检查是否已订阅某个主题
     */
    public boolean isSubscribed(String topic) {
        return subscribedTopics.contains(topic);
    }
}