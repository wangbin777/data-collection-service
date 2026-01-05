package com.wangbin.collector.core.report.handler;

import com.wangbin.collector.common.constant.MessageConstant;
import com.wangbin.collector.common.constant.ProtocolConstant;
import com.wangbin.collector.common.enums.QualityEnum;
import com.wangbin.collector.core.report.model.ReportConfig;
import com.wangbin.collector.core.report.model.ReportData;
import com.wangbin.collector.core.report.model.ReportResult;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.mqttv5.client.*;
import org.eclipse.paho.mqttv5.client.persist.MemoryPersistence;
import org.eclipse.paho.mqttv5.common.*;
import org.eclipse.paho.mqttv5.common.packet.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * MQTT报告处理器 - v5版本
 */
@Slf4j
public class MqttReportHandler extends AbstractReportHandler {

    private MqttClientManager clientManager;
    private MessagePublisher messagePublisher;
    private SubscriptionManager subscriptionManager;
    private final Map<String, MqttConnectionConfig> connectionConfigs = new ConcurrentHashMap<>();

    public MqttReportHandler() {
        super("MqttReportHandler", "MQTT", "MQTT v5协议上报处理器");
    }

    @Override
    protected void doInit() throws Exception {
        log.info("初始化MQTT v5报告处理器...");

        clientManager = new MqttClientManager();
        clientManager.init();

        messagePublisher = new MessagePublisher(clientManager);
        messagePublisher.init();

        subscriptionManager = new SubscriptionManager(clientManager);
        subscriptionManager.init();

        log.info("MQTT v5报告处理器初始化完成");
    }

    @Override
    protected ReportResult doReport(ReportData data, ReportConfig config) throws Exception {
        log.debug("开始MQTT v5上报：{} -> {}:{}", data.getPointCode(), config.getHost(), config.getPort());
        long startTime = System.currentTimeMillis();

        try {
            // 1. 获取连接配置
            MqttConnectionConfig connConfig = getConnectionConfig(config);

            // 2. 获取MQTT客户端
            MqttAsyncClient mqttClient = clientManager.getClient(connConfig);
            if (mqttClient == null || !mqttClient.isConnected()) {
                throw new MqttException(new Throwable("MQTT客户端未连接"));
            }

            // 3. 构建发布选项
            MqttPublishOptions publishOptions = buildPublishOptions(data, config);

            // 4. 构建消息内容
            byte[] messagePayload = buildMessagePayload(data, config);

            // 5. 发布消息 - v5 API
            PublishResult publishResult = messagePublisher.publish(
                    mqttClient,
                    publishOptions.getTopic(),
                    messagePayload,
                    publishOptions
            );

            // 6. 创建上报结果
            ReportResult result = ReportResult.success(data.getPointCode(), config.getTargetId());
            result.setCostTime(System.currentTimeMillis() - startTime);
            result.addMetadata("mqttMessageId", publishResult.getMessageId());
            result.addMetadata("mqttQos", publishOptions.getQos());

            if (publishResult.isSuccess()) {
                log.debug("MQTT v5上报成功：{} -> {}:{}, QoS: {}, 耗时：{}ms",
                        data.getPointCode(), config.getHost(), config.getPort(),
                        publishOptions.getQos(), result.getCostTime());
            } else {
                result.setSuccess(false);
                result.setErrorMessage("MQTT v5发布失败: " + publishResult.getErrorMessage());
                log.warn("MQTT v5上报失败：{} -> {}:{}, 错误：{}",
                        data.getPointCode(), config.getHost(), config.getPort(),
                        publishResult.getErrorMessage());
            }

            return result;

        } catch (Exception e) {
            log.error("MQTT v5上报异常：{} -> {}:{}",
                    data.getPointCode(), config.getHost(), config.getPort(), e);
            throw e;
        }
    }

    @Override
    protected List<ReportResult> doBatchReport(List<ReportData> dataList, ReportConfig config) throws Exception {
        log.debug("开始批量MQTT v5上报：{}:{}，数据量：{}", config.getHost(), config.getPort(), dataList.size());

        List<ReportResult> results = new ArrayList<>(dataList.size());
        MqttConnectionConfig connConfig = getConnectionConfig(config);

        try {
            // 1. 获取MQTT客户端
            MqttAsyncClient mqttClient = clientManager.getClient(connConfig);
            if (mqttClient == null || !mqttClient.isConnected()) {
                throw new MqttException(new Throwable("MQTT客户端未连接"));
            }

            // 2. 批量发布消息
            List<PublishTask> publishTasks = new ArrayList<>(dataList.size());

            for (ReportData data : dataList) {
                // 构建发布选项
                MqttPublishOptions publishOptions = buildPublishOptions(data, config);

                // 构建消息内容
                byte[] messagePayload = buildMessagePayload(data, config);

                // 创建发布任务
                PublishTask task = new PublishTask(
                        mqttClient,
                        publishOptions.getTopic(),
                        messagePayload,
                        publishOptions,
                        data.getPointCode(),
                        config.getTargetId()
                );
                publishTasks.add(task);
            }

            // 3. 执行批量发布
            List<PublishResult> publishResults = messagePublisher.publishBatch(publishTasks);

            // 4. 转换发布结果为上报结果
            for (int i = 0; i < publishResults.size(); i++) {
                PublishResult publishResult = publishResults.get(i);
                ReportData data = dataList.get(i);

                ReportResult result = ReportResult.success(data.getPointCode(), config.getTargetId());
                result.addMetadata("mqttMessageId", publishResult.getMessageId());

                if (!publishResult.isSuccess()) {
                    result.setSuccess(false);
                    result.setErrorMessage("MQTT v5发布失败: " + publishResult.getErrorMessage());
                }

                results.add(result);
            }

            // 统计成功数量
            long successCount = results.stream().filter(ReportResult::isSuccess).count();
            log.debug("批量MQTT v5上报完成：{}:{}, 成功：{}，失败：{}，总计：{}",
                    config.getHost(), config.getPort(),
                    successCount, results.size() - successCount, results.size());

        } catch (Exception e) {
            log.error("批量MQTT v5上报异常：{}:{}", config.getHost(), config.getPort(), e);

            // 为所有数据创建错误结果
            for (ReportData data : dataList) {
                ReportResult errorResult = ReportResult.error(
                        data.getPointCode(),
                        "批量MQTT v5上报异常: " + e.getMessage(),
                        config.getTargetId()
                );
                results.add(errorResult);
            }
        }

        return results;
    }

    @Override
    protected void doConfigUpdate(ReportConfig config) throws Exception {
        log.debug("更新MQTT v5处理器配置：{}", config.getTargetId());

        try {
            // 1. 获取或创建连接配置
            MqttConnectionConfig connConfig = getConnectionConfig(config);

            // 2. 更新客户端管理器配置
            clientManager.updateConnectionConfig(connConfig);

            // 3. 更新订阅配置
            updateSubscriptions(connConfig);

            log.info("MQTT v5处理器配置更新完成：{}", config.getTargetId());
        } catch (Exception e) {
            log.error("MQTT v5处理器配置更新失败：{}", config.getTargetId(), e);
            throw e;
        }
    }

    @Override
    protected void doConfigRemove(ReportConfig config) throws Exception {
        log.debug("删除MQTT v5处理器配置：{}", config.getTargetId());

        try {
            // 1. 移除连接配置
            String configKey = getConnectionConfigKey(config);
            connectionConfigs.remove(configKey);

            // 2. 关闭相关客户端
            clientManager.removeClient(configKey);

            // 3. 移除相关订阅
            subscriptionManager.removeSubscriptions(configKey);

            log.info("MQTT v5处理器配置删除完成：{}", config.getTargetId());
        } catch (Exception e) {
            log.error("MQTT v5处理器配置删除失败：{}", config.getTargetId(), e);
            throw e;
        }
    }

    @Override
    protected void doDestroy() throws Exception {
        log.info("销毁MQTT v5报告处理器...");

        try {
            // 1. 销毁订阅管理器
            if (subscriptionManager != null) {
                subscriptionManager.destroy();
                subscriptionManager = null;
            }

            // 2. 销毁消息发布管理器
            if (messagePublisher != null) {
                messagePublisher.destroy();
                messagePublisher = null;
            }

            // 3. 销毁客户端管理器
            if (clientManager != null) {
                clientManager.destroy();
                clientManager = null;
            }

            // 4. 清空配置
            connectionConfigs.clear();

            log.info("MQTT v5报告处理器销毁完成");
        } catch (Exception e) {
            log.error("MQTT v5报告处理器销毁失败", e);
            throw e;
        }
    }

    @Override
    protected Map<String, Object> getImplementationStatus() {
        Map<String, Object> status = new HashMap<>();

        status.put("clientManager", clientManager != null ?
                clientManager.getStatus() : "未初始化");
        status.put("messagePublisher", messagePublisher != null ?
                "已初始化" : "未初始化");
        status.put("subscriptionManager", subscriptionManager != null ?
                "已初始化" : "未初始化");
        status.put("connectionConfigsCount", connectionConfigs.size());

        return status;
    }

    @Override
    protected Map<String, Object> getImplementationStatistics() {
        Map<String, Object> stats = new HashMap<>();

        if (clientManager != null) {
            stats.put("clientManager", clientManager.getStatistics());
        }

        if (messagePublisher != null) {
            stats.put("messagePublisher", messagePublisher.getStatistics());
        }

        if (subscriptionManager != null) {
            stats.put("subscriptionManager", subscriptionManager.getStatistics());
        }

        return stats;
    }

    // =============== 辅助方法 ===============

    private void loadMqttConfig() {
        log.debug("加载MQTT v5配置完成");
    }

    private MqttConnectionConfig getConnectionConfig(ReportConfig config) {
        String configKey = getConnectionConfigKey(config);

        return connectionConfigs.computeIfAbsent(configKey, key -> {
            MqttConnectionConfig connConfig = new MqttConnectionConfig(config);

            // 使用常量获取参数
            connConfig.setClientId(config.getMqttClientId());
            connConfig.setKeepAliveInterval(config.getIntParam(
                    ProtocolConstant.MQTT_PARAM_KEEP_ALIVE, 60));
            connConfig.setConnectionTimeout((int) config.getEffectiveConnectTimeout());
            connConfig.setCleanStart(config.getBooleanParam(
                    ProtocolConstant.MQTT_PARAM_CLEAN_SESSION, true)); // v5: cleanStart
            connConfig.setAutomaticReconnect(true);
            connConfig.setMaxReconnectDelay(60000);

            // 认证信息
            connConfig.setUsername(config.getStringParam(ProtocolConstant.MQTT_PARAM_USERNAME));

            String password = config.getStringParam(ProtocolConstant.MQTT_PARAM_PASSWORD);
            if (password != null) {
                connConfig.setPassword(password.toCharArray());
            }

            // SSL配置
            connConfig.setSslEnabled(config.getBooleanParam(
                    ProtocolConstant.MQTT_PARAM_SSL_ENABLED, false));

            // 遗嘱消息
            String willTopic = config.getStringParam(ProtocolConstant.MQTT_PARAM_WILL_TOPIC);
            String willMessage = config.getStringParam(ProtocolConstant.MQTT_PARAM_WILL_MESSAGE);
            if (willTopic != null && willMessage != null) {
                MqttWillMessage will = new MqttWillMessage();
                will.setTopic(willTopic);
                will.setMessage(willMessage.getBytes());
                will.setQos(config.getIntParam(ProtocolConstant.MQTT_PARAM_WILL_QOS, 1));
                will.setRetained(config.getBooleanParam(
                        ProtocolConstant.MQTT_PARAM_WILL_RETAINED, false));
                connConfig.setWillMessage(will);
            }

            // 发布主题
            String publishTopic = config.getStringParam(ProtocolConstant.MQTT_PARAM_PUBLISH_TOPIC);
            if (publishTopic != null) {
                connConfig.setDefaultPublishTopic(publishTopic);
            }

            // 订阅主题
            Object subscribeTopics = config.getParam(ProtocolConstant.MQTT_PARAM_SUBSCRIBE_TOPICS);
            if (subscribeTopics instanceof java.util.List) {
                @SuppressWarnings("unchecked")
                java.util.List<String> topics = (java.util.List<String>) subscribeTopics;
                connConfig.setSubscribeTopics(topics);
            }

            return connConfig;
        });
    }

    private String getConnectionConfigKey(ReportConfig config) {
        return config.getTargetId() + "@" + config.getHost() + ":" + config.getPort();
    }

    private MqttPublishOptions buildPublishOptions(ReportData data, ReportConfig config) {
        MqttConnectionConfig connConfig = getConnectionConfig(config);
        MqttPublishOptions options = new MqttPublishOptions();

        // 设置主题
        String topic = connConfig.getDefaultPublishTopic();
        if (topic == null || topic.isEmpty()) {
            // 默认主题格式
            topic = "data/" + config.getTargetId() + "/" + data.getPointCode();
        } else {
            // 替换主题中的变量
            topic = replaceTopicVariables(topic, data, config);
        }
        options.setTopic(topic);

        // 设置QoS级别
        int qos = getQosLevel(config);
        options.setQos(qos);

        // 设置是否保留消息
        boolean retained = isRetainedMessage(config);
        options.setRetained(retained);

        return options;
    }

    private String replaceTopicVariables(String topic, ReportData data, ReportConfig config) {
        return topic.replace("{pointCode}", data.getPointCode())
                .replace("{targetId}", config.getTargetId())
                .replace("{timestamp}", String.valueOf(data.getTimestamp()));
    }

    private int getQosLevel(ReportConfig config) {
        Map<String, Object> params = config.getParams();
        if (params != null) {
            Object qosObj = params.get("qos");
            if (qosObj instanceof Number) {
                int qos = ((Number) qosObj).intValue();
                if (qos >= 0 && qos <= 2) {
                    return qos;
                }
            }
        }
        return 1; // 默认QoS 1
    }

    private boolean isRetainedMessage(ReportConfig config) {
        Map<String, Object> params = config.getParams();
        if (params != null) {
            Object retainedObj = params.get("retained");
            if (retainedObj instanceof Boolean) {
                return (Boolean) retainedObj;
            }
        }
        return false; // 默认不保留
    }

    private byte[] buildMessagePayload(ReportData data, ReportConfig config) {
        String dataFormat = getStringConfig("dataFormat", "JSON");

        switch (dataFormat.toUpperCase()) {
            case "TEXT":
                return buildTextPayload(data, config);
            case "BINARY":
                return buildBinaryPayload(data, config);
            case "JSON":
            default:
                return buildJsonPayload(data, config);
        }
    }

    private byte[] buildJsonPayload(ReportData data, ReportConfig config) {
        Map<String, Object> jsonData = new LinkedHashMap<>();
        String messageId = data.getBatchId();
        if (messageId == null) {
            messageId = UUID.randomUUID().toString();
        }
        jsonData.put("id", messageId);
        jsonData.put("version", MessageConstant.MESSAGE_VERSION_1_0);
        jsonData.put("method", data.getMethod());
        jsonData.put("deviceId", data.getDeviceId());
        jsonData.put("timestamp", data.getTimestamp());

        Map<String, Object> params = new LinkedHashMap<>();
        if (data.hasProperties()) {
            params.putAll(data.getProperties());
        } else if (data.getPointCode() != null) {
            params.put(data.getPointCode(), data.getValue());
        }
        jsonData.put("params", params);

        if (!data.getPropertyQuality().isEmpty()) {
            jsonData.put("quality", data.getPropertyQuality());
        }
        if (!data.getPropertyTs().isEmpty()) {
            jsonData.put("propertyTs", data.getPropertyTs());
        }
        if (data.getMetadata() != null && !data.getMetadata().isEmpty()) {
            jsonData.put("metadata", data.getMetadata());
        }

        String jsonString = simpleJsonEncode(jsonData);
        return jsonString.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private byte[] buildTextPayload(ReportData data, ReportConfig config) {
        StringBuilder text = new StringBuilder();
        text.append("pointCode=").append(data.getPointCode()).append(";");
        text.append("value=").append(data.getValue()).append(";");
        text.append("timestamp=").append(data.getTimestamp()).append(";");

        if (data.getQuality() != null) {
            text.append("quality=").append(data.getQuality()).append(";");
        }

        return text.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private byte[] buildBinaryPayload(ReportData data, ReportConfig config) {
        // 简化实现
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        java.io.DataOutputStream dos = new java.io.DataOutputStream(baos);

        try {
            // 写入点位编码
            if (data.getPointCode() != null) {
                byte[] pointCodeBytes = data.getPointCode().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                dos.writeInt(pointCodeBytes.length);
                dos.write(pointCodeBytes);
            } else {
                dos.writeInt(0);
            }

            // 写入时间戳
            dos.writeLong(data.getTimestamp());

            // 写入数据值
            if (data.getValue() instanceof Number) {
                dos.writeDouble(((Number) data.getValue()).doubleValue());
            } else {
                dos.writeDouble(0.0);
            }

            dos.flush();
            return baos.toByteArray();

        } catch (java.io.IOException e) {
            log.error("构建二进制消息失败", e);
            return new byte[0];
        }
    }

    private String simpleJsonEncode(Map<String, Object> data) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");

        boolean first = true;
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            if (!first) {
                sb.append(",");
            }
            first = false;

            sb.append("\"").append(entry.getKey()).append("\":");

            Object value = entry.getValue();
            if (value instanceof String) {
                sb.append("\"").append(escapeJson((String) value)).append("\"");
            } else if (value instanceof Number) {
                sb.append(value);
            } else if (value instanceof Boolean) {
                sb.append(value);
            } else if (value instanceof Map) {
                sb.append(simpleJsonEncode((Map<String, Object>) value));
            } else if (value == null) {
                sb.append("null");
            } else {
                sb.append("\"").append(escapeJson(value.toString())).append("\"");
            }
        }

        sb.append("}");
        return sb.toString();
    }

    private String escapeJson(String text) {
        if (text == null) {
            return "";
        }

        return text.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }

    private void updateSubscriptions(MqttConnectionConfig connConfig) {
        List<String> topics = connConfig.getSubscribeTopics();
        if (topics != null && !topics.isEmpty()) {
            subscriptionManager.updateSubscriptions(connConfig, topics);
        }
    }

    // =============== 内部类 ===============

    /**
     * MQTT连接配置类 - v5
     */
    @Data
    private static class MqttConnectionConfig {
        private final String targetId;
        private final String host;
        private final int port;
        private String clientId;
        private String username;
        private char[] password;
        private int keepAliveInterval = 60;
        private int connectionTimeout = 30;
        private boolean cleanStart = true; // v5: cleanStart 替代 cleanSession
        private boolean automaticReconnect = true;
        private int maxReconnectDelay = 60000;
        private boolean sslEnabled = false;
        private MqttWillMessage willMessage;
        private String defaultPublishTopic;
        private List<String> subscribeTopics;

        public MqttConnectionConfig(ReportConfig config) {
            this.targetId = config.getTargetId();
            this.host = config.getHost();
            this.port = config.getPort();
        }

        public String getBrokerUrl() {
            String protocol = sslEnabled ? "ssl://" : "tcp://";
            return protocol + host + ":" + port;
        }

        public String getKey() {
            return targetId + "@" + host + ":" + port;
        }
    }

    /**
     * MQTT遗嘱消息类
     */
    private static class MqttWillMessage {
        private String topic;
        private byte[] message;
        private int qos = 1;
        private boolean retained = false;

        public String getTopic() { return topic; }
        public void setTopic(String topic) { this.topic = topic; }

        public byte[] getMessage() { return message; }
        public void setMessage(byte[] message) { this.message = message; }

        public int getQos() { return qos; }
        public void setQos(int qos) { this.qos = qos; }

        public boolean isRetained() { return retained; }
        public void setRetained(boolean retained) { this.retained = retained; }
    }

    /**
     * MQTT发布选项类
     */
    private static class MqttPublishOptions {
        private String topic;
        private int qos = 1;
        private boolean retained = false;

        public String getTopic() { return topic; }
        public void setTopic(String topic) { this.topic = topic; }

        public int getQos() { return qos; }
        public void setQos(int qos) { this.qos = qos; }

        public boolean isRetained() { return retained; }
        public void setRetained(boolean retained) { this.retained = retained; }
    }

    /**
     * 发布任务类
     */
    @Data
    private static class PublishTask {
        private final MqttAsyncClient client;
        private final String topic;
        private final byte[] payload;
        private final MqttPublishOptions options;
        private final String pointCode;
        private final String targetId;

        public PublishTask(MqttAsyncClient client, String topic, byte[] payload,
                           MqttPublishOptions options, String pointCode, String targetId) {
            this.client = client;
            this.topic = topic;
            this.payload = payload;
            this.options = options;
            this.pointCode = pointCode;
            this.targetId = targetId;
        }
    }

    /**
     * 发布结果类
     */
    @Data
    private static class PublishResult {
        private final boolean success;
        private final String errorMessage;
        private final int messageId;
        private final String pointCode;
        private final String targetId;

        public PublishResult(boolean success, String errorMessage, int messageId,
                             String pointCode, String targetId) {
            this.success = success;
            this.errorMessage = errorMessage;
            this.messageId = messageId;
            this.pointCode = pointCode;
            this.targetId = targetId;
        }
    }

    /**
     * MQTT客户端管理器 - v5
     */
    private static class MqttClientManager {
        private final Map<String, MqttAsyncClient> clients = new ConcurrentHashMap<>();
        private final Map<String, MqttConnectionConfig> clientConfigs = new ConcurrentHashMap<>();
        private ScheduledExecutorService monitorExecutor;

        public void init() {
            monitorExecutor = Executors.newSingleThreadScheduledExecutor();
            monitorExecutor.scheduleAtFixedRate(this::monitorClients, 30, 30, TimeUnit.SECONDS);
            log.info("MQTT v5客户端管理器初始化完成");
        }

        public MqttAsyncClient getClient(MqttConnectionConfig config) throws MqttException {
            String configKey = config.getKey();
            return clients.computeIfAbsent(configKey, key -> {
                try {
                    return createMqttClient(config);
                } catch (MqttException e) {
                    throw new RuntimeException("创建MQTT v5客户端失败", e);
                }
            });
        }

        public void updateConnectionConfig(MqttConnectionConfig config) {
            String configKey = config.getKey();
            MqttAsyncClient client = clients.get(configKey);

            if (client != null) {
                try {
                    // 断开旧连接
                    if (client.isConnected()) {
                        client.disconnect();
                    }

                    // 更新配置
                    clientConfigs.put(configKey, config);

                    // 重新连接
                    connectClient(client, config);

                } catch (MqttException e) {
                    log.error("更新MQTT v5客户端配置失败：{}", config.getBrokerUrl(), e);
                }
            }
        }

        public void removeClient(String configKey) {
            MqttAsyncClient client = clients.remove(configKey);
            clientConfigs.remove(configKey);

            if (client != null) {
                try {
                    if (client.isConnected()) {
                        client.disconnect();
                    }
                    client.close();
                } catch (MqttException e) {
                    log.warn("关闭MQTT v5客户端失败：{}", configKey, e);
                }
            }
        }

        public void destroy() {
            // 停止监控任务
            if (monitorExecutor != null) {
                monitorExecutor.shutdown();
                try {
                    if (!monitorExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                        monitorExecutor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    monitorExecutor.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }

            // 关闭所有客户端
            for (Map.Entry<String, MqttAsyncClient> entry : clients.entrySet()) {
                try {
                    MqttAsyncClient client = entry.getValue();
                    if (client.isConnected()) {
                        client.disconnect();
                    }
                    client.close();
                } catch (MqttException e) {
                    log.warn("关闭MQTT v5客户端失败：{}", entry.getKey(), e);
                }
            }

            clients.clear();
            clientConfigs.clear();
            log.info("MQTT v5客户端管理器销毁完成");
        }

        public Map<String, Object> getStatus() {
            Map<String, Object> status = new HashMap<>();
            status.put("clientCount", clients.size());

            Map<String, Object> clientStatus = new HashMap<>();
            for (Map.Entry<String, MqttAsyncClient> entry : clients.entrySet()) {
                MqttAsyncClient client = entry.getValue();
                Map<String, Object> clientInfo = new HashMap<>();
                clientInfo.put("connected", client.isConnected());
                clientInfo.put("serverURI", client.getServerURI());
                clientStatus.put(entry.getKey(), clientInfo);
            }
            status.put("clients", clientStatus);

            return status;
        }

        public Map<String, Object> getStatistics() {
            Map<String, Object> stats = new HashMap<>();

            int connectedCount = 0;
            for (MqttAsyncClient client : clients.values()) {
                if (client.isConnected()) {
                    connectedCount++;
                }
            }

            stats.put("totalClients", clients.size());
            stats.put("connectedClients", connectedCount);
            stats.put("disconnectedClients", clients.size() - connectedCount);

            return stats;
        }

        private MqttAsyncClient createMqttClient(MqttConnectionConfig config) throws MqttException {
            try {
                String brokerUrl = config.getBrokerUrl();
                String clientId = config.getClientId();
                MemoryPersistence persistence = new MemoryPersistence();

                // 总是使用异步客户端
                MqttAsyncClient asyncClient = new MqttAsyncClient(brokerUrl, clientId, persistence);

                MqttConnectionOptions options = buildConnectOptions(config);
                asyncClient.setCallback(new MqttCallbackHandler(config));

                // 连接服务器
                IMqttToken token = asyncClient.connect(options);
                token.waitForCompletion();

                log.info("MQTT v5客户端创建并连接成功：{} -> {}", clientId, brokerUrl);
                return asyncClient; // MqttAsyncClient 实现了 IMqttClient 接口

            } catch (MqttException e) {
                log.error("创建MQTT v5客户端失败：{} -> {}", config.getClientId(), config.getBrokerUrl(), e);
                throw e;
            }
        }

        private void connectClient(MqttAsyncClient client, MqttConnectionConfig config) throws MqttException {
            try {
                MqttConnectionOptions options = buildConnectOptions(config);

                // 判断是否是异步客户端
                if (client != null) {
                    // 异步客户端
                    IMqttToken token = client.connect(options);
                    token.waitForCompletion();
                } else {
                    // 同步客户端
                    client.connect(options);
                }

                log.info("MQTT v5客户端重新连接成功：{}", config.getBrokerUrl());
            } catch (MqttException e) {
                log.error("MQTT v5客户端重新连接失败：{}", config.getBrokerUrl(), e);
                throw e;
            }
        }

        private MqttConnectionOptions buildConnectOptions(MqttConnectionConfig config) {
            MqttConnectionOptions options = new MqttConnectionOptions();

            // v5 基本配置
            options.setCleanStart(config.isCleanStart()); // v5: cleanStart
            options.setConnectionTimeout(config.getConnectionTimeout());
            options.setKeepAliveInterval(config.getKeepAliveInterval());
            options.setAutomaticReconnect(config.isAutomaticReconnect());
            options.setMaxReconnectDelay(config.getMaxReconnectDelay());

            // 认证信息
            if (config.getUsername() != null) {
                options.setUserName(config.getUsername());
            }
            if (config.getPassword() != null) {
                options.setPassword(new String(config.getPassword()).getBytes());
            }

            // 遗嘱消息 - v5
            MqttWillMessage will = config.getWillMessage();
            if (will != null) {
                MqttMessage willMessage = new MqttMessage(will.getMessage());
                willMessage.setQos(will.getQos());
                willMessage.setRetained(will.isRetained());
                options.setWill(will.getTopic(), willMessage);
            }

            // SSL/TLS配置
            if (config.isSslEnabled()) {
                configureSsl(options);
            }

            return options;
        }

        private void configureSsl(MqttConnectionOptions options) {
            // SSL/TLS配置
            // 这里需要根据实际需求配置SSL上下文
        }

        private void monitorClients() {
            for (Map.Entry<String, MqttAsyncClient> entry : clients.entrySet()) {
                String configKey = entry.getKey();
                MqttAsyncClient client = entry.getValue();
                MqttConnectionConfig config = clientConfigs.get(configKey);

                if (config != null && !client.isConnected()) {
                    try {
                        log.info("MQTT v5客户端断开连接，尝试重新连接：{}", configKey);
                        MqttConnectionOptions options = buildConnectOptions(config);

                        // 异步客户端
                        IMqttToken token = client.connect(options);
                        token.waitForCompletion();

                        log.info("MQTT v5客户端重新连接成功：{}", configKey);
                    } catch (MqttException e) {
                        log.warn("MQTT v5客户端重新连接失败：{}", configKey, e);
                    }
                }
            }
        }
    }

    /**
         * MQTT回调处理器 - v5
         */
        private record MqttCallbackHandler(MqttConnectionConfig config) implements MqttCallback {

        @Override
            public void disconnected(MqttDisconnectResponse disconnectResponse) {
                log.warn("MQTT v5连接断开：{}，原因码：{}",
                        config.getBrokerUrl(),
                        disconnectResponse.getReturnCode());
            }

            @Override
            public void mqttErrorOccurred(MqttException exception) {
                log.error("MQTT v5错误：{}", config.getBrokerUrl(), exception);
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) throws Exception {
                log.debug("收到MQTT v5消息：主题={}，QoS={}，内容长度={}",
                        topic, message.getQos(), message.getPayload().length);
            }

            @Override
            public void deliveryComplete(IMqttToken token) {
                // 消息发布完成回调
                log.debug("MQTT v5消息发布完成：消息ID={}", token.getMessageId());
            }

            @Override
            public void connectComplete(boolean reconnect, String serverURI) {
                log.info("MQTT v5连接{}完成：{}", reconnect ? "重连" : "初次", serverURI);
            }

            @Override
            public void authPacketArrived(int reasonCode, MqttProperties properties) {
                log.debug("MQTT v5认证包到达：原因码={}", reasonCode);
            }
        }

    /**
     * 消息发布管理器 - v5
     */
    private static class MessagePublisher {
        private final MqttClientManager clientManager;
        private final ExecutorService publishExecutor;
        private final AtomicLong totalPublishCount = new AtomicLong(0);
        private final AtomicLong successPublishCount = new AtomicLong(0);
        private final AtomicLong failurePublishCount = new AtomicLong(0);

        public MessagePublisher(MqttClientManager clientManager) {
            this.clientManager = clientManager;
            this.publishExecutor = Executors.newFixedThreadPool(
                    Runtime.getRuntime().availableProcessors() * 2
            );
        }

        public void init() {
            log.info("MQTT v5消息发布管理器初始化完成");
        }

        public PublishResult publish(MqttAsyncClient client, String topic,
                                     byte[] payload, MqttPublishOptions options) {
            totalPublishCount.incrementAndGet();
            long startTime = System.currentTimeMillis();

            try {
                // 创建MQTT v5消息
                MqttMessage message = new MqttMessage(payload);
                message.setQos(options.getQos());
                message.setRetained(options.isRetained());

                IMqttToken token = null;
                int messageId = 0;

                // 根据客户端类型发布消息
                if (client != null) {
                    // 异步客户端
                    token = client.publish(topic, message);

                    // 等待发布完成（根据QoS级别）
                    if (options.getQos() > 0) {
                        token.waitForCompletion(5000); // 等待5秒
                    }

                    // 获取消息ID
                    try {
                        MqttPublish publishPacket = (MqttPublish) token.getRequestMessage();
                        if (publishPacket != null) {
                            messageId = publishPacket.getMessageId();
                        }
                    } catch (Exception e) {
                        messageId = (int)(System.currentTimeMillis() % 1000);
                    }
                } else {
                    // 同步客户端
                    client.publish(topic, message);
                    // 同步发布无法获取token，使用默认消息ID
                    messageId = (int)(System.currentTimeMillis() % 10000);
                }

                // 检查发布结果
                MqttException exception = (token != null) ? token.getException() : null;
                if (exception == null) {
                    successPublishCount.incrementAndGet();
                    long costTime = System.currentTimeMillis() - startTime;

                    log.debug("MQTT v5消息发布成功：主题={}，QoS={}，耗时={}ms",
                            topic, options.getQos(), costTime);

                    return new PublishResult(true, null, messageId, null, null);
                } else {
                    failurePublishCount.incrementAndGet();
                    log.warn("MQTT v5消息发布失败：主题={}，错误={}", topic, exception.getMessage());
                    return new PublishResult(false, exception.getMessage(), 0, null, null);
                }

            } catch (Exception e) {
                failurePublishCount.incrementAndGet();
                log.error("MQTT v5消息发布异常：主题={}", topic, e);
                return new PublishResult(false, e.getMessage(), 0, null, null);
            }
        }

        public List<PublishResult> publishBatch(List<PublishTask> tasks) {
            List<PublishResult> results = new ArrayList<>(tasks.size());
            List<Future<PublishResult>> futures = new ArrayList<>(tasks.size());

            // 提交所有发布任务
            for (PublishTask task : tasks) {
                Future<PublishResult> future = publishExecutor.submit(() ->
                        publish(task.getClient(), task.getTopic(), task.getPayload(), task.getOptions())
                );
                futures.add(future);
            }

            // 收集所有结果
            for (int i = 0; i < futures.size(); i++) {
                Future<PublishResult> future = futures.get(i);
                PublishTask task = tasks.get(i);

                try {
                    PublishResult publishResult = future.get(10, TimeUnit.SECONDS);
                    // 添加业务信息
                    PublishResult resultWithInfo = new PublishResult(
                            publishResult.isSuccess(),
                            publishResult.getErrorMessage(),
                            publishResult.getMessageId(),
                            task.getPointCode(),
                            task.getTargetId()
                    );
                    results.add(resultWithInfo);
                } catch (Exception e) {
                    log.error("MQTT v5批量发布任务执行失败", e);
                    results.add(new PublishResult(
                            false,
                            "任务执行失败: " + e.getMessage(),
                            0,
                            task.getPointCode(),
                            task.getTargetId()
                    ));
                }
            }

            return results;
        }

        public void destroy() {
            // 关闭线程池
            if (publishExecutor != null) {
                publishExecutor.shutdown();
                try {
                    if (!publishExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                        publishExecutor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    publishExecutor.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }

            log.info("MQTT v5消息发布管理器销毁完成");
        }

        public Map<String, Object> getStatistics() {
            Map<String, Object> stats = new HashMap<>();

            long total = totalPublishCount.get();
            long success = successPublishCount.get();
            long failure = failurePublishCount.get();

            stats.put("totalPublishCount", total);
            stats.put("successPublishCount", success);
            stats.put("failurePublishCount", failure);

            if (total > 0) {
                double successRate = (double) success / total * 100;
                stats.put("publishSuccessRate", String.format("%.2f%%", successRate));
            }

            return stats;
        }
    }

    /**
     * 订阅管理器 - v5
     */
    private static class SubscriptionManager {
        private final MqttClientManager clientManager;
        private final Map<String, Set<String>> clientSubscriptions = new ConcurrentHashMap<>();
        private final AtomicLong totalSubscribeCount = new AtomicLong(0);

        public SubscriptionManager(MqttClientManager clientManager) {
            this.clientManager = clientManager;
        }

        public void init() {
            log.info("MQTT v5订阅管理器初始化完成");
        }

        public void updateSubscriptions(MqttConnectionConfig config, List<String> topics) {
            String configKey = config.getKey();

            try {
                MqttAsyncClient client = clientManager.getClient(config);
                if (client == null || !client.isConnected()) {
                    log.warn("无法更新订阅，客户端未连接：{}", configKey);
                    return;
                }

                // 获取当前订阅
                Set<String> currentTopics = clientSubscriptions.getOrDefault(configKey, new HashSet<>());
                Set<String> newTopics = new HashSet<>(topics);

                // 需要取消的订阅
                Set<String> toUnsubscribe = new HashSet<>(currentTopics);
                toUnsubscribe.removeAll(newTopics);

                // 需要新增的订阅
                Set<String> toSubscribe = new HashSet<>(newTopics);
                toSubscribe.removeAll(currentTopics);

                // 取消订阅
                for (String topic : toUnsubscribe) {
                    try {
                        client.unsubscribe(topic);
                        log.debug("取消MQTT v5订阅：{} -> {}", configKey, topic);
                    } catch (MqttException e) {
                        log.warn("取消MQTT v5订阅失败：{} -> {}", configKey, topic, e);
                    }
                }

                // 新增订阅
                for (String topic : toSubscribe) {
                    try {
                        // v5: subscribe 返回 IMqttToken
                        IMqttToken token = client.subscribe(topic, 1); // 默认QoS 1
                        token.waitForCompletion(5000);
                        totalSubscribeCount.incrementAndGet();
                        log.debug("新增MQTT v5订阅：{} -> {}", configKey, topic);
                    } catch (MqttException e) {
                        log.warn("新增MQTT v5订阅失败：{} -> {}", configKey, topic, e);
                    }
                }

                // 更新订阅记录
                clientSubscriptions.put(configKey, newTopics);

                log.info("MQTT v5订阅更新完成：{}，订阅数：{}", configKey, newTopics.size());

            } catch (Exception e) {
                log.error("更新MQTT v5订阅失败：{}", configKey, e);
            }
        }

        public void removeSubscriptions(String configKey) {
            Set<String> topics = clientSubscriptions.remove(configKey);
            if (topics != null && !topics.isEmpty()) {
                log.info("移除MQTT v5订阅：{}，订阅数：{}", configKey, topics.size());
            }
        }

        public void destroy() {
            clientSubscriptions.clear();
            log.info("MQTT v5订阅管理器销毁完成");
        }

        public Map<String, Object> getStatistics() {
            Map<String, Object> stats = new HashMap<>();

            int totalSubscriptions = 0;
            for (Set<String> topics : clientSubscriptions.values()) {
                totalSubscriptions += topics.size();
            }

            stats.put("totalSubscribeCount", totalSubscribeCount.get());
            stats.put("currentSubscriptions", totalSubscriptions);
            stats.put("clientCount", clientSubscriptions.size());

            return stats;
        }
    }
}
