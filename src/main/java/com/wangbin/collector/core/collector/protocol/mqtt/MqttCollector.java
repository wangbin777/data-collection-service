package com.wangbin.collector.core.collector.protocol.mqtt;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONPath;
import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.core.collector.protocol.base.BaseCollector;
import com.wangbin.collector.core.config.CollectorProperties;
import com.wangbin.collector.core.connection.adapter.ConnectionAdapter;
import com.wangbin.collector.core.connection.adapter.MqttConnectionAdapter;
import com.wangbin.collector.core.connection.adapter.MqttReceivedMessage;
import com.wangbin.collector.core.connection.model.ConnectionConfig;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static com.wangbin.collector.core.collector.protocol.mqtt.MqttCollectorUtils.asBoolean;
import static com.wangbin.collector.core.collector.protocol.mqtt.MqttCollectorUtils.asInt;

@Slf4j
public class MqttCollector extends BaseCollector {

    private CollectorProperties.MqttConfig defaultConfig;
    private MqttConnectionConfig connectionConfig;
    private ConnectionConfig managedConnectionConfig;
    private MqttConnectionAdapter mqttConnection;
    private final Consumer<MqttReceivedMessage> inboundListener = this::handleInboundMessage;

    private final Map<String, DataPoint> pointDefinitions = new ConcurrentHashMap<>();
    private final Map<String, MqttPointOptions> pointOptions = new ConcurrentHashMap<>();
    private final Map<String, Object> latestValues = new ConcurrentHashMap<>();
    private final Map<String, Long> latestTimestamps = new ConcurrentHashMap<>();
    private final Map<String, Set<String>> topicBindings = new ConcurrentHashMap<>();
    private final Map<String, Integer> topicRefCount = new ConcurrentHashMap<>();
    private final Set<String> baseSubscribedTopics = ConcurrentHashMap.newKeySet();

    @Override
    public String getCollectorType() {
        return "MQTT";
    }

    @Override
    public String getProtocolType() {
        return "MQTT";
    }

    @Override
    protected void doConnect() throws Exception {
        initConfig();
        if (connectionManager == null) {
            throw new IllegalStateException("连接管理器未初始化");
        }
        this.managedConnectionConfig = buildConnectionConfig();
        ConnectionAdapter adapter = connectionManager.createConnection(managedConnectionConfig);
        connectionManager.connect(deviceInfo.getDeviceId());
        if (!(adapter instanceof MqttConnectionAdapter mqttAdapter)) {
            throw new IllegalStateException("MQTT连接适配器类型不匹配");
        }
        this.mqttConnection = mqttAdapter;
        this.mqttConnection.addMessageListener(inboundListener);
        for (MqttTopicSubscription subscription : connectionConfig.getDefaultTopics()) {
            ensureTopicSubscription(subscription.getTopic(), subscription.getQos());
            baseSubscribedTopics.add(subscription.getTopic());
        }
    }

    @Override
    protected void doDisconnect() throws Exception {
        if (mqttConnection != null) {
            mqttConnection.removeMessageListener(inboundListener);
        }
        if (connectionManager != null && deviceInfo != null) {
            connectionManager.removeConnection(deviceInfo.getDeviceId());
        }
        mqttConnection = null;
        managedConnectionConfig = null;
        topicBindings.clear();
        topicRefCount.clear();
        baseSubscribedTopics.clear();
        latestValues.clear();
        latestTimestamps.clear();
    }

    @Override
    protected Object doReadPoint(DataPoint point) {
        return latestValues.get(point.getPointId());
    }

    @Override
    protected Map<String, Object> doReadPoints(List<DataPoint> points) {
        Map<String, Object> result = new ConcurrentHashMap<>();
        if (points == null) {
            return result;
        }
        for (DataPoint point : points) {
            result.put(point.getPointId(), latestValues.get(point.getPointId()));
        }
        return result;
    }

    @Override
    protected boolean doWritePoint(DataPoint point, Object value) throws Exception {
        MqttPointOptions options = resolvePointOptions(point);
        byte[] payload = buildPayloadForWrite(value, options);
        publish(options.getWriteTopic(), payload, options.getQos(), options.isRetain());
        return true;
    }

    @Override
    protected Map<String, Boolean> doWritePoints(Map<DataPoint, Object> points) throws Exception {
        Map<String, Boolean> result = new ConcurrentHashMap<>();
        if (points == null) {
            return result;
        }
        for (Map.Entry<DataPoint, Object> entry : points.entrySet()) {
            boolean success = doWritePoint(entry.getKey(), entry.getValue());
            result.put(entry.getKey().getPointId(), success);
        }
        return result;
    }

    @Override
    protected void doSubscribe(List<DataPoint> points) throws Exception {
        if (points == null || points.isEmpty()) {
            return;
        }
        for (DataPoint point : points) {
            MqttPointOptions options = resolvePointOptions(point);
            bindPointToTopic(point, options);
        }
    }

    @Override
    protected void doUnsubscribe(List<DataPoint> points) throws Exception {
        if (points == null || points.isEmpty()) {
            for (String topic : List.copyOf(topicBindings.keySet())) {
                if (!baseSubscribedTopics.contains(topic)) {
                    if (mqttConnection != null) {
                        mqttConnection.unsubscribe(topic);
                    }
                    topicRefCount.remove(topic);
                }
            }
            topicBindings.clear();
            pointOptions.clear();
            pointDefinitions.clear();
            latestValues.clear();
            latestTimestamps.clear();
            return;
        }
        for (DataPoint point : points) {
            MqttPointOptions options = pointOptions.remove(point.getPointId());
            pointDefinitions.remove(point.getPointId());
            latestValues.remove(point.getPointId());
            latestTimestamps.remove(point.getPointId());
            if (options != null) {
                removePointFromTopic(point.getPointId(), options.getTopic());
            }
        }
    }

    @Override
    protected Map<String, Object> doGetDeviceStatus() {
        Map<String, Object> status = new ConcurrentHashMap<>();
        status.put("brokerUrl", connectionConfig != null ? connectionConfig.getBrokerUrl() : "N/A");
        status.put("clientId", connectionConfig != null ? connectionConfig.getClientId() : "N/A");
        status.put("protocolVersion", connectionConfig != null ? connectionConfig.getVersion().name() : "UNKNOWN");
        status.put("connected", mqttConnection != null && mqttConnection.isConnected());
        status.put("subscriptions", topicBindings.keySet());
        status.put("cachedPoints", latestValues.size());
        status.put("lastTimestamps", latestTimestamps);
        status.put("connectionStats", mqttConnection != null ? mqttConnection.getStatistics() : Collections.emptyMap());
        return status;
    }

    @Override
    protected Object doExecuteCommand(int unitId, String command, Map<String, Object> params) throws Exception {
        String normalized = command != null ? command.toLowerCase(Locale.ROOT) : "";
        Map<String, Object> safeParams = params != null ? params : Collections.emptyMap();
        return switch (normalized) {
            case "publish" -> executePublishCommand(safeParams);
            case "subscribe" -> executeSubscribeCommand(safeParams);
            case "unsubscribe" -> executeUnsubscribeCommand(safeParams);
            case "status" -> doGetDeviceStatus();
            default -> throw new IllegalArgumentException("Unsupported MQTT command: " + command);
        };
    }

    @Override
    protected void buildReadPlans(String deviceId, List<DataPoint> points) {
        if (points == null) {
            return;
        }
        int defaultQos = connectionConfig != null ? connectionConfig.getDefaultQos()
                : (collectorProperties != null ? collectorProperties.getMqtt().getQos() : 1);
        for (DataPoint point : points) {
            pointDefinitions.put(point.getPointId(), point);
            pointOptions.put(point.getPointId(), MqttPointOptions.from(point, defaultQos));
        }
        log.info("MQTT loaded {} points for device {}", pointOptions.size(), deviceId);
    }

    private void initConfig() {
        this.defaultConfig = collectorProperties != null
                ? collectorProperties.getMqtt()
                : new CollectorProperties.MqttConfig();
        this.connectionConfig = MqttConnectionConfig.from(deviceInfo, defaultConfig);
    }

    private ConnectionConfig buildConnectionConfig() {
        ConnectionConfig cfg = new ConnectionConfig();
        cfg.setDeviceId(deviceInfo.getDeviceId());
        cfg.setDeviceName(deviceInfo.getDeviceName());
        cfg.setProtocolType(deviceInfo.getProtocolType());
        cfg.setConnectionType("MQTT");
        cfg.setProductKey(deviceInfo.getProductKey());
        Object deviceSecret = deviceInfo.getAuthParam("deviceSecret");
        if (deviceSecret != null) {
            cfg.setDeviceSecret(deviceSecret.toString());
        }
        cfg.setUrl(connectionConfig.getBrokerUrl());
        cfg.setClientId(connectionConfig.getClientId());
        cfg.setUsername(connectionConfig.getUsername());
        cfg.setPassword(connectionConfig.getPassword());
        cfg.setConnectTimeout(connectionConfig.getConnectionTimeoutSeconds() * 1000);
        int ioTimeout = Math.max(1, connectionConfig.getKeepAliveIntervalSeconds()) * 1000;
        cfg.setReadTimeout(ioTimeout);
        cfg.setWriteTimeout(ioTimeout);
        cfg.setHeartbeatInterval(connectionConfig.getKeepAliveIntervalSeconds() * 1000);
        cfg.setAutoReconnect(connectionConfig.isAutomaticReconnect());
        cfg.setGroupId(connectionConfig.getGroupId() != null && !connectionConfig.getGroupId().isBlank()
                ? connectionConfig.getGroupId()
                : deviceInfo.getGroupId());
        cfg.setMaxGroupConnections(Math.max(0, connectionConfig.getMaxGroupConnections()));
        cfg.setMaxPendingMessages(Math.max(1, connectionConfig.getMaxPendingMessages()));
        cfg.setDispatchBatchSize(Math.max(1, connectionConfig.getDispatchBatchSize()));
        cfg.setDispatchFlushInterval(Math.max(0L, connectionConfig.getDispatchFlushIntervalMillis()));
        cfg.setOverflowStrategy(connectionConfig.getOverflowStrategy());
        cfg.setProtocolConfig(buildProtocolOverrides());
        if (deviceInfo.getConnectionConfig() != null) {
            cfg.setExtraParams(new HashMap<>(deviceInfo.getConnectionConfig()));
        }
        try {
            URI uri = new URI(connectionConfig.getBrokerUrl());
            if (uri.getHost() != null) {
                cfg.setHost(uri.getHost());
            }
            if (uri.getPort() > 0) {
                cfg.setPort(uri.getPort());
            } else if (cfg.getHost() != null) {
                cfg.setPort(isSslScheme(uri.getScheme()) ? 8883 : 1883);
            }
            cfg.setSslEnabled(isSslScheme(uri.getScheme()));
        } catch (Exception e) {
            log.warn("解析 MQTT 地址失败: {}", connectionConfig.getBrokerUrl(), e);
            cfg.setHost(deviceInfo.getIpAddress());
            cfg.setPort(deviceInfo.getPort());
        }
        return cfg;
    }

    private Map<String, Object> buildProtocolOverrides() {
        Map<String, Object> overrides = new HashMap<>();
        if (deviceInfo.getProtocolConfig() != null) {
            overrides.putAll(deviceInfo.getProtocolConfig());
        }
        overrides.put("version", connectionConfig.getVersion().name());
        overrides.put("cleanSession", connectionConfig.isCleanSession());
        overrides.put("autoReconnect", connectionConfig.isAutomaticReconnect());
        List<String> topics = new java.util.ArrayList<>();
        for (MqttTopicSubscription sub : connectionConfig.getDefaultTopics()) {
            if (sub.getTopic() != null && !sub.getTopic().isBlank()) {
                topics.add(sub.getTopic());
            }
        }
        if (!topics.isEmpty()) {
            overrides.put("subscribeTopics", topics);
            overrides.put("subscribeQos", connectionConfig.getDefaultQos());
        }
        return overrides;
    }

    private boolean isSslScheme(String scheme) {
        if (scheme == null) {
            return false;
        }
        String lower = scheme.toLowerCase(Locale.ROOT);
        return lower.startsWith("ssl") || lower.startsWith("tls") || lower.startsWith("mqtts");
    }

    private MqttPointOptions resolvePointOptions(DataPoint point) {
        return pointOptions.computeIfAbsent(point.getPointId(),
                id -> MqttPointOptions.from(point, connectionConfig != null ? connectionConfig.getDefaultQos() : 1));
    }

    private void bindPointToTopic(DataPoint point, MqttPointOptions options) throws Exception {
        pointDefinitions.put(point.getPointId(), point);
        topicBindings.computeIfAbsent(options.getTopic(), t -> ConcurrentHashMap.newKeySet())
                .add(point.getPointId());
        ensureTopicSubscription(options.getTopic(), options.getQos());
    }

    private void removePointFromTopic(String pointId, String topic) throws Exception {
        Set<String> bindings = topicBindings.get(topic);
        if (bindings != null) {
            bindings.remove(pointId);
            if (bindings.isEmpty() && !baseSubscribedTopics.contains(topic)) {
                topicBindings.remove(topic);
            }
        }
        decrementTopicSubscription(topic);
    }

    private synchronized void ensureTopicSubscription(String topic, int qos) throws Exception {
        if (topic == null || topic.isBlank()) {
            return;
        }
        if (mqttConnection == null) {
            throw new IllegalStateException("MQTT连接未建立");
        }
        int count = topicRefCount.getOrDefault(topic, 0);
        if (count == 0 && !mqttConnection.isSubscribed(topic)) {
            mqttConnection.subscribe(topic, qos);
        }
        topicRefCount.put(topic, count + 1);
    }

    private synchronized void decrementTopicSubscription(String topic) throws Exception {
        Integer current = topicRefCount.get(topic);
        if (current == null) {
            return;
        }
        if (current <= 1) {
            topicRefCount.remove(topic);
            if (!baseSubscribedTopics.contains(topic) && mqttConnection != null) {
                mqttConnection.unsubscribe(topic);
            }
        } else {
            topicRefCount.put(topic, current - 1);
        }
    }

    private void publish(String topic, byte[] payload, int qos, boolean retained) throws Exception {
        if (mqttConnection == null || !mqttConnection.isConnected()) {
            throw new IllegalStateException("MQTT client not connected");
        }
        mqttConnection.publish(topic, payload, qos, retained);
    }

    private byte[] buildPayloadForWrite(Object value, MqttPointOptions options) {
        String payloadText;
        if (options.getPublishTemplate() != null && !options.getPublishTemplate().isBlank()) {
            payloadText = options.getPublishTemplate().replace("${value}", Objects.toString(value, ""));
        } else {
            payloadText = Objects.toString(value, "");
        }
        return payloadText.getBytes(options.getCharset());
    }

    private Object executePublishCommand(Map<String, Object> params) throws Exception {
        String topic = Objects.toString(params.get("topic"), "");
        if (topic.isBlank()) {
            throw new IllegalArgumentException("topic is required");
        }
        Object payloadObj = params.getOrDefault("payload", "");
        int qos = asInt(params.get("qos"), connectionConfig != null ? connectionConfig.getDefaultQos() : 1);
        boolean retained = asBoolean(params.get("retained"), false);
        byte[] payload = Objects.toString(payloadObj, "").getBytes(StandardCharsets.UTF_8);
        publish(topic, payload, qos, retained);
        return Map.of("topic", topic, "status", "success");
    }

    private Object executeSubscribeCommand(Map<String, Object> params) throws Exception {
        String topic = Objects.toString(params.get("topic"), "");
        if (topic.isBlank()) {
            throw new IllegalArgumentException("topic is required");
        }
        int qos = asInt(params.get("qos"), connectionConfig != null ? connectionConfig.getDefaultQos() : 1);
        ensureTopicSubscription(topic, qos);
        baseSubscribedTopics.add(topic);
        return Map.of("topic", topic, "status", "subscribed");
    }

    private Object executeUnsubscribeCommand(Map<String, Object> params) throws Exception {
        String topic = Objects.toString(params.get("topic"), "");
        if (topic.isBlank()) {
            throw new IllegalArgumentException("topic is required");
        }
        baseSubscribedTopics.remove(topic);
        topicBindings.remove(topic);
        if (topicRefCount.containsKey(topic)) {
            topicRefCount.put(topic, 1);
            decrementTopicSubscription(topic);
        }
        return Map.of("topic", topic, "status", "unsubscribed");
    }

    private void handleInboundMessage(MqttReceivedMessage message) {
        if (message == null) {
            return;
        }
        MqttMessageEnvelope envelope = new MqttMessageEnvelope(
                message.getPayload(),
                message.getQos(),
                message.isRetained(),
                message.getUserProperties());
        handleIncomingMessage(message.getTopic(), envelope);
    }

    private void handleIncomingMessage(String topic, MqttMessageEnvelope envelope) {
        Set<String> bindings = topicBindings.get(topic);
        if (bindings == null || bindings.isEmpty()) {
            log.debug("收到无绑定点位的 MQTT 消息，topic={}", topic);
            return;
        }
        for (String pointId : bindings) {
            DataPoint point = pointDefinitions.get(pointId);
            MqttPointOptions options = pointOptions.get(pointId);
            if (point == null || options == null) {
                continue;
            }
            try {
                Object converted = convertPayload(point, options, envelope.getPayload());
                if (converted != null) {
                    latestValues.put(pointId, converted);
                    latestTimestamps.put(pointId, System.currentTimeMillis());
                }
            } catch (Exception ex) {
                log.warn("解析 MQTT 消息失败，pointId={}, topic={}", pointId, topic, ex);
            }
        }
    }

    private Object convertPayload(DataPoint point, MqttPointOptions options, byte[] payload) {
        if (payload == null) {
            return null;
        }
        byte[] actualPayload = payload;
        if ("base64".equalsIgnoreCase(options.getPayloadEncoding())) {
            actualPayload = Base64.getDecoder().decode(payload);
        }
        String text = new String(actualPayload, options.getCharset());
        Object raw = text;
        if (options.getJsonPath() != null && !options.getJsonPath().isBlank()) {
            Object json = JSON.parse(text);
            raw = JSONPath.eval(json, options.getJsonPath());
        }
        return convertToDataType(point.getDataType(), raw);
    }

    private Object convertToDataType(String dataType, Object raw) {
        if (raw == null) {
            return null;
        }
        if (dataType == null || dataType.isBlank()) {
            return raw;
        }
        String type = dataType.trim().toUpperCase(Locale.ROOT);
        try {
            return switch (type) {
                case "INT", "INTEGER" -> toNumber(raw).intValue();
                case "LONG" -> toNumber(raw).longValue();
                case "FLOAT" -> toNumber(raw).floatValue();
                case "DOUBLE" -> toNumber(raw).doubleValue();
                case "BOOLEAN" -> toBoolean(raw);
                case "SHORT" -> toNumber(raw).shortValue();
                case "BYTE" -> toNumber(raw).byteValue();
                default -> raw.toString();
            };
        } catch (Exception ex) {
            log.warn("MQTT 数据类型转换失败: type={}, value={}", type, raw, ex);
            return null;
        }
    }

    private Number toNumber(Object value) {
        if (value instanceof Number number) {
            return number;
        }
        return Double.parseDouble(value.toString());
    }

    private boolean toBoolean(Object value) {
        if (value instanceof Boolean bool) {
            return bool;
        }
        String text = value.toString().trim().toLowerCase(Locale.ROOT);
        if (text.isEmpty()) {
            return false;
        }
        return "true".equals(text) || "1".equals(text) || "on".equals(text) || "yes".equals(text);
    }
}
