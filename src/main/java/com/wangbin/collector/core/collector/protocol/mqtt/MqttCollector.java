package com.wangbin.collector.core.collector.protocol.mqtt;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONPath;
import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.common.domain.entity.DeviceConnection;
import com.wangbin.collector.core.collector.protocol.base.BaseCollector;
import com.wangbin.collector.core.config.CollectorProperties;
import com.wangbin.collector.core.connection.adapter.ConnectionAdapter;
import com.wangbin.collector.core.connection.adapter.MqttConnectionAdapter;
import com.wangbin.collector.core.connection.adapter.MqttReceivedMessage;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
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
        ConnectionAdapter adapter = connectionManager.createConnection(deviceInfo);
        connectionManager.connect(deviceInfo.getDeviceId());
        if (!(adapter instanceof MqttConnectionAdapter mqttAdapter)) {
            throw new IllegalStateException("MQTT连接适配器类型不匹配");
        }
        this.mqttConnection = mqttAdapter;
        this.mqttConnection.addMessageListener(inboundListener);
        for (MqttTopicSubscription subscription : getDefaultSubscriptions()) {
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
        status.put("brokerUrl", getBrokerUrl());
        status.put("clientId", getClientId());
        status.put("protocolVersion", getProtocolVersion());
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
        int defaultQos = getDefaultQos();
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
    }


    private Map<String, Object> getConnectionProperties() {
        return deviceInfo.getConnectionConfig().getExtraParams();
    }

    private String getBrokerUrl() {
        String url = deviceInfo.getConnectionConfig().getUrl();
        if (url == null || url.isBlank()) {
            url = toString(getConnectionProperties().get("brokerUrl"), "N/A");
        }
        return url != null ? url : "N/A";
    }

    private String getClientId() {
        String clientId = deviceInfo.getConnectionConfig().getClientId();
        if (clientId == null || clientId.isBlank()) {
            clientId = toString(getConnectionProperties().get("clientId"), deviceInfo.getDeviceId() + "_mqtt");
        }
        return clientId;
    }

    private String getProtocolVersion() {
        return toString(getConnectionProperties().get("version"), "UNKNOWN");
    }

    private int getDefaultQos() {
        return getIntValue(getConnectionProperties().get("subscribeQos"),
                defaultConfig != null ? defaultConfig.getQos() : 1);
    }

    private List<MqttTopicSubscription> getDefaultSubscriptions() {
        return parseTopics(getConnectionProperties().get("subscribeTopics"), getDefaultQos());
    }

    private List<MqttTopicSubscription> parseTopics(Object value, int defaultQos) {
        List<MqttTopicSubscription> topics = new ArrayList<>();
        if (value == null) {
            return topics;
        }
        if (value instanceof Collection<?> collection) {
            for (Object item : collection) {
                if (item == null) {
                    continue;
                }
                String text = item.toString().trim();
                if (!text.isEmpty()) {
                    topics.add(new MqttTopicSubscription(text, defaultQos));
                }
            }
            return topics;
        }
        String[] parts = value.toString().split(",");
        for (String part : parts) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) {
                topics.add(new MqttTopicSubscription(trimmed, defaultQos));
            }
        }
        return topics;
    }

    private int getIntValue(Object value, int defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number number) {
            return number.intValue();
        }
        try {
            return Integer.parseInt(value.toString());
        } catch (NumberFormatException ex) {
            return defaultValue;
        }
    }

    private long getLongValue(Object value, long defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number number) {
            return number.longValue();
        }
        try {
            return Long.parseLong(value.toString());
        } catch (NumberFormatException ex) {
            return defaultValue;
        }
    }

    private boolean getBooleanValue(Object value, boolean defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Boolean bool) {
            return bool;
        }
        return Boolean.parseBoolean(value.toString());
    }

    private String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return null;
    }

    private String toString(Object value) {
        return value != null ? value.toString() : null;
    }

    private String toString(Object value, String defaultValue) {
        return value != null ? value.toString() : defaultValue;
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
                id -> MqttPointOptions.from(point, getDefaultQos()));
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
        int qos = asInt(params.get("qos"), getDefaultQos());
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
        int qos = asInt(params.get("qos"), getDefaultQos());
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
