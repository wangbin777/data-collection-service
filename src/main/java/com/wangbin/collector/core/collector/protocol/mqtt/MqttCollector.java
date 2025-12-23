package com.wangbin.collector.core.collector.protocol.mqtt;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONPath;
import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.core.collector.protocol.base.BaseCollector;
import com.wangbin.collector.core.config.CollectorProperties;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.wangbin.collector.core.collector.protocol.mqtt.MqttCollectorUtils.asBoolean;
import static com.wangbin.collector.core.collector.protocol.mqtt.MqttCollectorUtils.asInt;

@Slf4j
public class MqttCollector extends BaseCollector {

    private CollectorProperties.MqttConfig defaultConfig;
    private MqttConnectionConfig connectionConfig;
    private MqttClientAdapter clientAdapter;

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
        this.clientAdapter = connectionConfig.getVersion() == MqttProtocolVersion.V5
                ? new PahoV5ClientAdapter(connectionConfig, this::handleIncomingMessage)
                : new PahoV3ClientAdapter(connectionConfig, this::handleIncomingMessage);
        clientAdapter.connect();
        for (MqttTopicSubscription subscription : connectionConfig.getDefaultTopics()) {
            ensureTopicSubscription(subscription.getTopic(), subscription.getQos());
            baseSubscribedTopics.add(subscription.getTopic());
        }
    }

    @Override
    protected void doDisconnect() throws Exception {
        if (clientAdapter != null) {
            try {
                clientAdapter.disconnect();
            } finally {
                clientAdapter = null;
            }
        }
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
                    clientAdapter.unsubscribe(topic);
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
        status.put("connected", clientAdapter != null && clientAdapter.isConnected());
        status.put("subscriptions", topicBindings.keySet());
        status.put("cachedPoints", latestValues.size());
        status.put("lastTimestamps", latestTimestamps);
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
        int count = topicRefCount.getOrDefault(topic, 0);
        if (count == 0 && clientAdapter != null) {
            clientAdapter.subscribe(topic, qos);
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
            if (!baseSubscribedTopics.contains(topic) && clientAdapter != null) {
                clientAdapter.unsubscribe(topic);
            }
        } else {
            topicRefCount.put(topic, current - 1);
        }
    }

    private void publish(String topic, byte[] payload, int qos, boolean retained) throws Exception {
        if (clientAdapter == null || !clientAdapter.isConnected()) {
            throw new IllegalStateException("MQTT client not connected");
        }
        clientAdapter.publish(topic, payload, qos, retained);
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
