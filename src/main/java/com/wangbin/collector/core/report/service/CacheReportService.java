
package com.wangbin.collector.core.report.service;

import com.wangbin.collector.common.constant.MessageConstant;
import com.wangbin.collector.common.constant.ProtocolConstant;
import com.wangbin.collector.common.domain.entity.CollectionConfig;
import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.common.enums.QualityEnum;
import com.wangbin.collector.core.config.manager.ConfigManager;
import com.wangbin.collector.core.config.model.ConfigUpdateEvent;
import com.wangbin.collector.core.processor.ProcessResult;
import com.wangbin.collector.core.report.config.ReportProperties;
import com.wangbin.collector.core.report.model.ReportConfig;
import com.wangbin.collector.core.report.model.ReportData;
import com.wangbin.collector.core.report.model.ReportResult;
import com.wangbin.collector.core.report.shadow.DeviceShadow;
import com.wangbin.collector.core.report.shadow.ShadowManager;
import com.wangbin.collector.core.report.shadow.ShadowManager.EventInfo;
import com.wangbin.collector.core.report.shadow.ShadowManager.ShadowUpdateResult;
import com.wangbin.collector.core.report.shadow.ValueMeta;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 缓存上报服务：聚合快照/变化/事件并统一推送
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CacheReportService {

    private final ReportManager reportManager;
    private final ConfigManager configManager;
    private final ReportProperties reportProperties;
    private final ShadowManager shadowManager;

    private final ConcurrentMap<String, ReportConfig> deviceConfigCache = new ConcurrentHashMap<>();
    private final Set<String> flushingDevices = ConcurrentHashMap.newKeySet();
    private final ConcurrentMap<String, FlushTracker> flushTrackers = new ConcurrentHashMap<>();
    private final Object rateLock = new Object();
    private ScheduledExecutorService flushExecutor;
    private long rateWindowSecond = System.currentTimeMillis() / 1000;
    private int rateWindowCount = 0;

    @PostConstruct
    public void start() {
        if (!isMqttEnabled()) {
            return;
        }
        long interval = Math.max(1000L, reportProperties.getIntervalMs());
        flushExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "report-flush");
            thread.setDaemon(true);
            return thread;
        });
        flushExecutor.scheduleAtFixedRate(this::flushDirtyDevices, interval, interval, TimeUnit.MILLISECONDS);
    }

    @PreDestroy
    public void shutdown() {
        if (flushExecutor != null) {
            flushExecutor.shutdownNow();
        }
    }

    public void reportPoint(String deviceId, String method, DataPoint point, Object cacheValue) {
        if (!isMqttEnabled() || deviceId == null || point == null || cacheValue == null) {
            return;
        }
        ProcessResult processResult = wrapProcessResult(cacheValue);
        if (processResult == null) {
            return;
        }
        ShadowUpdateResult updateResult = shadowManager.apply(deviceId, point, processResult);
        if (updateResult.changeTriggered()) {
            triggerImmediateFlush(deviceId);
        }
        EventInfo eventInfo = updateResult.eventInfo();
        if (eventInfo != null) {
            dispatchEvent(deviceId, point, processResult, eventInfo);
        }
    }

    private ProcessResult wrapProcessResult(Object cacheValue) {
        if (cacheValue instanceof ProcessResult processResult) {
            return processResult;
        }
        ProcessResult result = new ProcessResult();
        result.setSuccess(true);
        result.setRawValue(cacheValue);
        result.setProcessedValue(cacheValue);
        result.setQuality(QualityEnum.GOOD.getCode());
        return result;
    }

    private void triggerImmediateFlush(String deviceId) {
        if (flushExecutor == null) {
            return;
        }
        DeviceShadow shadow = shadowManager.getShadow(deviceId);
        if (shadow == null) {
            return;
        }
        long now = System.currentTimeMillis();
        if (now - shadow.getLastReportAt() < reportProperties.getMinReportIntervalMs()) {
            return;
        }
        flushExecutor.execute(() -> flushDevice(deviceId));
    }

    private void flushDirtyDevices() {
        Set<String> dirtyDevices = shadowManager.getDirtyDevices();
        if (dirtyDevices.isEmpty()) {
            return;
        }
        for (String deviceId : dirtyDevices) {
            try {
                flushDevice(deviceId);
            } catch (Exception e) {
                log.error("刷新设备 {} 缓存数据失败", deviceId, e);
            }
        }
    }

    private void flushDevice(String deviceId) {
        if (!flushingDevices.add(deviceId)) {
            return;
        }
        DeviceShadow shadow = shadowManager.getShadow(deviceId);
        if (shadow == null || shadow.isEmpty()) {
            shadowManager.clearDirty(deviceId);
            flushingDevices.remove(deviceId);
            return;
        }

        ReportData snapshot = buildSnapshot(shadow);
        if (!snapshot.hasProperties()) {
            shadowManager.clearDirty(deviceId);
            flushingDevices.remove(deviceId);
            return;
        }

        ReportConfig reportConfig = resolveReportConfig(deviceId);
        if (reportConfig == null || !reportConfig.validate()) {
            log.warn("跳过设备 {}，上报配置无效", deviceId);
            flushingDevices.remove(deviceId);
            return;
        }

        List<ReportData> chunks = splitSnapshot(snapshot);
        if (chunks.isEmpty()) {
            shadowManager.clearDirty(deviceId);
            flushingDevices.remove(deviceId);
            return;
        }

        long now = System.currentTimeMillis();
        FlushTracker tracker = new FlushTracker(
                deviceId,
                now - reportProperties.getIntervalMs(),
                now,
                Math.max(0, reportProperties.getRetryTimes())
        );
        flushTrackers.put(deviceId, tracker);
        for (ReportData chunk : chunks) {
            dispatch(chunk, reportConfig, false, tracker);
        }
    }

    private ReportData buildSnapshot(DeviceShadow shadow) {
        ReportData data = new ReportData();
        data.setDeviceId(shadow.getDeviceId());
        data.setTimestamp(System.currentTimeMillis());
        data.setPointCode("snapshot");
        data.addMetadata("schemaVersion", reportProperties.getSchemaVersion());
        data.addMetadata("seq", shadow.nextSeq());
        Map<String, ValueMeta> latest = shadow.snapshot();
        latest.forEach((field, meta) -> data.addProperty(field, meta.getValue(), meta.getTimestamp(), meta.getQuality()));
        return data;
    }

    List<ReportData> splitSnapshot(ReportData snapshot) {
        int maxFields = Math.max(1, reportProperties.getMaxPropertiesPerMessage());
        int maxBytes = reportProperties.getMaxPayloadBytes();
        List<ReportData> result = new ArrayList<>();
        if (snapshot.size() <= maxFields && (maxBytes <= 0 || snapshot.estimatePayloadSize() <= maxBytes)) {
            result.add(snapshot);
        } else {
            List<String> fields = new ArrayList<>(snapshot.getProperties().keySet());
            int index = 0;
            while (index < fields.size()) {
                ReportData chunk = snapshot.shallowCopy();
                int added = 0;
                while (index < fields.size() && added < maxFields) {
                    String field = fields.get(index);
                    chunk.addProperty(field,
                            snapshot.getProperties().get(field),
                            snapshot.getPropertyTs().getOrDefault(field, snapshot.getTimestamp()),
                            snapshot.getPropertyQuality().get(field));
                    index++;
                    added++;
                    if (maxBytes > 0 && chunk.estimatePayloadSize() >= maxBytes) {
                        break;
                    }
                }
                result.add(chunk);
            }
        }

        String batchId = UUID.randomUUID().toString();
        for (int i = 0; i < result.size(); i++) {
            ReportData chunk = result.get(i);
            if (result.size() > 1) {
                chunk.setPointCode("snapshot-" + i);
            }
            chunk.applyChunkMetadata(batchId, i, result.size());
        }
        return result;
    }

    private void dispatch(ReportData data,
                          ReportConfig config,
                          boolean highPriority,
                          FlushTracker tracker) {
        String chunkKey = tracker != null ? tracker.registerDispatch(data) : null;
        if (!tryAcquireSlot(highPriority)) {
            log.warn("网关限流丢弃本次上报: {} -> {}", data.getPointCode(), config.getTargetId());
            handleChunkResult(data, false, tracker, chunkKey, config, highPriority);
            return;
        }

        CompletableFuture<ReportResult> future = reportManager.reportAsync(data, config);
        future.whenComplete((result, throwable) -> {
            boolean success = throwable == null && result != null && result.isSuccess();
            if (throwable != null) {
                log.error("发送数据失败: {} -> {}", data.getPointCode(), config.getTargetId(), throwable);
            } else if (result != null && !result.isSuccess()) {
                log.warn("上报失败: {} -> {} , err={}", data.getPointCode(),
                        config.getTargetId(), result.getErrorMessage());
            }
            handleChunkResult(data, success, tracker, chunkKey, config, highPriority);
        });
    }

    private void handleChunkResult(ReportData data,
                                   boolean success,
                                   FlushTracker tracker,
                                   String chunkKey,
                                   ReportConfig config,
                                   boolean highPriority) {
        if (tracker == null) {
            return;
        }

        boolean scheduledRetry = false;
        if (success) {
            shadowManager.markReportedValues(data.getDeviceId(), data.getProperties(), data.getPropertyTs());
        } else if (chunkKey != null && tracker.shouldRetry(chunkKey)) {
            scheduledRetry = true;
            log.warn("分片重试: device={}, key={}, attempt={} / {}",
                    tracker.deviceId, chunkKey, tracker.getAttemptCount(chunkKey), reportProperties.getRetryTimes());
            dispatch(data, config, highPriority, tracker);
        } else {
            tracker.markFailure();
        }

        boolean allCompleted = tracker.markCompleted();
        if (!scheduledRetry && allCompleted) {
            flushTrackers.remove(tracker.deviceId);
            flushingDevices.remove(tracker.deviceId);
            if (!tracker.hasFailure()) {
                shadowManager.markReported(tracker.deviceId, tracker.windowStart, tracker.windowEnd);
            }
        }
    }

    private void dispatchEvent(String deviceId,
                               DataPoint point,
                               ProcessResult result,
                               EventInfo eventInfo) {
        ReportConfig reportConfig = resolveReportConfig(deviceId);
        if (reportConfig == null || !reportConfig.validate()) {
            log.warn("跳过事件上报，设备 {} 配置无效", deviceId);
            return;
        }
        ReportData eventData = new ReportData();
        eventData.setDeviceId(deviceId);
        eventData.setPointId(point.getPointId());
        eventData.setPointCode(Optional.ofNullable(point.getPointCode()).orElse(point.getPointId()));
        eventData.setPointName(point.getPointName());
        eventData.setTimestamp(System.currentTimeMillis());
        eventData.setMethod(MessageConstant.MESSAGE_TYPE_EVENT_POST);
        eventData.setValue(result.getFinalValue());
        eventData.setQuality(QualityEnum.fromCode(result.getQuality()).getText());
        eventData.addMetadata("eventType", eventInfo.eventType());
        if (eventInfo.level() != null) {
            eventData.addMetadata("eventLevel", eventInfo.level());
        }
        if (eventInfo.message() != null) {
            eventData.addMetadata("eventMessage", eventInfo.message());
        }
        if (eventInfo.ruleId() != null) {
            eventData.addMetadata("ruleId", eventInfo.ruleId());
        }
        if (eventInfo.ruleName() != null) {
            eventData.addMetadata("ruleName", eventInfo.ruleName());
        }
        eventData.addMetadata("pointAlias", point.getReportField());
        dispatch(eventData, reportConfig, true, null);
    }

    private boolean tryAcquireSlot(boolean highPriority) {
        if (highPriority) {
            return true;
        }
        int limit = reportProperties.getMaxGatewayMessagesPerSecond();
        if (limit <= 0) {
            return true;
        }
        long second = System.currentTimeMillis() / 1000;
        synchronized (rateLock) {
            if (second != rateWindowSecond) {
                rateWindowSecond = second;
                rateWindowCount = 0;
            }
            if (rateWindowCount >= limit) {
                return false;
            }
            rateWindowCount++;
            return true;
        }
    }

    @EventListener
    public void handleConfigUpdate(ConfigUpdateEvent event) {
        if (event.getDeviceId() != null) {
            deviceConfigCache.remove(event.getDeviceId());
        }
    }

    private boolean isMqttEnabled() {
        return reportProperties != null && reportProperties.mqttEnabled();
    }

    private ReportConfig resolveReportConfig(String deviceId) {
        return deviceConfigCache.compute(deviceId, (key, existing) -> {
            if (existing != null && existing.validate()) {
                return existing;
            }
            return buildReportConfig(deviceId);
        });
    }

    private ReportConfig buildReportConfig(String deviceId) {
        ReportProperties.Mqtt mqtt = reportProperties.getMqtt();
        BrokerEndpoint endpoint = parseBrokerEndpoint(mqtt.getBrokerUrl());
        if (endpoint == null) {
            log.error("MQTT broker 地址无效: {}", mqtt.getBrokerUrl());
            return null;
        }

        CollectionConfig collectionConfig = configManager.getCollectionConfig(deviceId);

        ReportConfig config = new ReportConfig();
        config.setProtocol(ProtocolConstant.PROTOCOL_MQTT);
        config.setHost(endpoint.host());
        config.setPort(endpoint.port());
        config.setTargetId(resolveTargetId(deviceId, collectionConfig));
        config.setMaxRetryCount(reportProperties.getRetryTimes());
        config.setRetryInterval((int) reportProperties.getIntervalMs());
        config.setConnectTimeout(reportProperties.getTimeout());
        config.setReadTimeout(reportProperties.getTimeout());

        Map<String, Object> params = new HashMap<>();
        params.put(ProtocolConstant.MQTT_PARAM_CLIENT_ID, resolveClientId(deviceId, mqtt.getClientId()));
        params.put(ProtocolConstant.MQTT_PARAM_USERNAME, mqtt.getUsername());
        params.put(ProtocolConstant.MQTT_PARAM_PASSWORD, mqtt.getPassword());
        params.put(ProtocolConstant.MQTT_PARAM_KEEP_ALIVE, mqtt.getKeepAliveInterval());
        params.put(ProtocolConstant.MQTT_PARAM_CLEAN_SESSION, mqtt.isCleanSession());
        params.put(ProtocolConstant.MQTT_PARAM_PUBLISH_TOPIC, resolveTopicTemplate(deviceId, collectionConfig, mqtt));
        params.put("qos", resolveQos(collectionConfig, mqtt));
        params.put("retained", resolveRetained(collectionConfig, mqtt));
        config.setParams(params);

        return config;
    }

    private String resolveClientId(String deviceId, String template) {
        if (template == null || template.isEmpty()) {
            return "collector-" + deviceId;
        }
        return template
                .replace("{deviceId}", deviceId)
                .replace("${deviceId}", deviceId);
    }

    private String resolveTargetId(String deviceId, CollectionConfig config) {
        if (config != null && config.getTargetId() != null && !config.getTargetId().isEmpty()) {
            return config.getTargetId();
        }
        return deviceId;
    }

    private String resolveTopicTemplate(String deviceId, CollectionConfig config, ReportProperties.Mqtt mqtt) {
        String template = mqtt.getTopicTemplate();
        Map<String, Object> reportParams = config != null ? config.getReportParams() : null;
        if (reportParams != null && reportParams.get("reportTopic") != null) {
            template = String.valueOf(reportParams.get("reportTopic"));
        } else if (template == null || template.isEmpty()) {
            template = mqtt.getTopics().getOrDefault("property", "data/{deviceId}/{pointCode}");
        }
        return template
                .replace("{deviceId}", deviceId)
                .replace("${deviceId}", deviceId);
    }

    private int resolveQos(CollectionConfig config, ReportProperties.Mqtt mqtt) {
        Map<String, Object> reportParams = config != null ? config.getReportParams() : null;
        if (reportParams != null && reportParams.get("qos") != null) {
            Object qos = reportParams.get("qos");
            if (qos instanceof Number number) {
                return number.intValue();
            }
            try {
                return Integer.parseInt(qos.toString());
            } catch (NumberFormatException ignore) {
                // fall through
            }
        }
        return mqtt.getQos();
    }

    private boolean resolveRetained(CollectionConfig config, ReportProperties.Mqtt mqtt) {
        Map<String, Object> reportParams = config != null ? config.getReportParams() : null;
        if (reportParams != null && reportParams.get("retain") != null) {
            Object retain = reportParams.get("retain");
            if (retain instanceof Boolean bool) {
                return bool;
            }
            return Boolean.parseBoolean(retain.toString());
        }
        return mqtt.isRetained();
    }

    private BrokerEndpoint parseBrokerEndpoint(String brokerUrl) {
        if (brokerUrl == null || brokerUrl.isEmpty()) {
            return null;
        }
        try {
            URI uri = URI.create(brokerUrl);
            String host = Optional.ofNullable(uri.getHost()).orElse(uri.getPath());
            int port = uri.getPort();
            if (port <= 0) {
                port = Objects.equals("ssl", uri.getScheme()) || Objects.equals("tls", uri.getScheme())
                        ? 8883 : ProtocolConstant.DEFAULT_MQTT_PORT;
            }
            return new BrokerEndpoint(host, port);
        } catch (Exception e) {
            log.error("解析 MQTT broker 地址失败: {}", brokerUrl, e);
            return null;
        }
    }

    private record BrokerEndpoint(String host, int port) {
    }

    private static class FlushTracker {
        private final String deviceId;
        private final AtomicInteger inFlight = new AtomicInteger(0);
        private final AtomicBoolean failure = new AtomicBoolean(false);
        private final long windowStart;
        private final long windowEnd;
        private final int maxRetries;
        private final ConcurrentMap<String, Integer> attempts = new ConcurrentHashMap<>();

        private FlushTracker(String deviceId, long windowStart, long windowEnd, int maxRetries) {
            this.deviceId = deviceId;
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
            this.maxRetries = Math.max(0, maxRetries);
        }

        String registerDispatch(ReportData data) {
            inFlight.incrementAndGet();
            String chunkKey = buildChunkKey(data);
            attempts.merge(chunkKey, 1, Integer::sum);
            return chunkKey;
        }

        private String buildChunkKey(ReportData data) {
            Object batchId = data.getMetadata().getOrDefault("batchId", data.getDeviceId());
            Object chunkIndex = data.getMetadata().getOrDefault("chunkIndex", data.getPointCode());
            Object seq = data.getMetadata().getOrDefault("seq", data.getTimestamp());
            return batchId + ":" + chunkIndex + ":" + seq;
        }

        boolean shouldRetry(String chunkKey) {
            if (chunkKey == null) {
                return false;
            }
            return attempts.getOrDefault(chunkKey, 0) <= maxRetries;
        }

        int getAttemptCount(String chunkKey) {
            return attempts.getOrDefault(chunkKey, 0);
        }

        void markFailure() {
            failure.set(true);
        }

        boolean hasFailure() {
            return failure.get();
        }

        boolean markCompleted() {
            return inFlight.decrementAndGet() == 0;
        }
    }
}
