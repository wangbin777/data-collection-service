
package com.wangbin.collector.core.report.service;

import com.wangbin.collector.common.constant.MessageConstant;
import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.common.enums.QualityEnum;
import com.wangbin.collector.core.config.model.ConfigUpdateEvent;
import com.wangbin.collector.core.processor.ProcessResult;
import com.wangbin.collector.core.report.config.ReportProperties;
import com.wangbin.collector.core.report.model.ReportConfig;
import com.wangbin.collector.core.report.model.ReportData;
import com.wangbin.collector.core.report.model.ReportIdentity;
import com.wangbin.collector.core.report.model.ReportResult;
import com.wangbin.collector.core.report.service.support.GatewayRateLimiter;
import com.wangbin.collector.core.report.service.support.ReportConfigProvider;
import com.wangbin.collector.core.report.service.support.ReportIdentityResolver;
import com.wangbin.collector.core.report.shadow.DeviceShadow;
import com.wangbin.collector.core.report.shadow.ShadowManager;
import com.wangbin.collector.core.report.shadow.ShadowManager.EventInfo;
import com.wangbin.collector.core.report.shadow.ShadowManager.ShadowUpdateResult;
import com.wangbin.collector.core.report.shadow.ValueMeta;
import com.wangbin.collector.monitor.alert.AlertNotification;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ScheduledFuture;
import java.time.Duration;
import java.time.Instant;

/**
 * 缓存上报服务：聚合快照/变化/事件并统一推送
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CacheReportService {

    private static final String SNAPSHOT_POINT_CODE = "snapshot";

    private final ReportManager reportManager;
    private final ReportProperties reportProperties;
    private final ShadowManager shadowManager;
    private final ReportIdentityResolver identityResolver;
    private final ReportConfigProvider reportConfigProvider;
    private final GatewayRateLimiter gatewayRateLimiter;
    @Qualifier("taskScheduler")
    private final TaskScheduler taskScheduler;

    private final ConcurrentMap<String, String> identityGatewayMapping = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, String> identityProductKeys = new ConcurrentHashMap<>();
    private final Set<String> flushingDevices = ConcurrentHashMap.newKeySet();
    private final ConcurrentMap<String, FlushTracker> flushTrackers = new ConcurrentHashMap<>();
    private ScheduledFuture<?> flushTask;

    @PostConstruct
    public void start() {
        if (!isMqttEnabled()) {
            return;
        }
        long interval = Math.max(1000L, reportProperties.getIntervalMs());
        flushTask = taskScheduler.scheduleAtFixedRate(this::flushDirtyDevices, Duration.ofMillis(interval));
    }

    @PreDestroy
    public void shutdown() {
        if (flushTask != null) {
            flushTask.cancel(true);
        }
    }

    public void reportPoint(String deviceId, String method, DataPoint point, Object cacheValue) {
        if (!isMqttEnabled() || deviceId == null || point == null || cacheValue == null) {
            return;
        }
        ProcessResult processResult = identityResolver.toProcessResult(cacheValue);
        if (processResult == null) {
            return;
        }
        List<ReportIdentity> identities = identityResolver.resolve(deviceId, point, defaultProductKey());
        if (identities.isEmpty()) {
            identities = List.of(new ReportIdentity(deviceId, deviceId, defaultProductKey()));
        }
        for (ReportIdentity identity : identities) {
            identityGatewayMapping.put(identity.cloudDeviceId(), identity.gatewayDeviceId());
            identityProductKeys.put(identity.cloudDeviceId(), identity.productKey());
            ShadowUpdateResult updateResult = shadowManager.apply(identity.cloudDeviceId(), point, processResult);
            if (updateResult.changeTriggered()) {
                triggerImmediateFlush(identity.cloudDeviceId());
            }
            EventInfo eventInfo = updateResult.eventInfo();
            if (eventInfo != null) {
                dispatchEvent(identity, point, processResult, eventInfo);
            }
        }
    }


    private String defaultProductKey() {
        ReportProperties.Mqtt mqtt = reportProperties.getMqtt();
        if (mqtt == null || mqtt.getProductKey() == null) {
            return "";
        }
        return mqtt.getProductKey();
    }

    private void triggerImmediateFlush(String deviceId) {
        DeviceShadow shadow = shadowManager.getShadow(deviceId);
        if (shadow == null) {
            return;
        }
        long now = System.currentTimeMillis();
        if (now - shadow.getLastReportAt() < reportProperties.getMinReportIntervalMs()) {
            return;
        }
        taskScheduler.schedule(() -> flushDevice(deviceId), Instant.now());
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
        String gatewayDeviceId = identityGatewayMapping.get(deviceId);
        DeviceShadow shadow = shadowManager.getShadow(deviceId);
        if (gatewayDeviceId == null) {
            log.warn("找不到云端设备 {} 对应的网关映射，跳过刷新", deviceId);
            shadowManager.clearDirty(deviceId);
            flushingDevices.remove(deviceId);
            return;
        }
        if (shadow == null || shadow.isEmpty()) {
            shadowManager.clearDirty(deviceId);
            flushingDevices.remove(deviceId);
            return;
        }

        ReportData snapshot = buildSnapshot(shadow, gatewayDeviceId);
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

    private ReportData buildSnapshot(DeviceShadow shadow, String rawDeviceId) {
        ReportData data = new ReportData();
        data.setDeviceId(shadow.getDeviceId());
        data.setTimestamp(System.currentTimeMillis());
        data.addMetadata("schemaVersion", reportProperties.getSchemaVersion());
        data.addMetadata("seq", shadow.nextSeq());
        if (rawDeviceId != null) {
            data.addMetadata("rawDeviceId", rawDeviceId);
        }
        String productKey = identityProductKeys.get(shadow.getDeviceId());
        if (productKey == null || productKey.isEmpty()) {
            productKey = defaultProductKey();
        }
        if (productKey != null && !productKey.isEmpty()) {
            data.addMetadata("productKey", productKey);
        }
        Map<String, ValueMeta> latest = shadow.snapshot();
        DeviceShadow.PointInfo primaryPoint = resolvePrimaryPointInfo(shadow, latest);
        if (primaryPoint != null) {
            ReportData.applyPointInfo(data, primaryPoint.pointId(), primaryPoint.pointCode(), primaryPoint.pointName());
        } else {
            ReportData.applyPointInfo(data, SNAPSHOT_POINT_CODE, SNAPSHOT_POINT_CODE, "snapshot");
        }
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
                String base = snapshot.getPointCode() != null ? snapshot.getPointCode() : SNAPSHOT_POINT_CODE;
                chunk.setPointCode(base + "-" + i);
            }
            chunk.applyChunkMetadata(batchId, i, result.size());
        }
        return result;
    }

    private DeviceShadow.PointInfo resolvePrimaryPointInfo(DeviceShadow shadow, Map<String, ValueMeta> latest) {
        if (shadow == null) {
            return null;
        }
        if (latest != null) {
            for (String field : latest.keySet()) {
                DeviceShadow.PointInfo info = shadow.getPointInfo(field);
                if (info != null) {
                    return info;
                }
            }
        }
        return shadow.snapshotPointInfos().values().stream().findFirst().orElse(null);
    }

    private void dispatch(ReportData data,
                          ReportConfig config,
                          boolean highPriority,
                          FlushTracker tracker) {
        String chunkKey = tracker != null ? tracker.registerDispatch(data) : null;
        if (!gatewayRateLimiter.tryAcquire(highPriority)) {
            log.warn("Gateway rate limit dropped current report: {} -> {}", data.getPointCode(), config.getTargetId());
            handleChunkResult(data, null, null, tracker, chunkKey, config, highPriority);
            return;
        }

        CompletableFuture<ReportResult> future = reportManager.reportAsync(data, config);
        future.whenComplete((result, throwable) ->
                handleChunkResult(data, result, throwable, tracker, chunkKey, config, highPriority));
    }



    private void handleChunkResult(ReportData data,
                                   ReportResult result,
                                   Throwable throwable,
                                   FlushTracker tracker,
                                   String chunkKey,
                                   ReportConfig config,
                                   boolean highPriority) {
        if (tracker == null) {
            return;
        }

        boolean success = throwable == null && result != null && result.isSuccess();
        boolean deferred = isDeferredResult(result);
        boolean scheduledRetry = false;

        if (throwable != null) {
            log.error("Send telemetry failed: {} -> {}", data.getPointCode(), config.getTargetId(), throwable);
        } else if (result != null && !result.isSuccess() && !deferred) {
            log.warn("Report rejected: {} -> {} , err={}", data.getPointCode(),
                    config.getTargetId(), result.getErrorMessage());
        }

        if (deferred) {
            scheduledRetry = true;
            scheduleDeferredRetry(data, config, highPriority, tracker);
        } else if (success) {
            shadowManager.markReportedValues(data.getDeviceId(), data.getProperties(), data.getPropertyTs());
        } else if (chunkKey != null && tracker.shouldRetry(chunkKey)) {
            scheduledRetry = true;
            log.warn("Chunk retry: device={}, key={}, attempt={} / {}",
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

    private boolean isDeferredResult(ReportResult result) {
        if (result == null || result.getMetadata() == null) {
            return false;
        }
        Object deferred = result.getMetadata().get("deferred");
        if (deferred instanceof Boolean bool) {
            return bool;
        }
        if (deferred instanceof String text) {
            return Boolean.parseBoolean(text);
        }
        return false;
    }

    private void scheduleDeferredRetry(ReportData data,
                                       ReportConfig config,
                                       boolean highPriority,
                                       FlushTracker tracker) {
        long delayMillis = Math.max(2000L, reportProperties.getIntervalMs());
        taskScheduler.schedule(() -> dispatch(data, config, highPriority, tracker),
                Instant.now().plusMillis(delayMillis));
        log.debug("Deferred retry scheduled device={} point={} delay={}ms",
                data.getDeviceId(), data.getPointCode(), delayMillis);
    }

    private void dispatchEvent(ReportIdentity identity,
                               DataPoint point,
                               ProcessResult result,
                               EventInfo eventInfo) {
        String deviceId = identity.cloudDeviceId();
        ReportConfig reportConfig = resolveReportConfig(deviceId);
        if (reportConfig == null || !reportConfig.validate()) {
            log.warn("跳过事件上报，设备 {} 配置无效", deviceId);
            return;
        }
        ReportData eventData = new ReportData();
        eventData.setDeviceId(deviceId);
        ReportData.applyPointInfo(eventData, point);
        eventData.setTimestamp(System.currentTimeMillis());
        eventData.setMethod(MessageConstant.MESSAGE_TYPE_EVENT_POST);
        eventData.setValue(result.getFinalValue());
        eventData.setQuality(QualityEnum.fromCode(result.getQuality()).getText());
        eventData.addMetadata("rawDeviceId", identity.gatewayDeviceId());
        if (identity.productKey() != null && !identity.productKey().isEmpty()) {
            eventData.addMetadata("productKey", identity.productKey());
        }
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
        if (point.getUnit() != null) {
            eventData.addMetadata("unit", point.getUnit());
        }
        if (point.getDeviceName() != null) {
            eventData.addMetadata("deviceName", point.getDeviceName());
        }
        dispatch(eventData, reportConfig, true, null);
    }

    public void reportAlert(AlertNotification notification) {
        if (!isMqttEnabled() || notification == null) {
            return;
        }
        String deviceId = notification.getDeviceId();
        if (deviceId == null || deviceId.isEmpty()) {
            log.warn("Skip alert upload, deviceId missing");
            return;
        }
        ReportConfig reportConfig = resolveReportConfig(deviceId);
        if (reportConfig == null) {
            log.warn("Skip alert upload, report config missing for {}", deviceId);
            return;
        }

        ReportData alertData = new ReportData();
        alertData.setDeviceId(deviceId);
        alertData.setPointId(notification.getPointId());
        String pointCode = Optional.ofNullable(notification.getPointCode())
                .orElse(Optional.ofNullable(notification.getPointId()).orElse("alarm"));
        alertData.setPointCode(pointCode);
        alertData.setPointName(notification.getPointCode());
        alertData.setMethod(MessageConstant.MESSAGE_TYPE_EVENT_POST);
        long timestamp = notification.getTimestamp() > 0
                ? notification.getTimestamp()
                : System.currentTimeMillis();
        alertData.setTimestamp(timestamp);
        alertData.setValue(notification.getValue());
        alertData.setQuality(QualityEnum.WARNING.getText());
        alertData.addMetadata("eventType",
                Optional.ofNullable(notification.getEventType()).orElse("ALARM"));
        alertData.addMetadata("eventLevel",
                Optional.ofNullable(notification.getLevel()).orElse("WARNING"));
        alertData.addMetadata("eventMessage", notification.getMessage());
        if (notification.getRuleId() != null) {
            alertData.addMetadata("ruleId", notification.getRuleId());
        }
        if (notification.getRuleName() != null) {
            alertData.addMetadata("ruleName", notification.getRuleName());
        }
        if (notification.getDeviceName() != null) {
            alertData.addMetadata("deviceName", notification.getDeviceName());
        }
        if (notification.getUnit() != null) {
            alertData.addMetadata("unit", notification.getUnit());
        }
        alertData.addMetadata("rawDeviceId", notification.getDeviceId());
        alertData.addProperty(pointCode, notification.getValue(),
                timestamp, QualityEnum.WARNING.getText());
        dispatch(alertData, reportConfig, true, null);
    }

    @EventListener
    public void handleConfigUpdate(ConfigUpdateEvent event) {
        if (event.getDeviceId() != null) {
            evictGateway(event.getDeviceId());
        }
    }

    private boolean isMqttEnabled() {
        return reportProperties != null && reportProperties.mqttEnabled();
    }

    private void evictGateway(String gatewayDeviceId) {
        if (gatewayDeviceId == null) {
            return;
        }
        reportConfigProvider.evict(gatewayDeviceId);
        List<String> orphanIdentities = new ArrayList<>();
        identityGatewayMapping.forEach((identity, gateway) -> {
            if (gatewayDeviceId.equals(gateway)) {
                orphanIdentities.add(identity);
            }
        });
        for (String identity : orphanIdentities) {
            identityGatewayMapping.remove(identity);
            identityProductKeys.remove(identity);
            shadowManager.removeShadow(identity);
        }
    }

    private ReportConfig resolveReportConfig(String deviceId) {
        String gatewayDeviceId = identityGatewayMapping.getOrDefault(deviceId, deviceId);
        ReportConfig config = reportConfigProvider.getConfig(gatewayDeviceId);
        if (config != null && config.validate()) {
            return config;
        }
        return null;
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
