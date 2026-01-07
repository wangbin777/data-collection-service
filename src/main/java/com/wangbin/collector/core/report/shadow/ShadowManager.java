package com.wangbin.collector.core.report.shadow;

import com.wangbin.collector.common.domain.entity.AlarmRule;
import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.common.enums.QualityEnum;
import com.wangbin.collector.core.processor.ProcessResult;
import com.wangbin.collector.core.report.config.ReportProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 设备影子管理器：负责缓存设备最新属性、判断变化/事件触发，并维护需要刷新的设备列表。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ShadowManager {

    private final ReportProperties reportProperties;
    private final Map<String, DeviceShadow> shadows = new ConcurrentHashMap<>();
    private final Set<String> dirtyDevices = ConcurrentHashMap.newKeySet();

    public ShadowUpdateResult apply(String deviceId, DataPoint point, ProcessResult result) {
        if (deviceId == null || point == null || result == null) {
            return ShadowUpdateResult.EMPTY;
        }

        DeviceShadow shadow = shadows.computeIfAbsent(deviceId, DeviceShadow::new);
        boolean changeTriggered = false;
        EventInfo eventInfo = null;
        String field = point.getReportField();


        if (field != null && point.isReportEnabled()) {
            QualityEnum qualityEnum = QualityEnum.fromCode(result.getQuality());
            ValueMeta meta = new ValueMeta(result.getFinalValue(), System.currentTimeMillis(), qualityEnum.getText());
            shadow.update(field, meta);
            dirtyDevices.add(deviceId);
            changeTriggered = shouldTriggerChange(shadow, point, result, field);
        }

        if (point.isEventReportingEnabled()) {
            String eventFieldKey = field != null
                    ? field
                    : Optional.ofNullable(point.getPointCode()).orElse(point.getPointId());
            eventInfo = evaluateEvent(shadow, point, result, eventFieldKey);
        }

        return new ShadowUpdateResult(changeTriggered, eventInfo);
    }

    private boolean shouldTriggerChange(DeviceShadow shadow,
                                        DataPoint point,
                                        ProcessResult result,
                                        String field) {
        if (field == null || !point.isChangeTriggerEnabled()) {
            return false;
        }

        Double threshold = point.getChangeThreshold();
        if (threshold == null || threshold <= 0) {
            return false;
        }

        Double lastReported = toDouble(shadow.getLastReportedValue(field));
        Double current = toDouble(result.getFinalValue());
        if (lastReported == null || current == null) {
            return false;
        }
        if (Math.abs(current - lastReported) < threshold) {
            return false;
        }

        long now = System.currentTimeMillis();
        long minInterval = point.getChangeMinIntervalMs(reportProperties.getMinReportIntervalMs());
        String changeKey = field + ":change";
        if (now - shadow.getLastChangeTriggerAt(changeKey) < minInterval) {
            return false;
        }
        shadow.markChangeTrigger(changeKey, now);
        return true;
    }

    private EventInfo evaluateEvent(DeviceShadow shadow,
                                    DataPoint point,
                                    ProcessResult result,
                                    String fieldKey) {
        long now = System.currentTimeMillis();
        long minInterval = point.getEventMinIntervalMs(reportProperties.getEventMinIntervalMs());

        EventInfo info = null;
        if (Strings.isNotBlank(fieldKey)) {
            info = matchAlarmRule(point, point.getAlarmRule(), result);
        }
        if (info == null) {
            info = evaluateProcessResultEvent(result);
        }
        if (info == null) {
            return null;
        }

        String eventType = info.eventType() != null ? info.eventType() : "EVENT";
        String eventKey = (fieldKey != null ? fieldKey : point.getPointId()) + "|" + eventType;
        if (now - shadow.getLastEventTriggerAt(eventKey) < minInterval) {
            return null;
        }

        String signatureBase = eventType + ":" + Objects.hash(info.message(), info.ruleId(), info.ruleName());
        if (now - shadow.getLastEventSignatureAt(signatureBase) < minInterval) {
            return null;
        }

        shadow.markEventTrigger(eventKey, now);
        shadow.markEventSignature(signatureBase, now);
        return info;
    }

    private EventInfo matchAlarmRule(DataPoint point, List<AlarmRule> rules, ProcessResult result) {
        if (point == null || point.getAlarmEnabled() == null || point.getAlarmEnabled() != 1) {
            return null;
        }
        if (CollectionUtils.isEmpty(rules)) {
            return null;
        }
        Double value = toDouble(result.getFinalValue());
        if (value == null) {
            return null;
        }
        for (AlarmRule rule : rules) {
            if (rule == null || Boolean.FALSE.equals(rule.getEnabled())) {
                continue;
            }
            if (rule.checkAlarm(value)) {
                return new EventInfo(
                        rule.getRuleId(),
                        rule.getRuleName(),
                        rule.getLevel(),
                        rule.getDescription() != null ? rule.getDescription() : "alarm triggered",
                        "ALARM"
                );
            }
        }
        return null;
    }

    private EventInfo evaluateProcessResultEvent(ProcessResult result) {
        Map<String, Object> metadata = result.getMetadata();
        if (metadata != null) {
            Object eventFlag = metadata.get("eventTriggered");
            if (eventFlag instanceof Boolean bool && bool) {
                return new EventInfo(
                        metadataToString(metadata, "ruleId"),
                        metadataToString(metadata, "ruleName"),
                        String.valueOf(metadata.getOrDefault("eventLevel", "WARNING")),
                        String.valueOf(metadata.getOrDefault("eventMessage", result.getMessage())),
                        String.valueOf(metadata.getOrDefault("eventType", "EVENT")));
            }
            if (metadata.get("eventType") != null) {
                return new EventInfo(
                        metadataToString(metadata, "ruleId"),
                        metadataToString(metadata, "ruleName"),
                        String.valueOf(metadata.getOrDefault("eventLevel", "INFO")),
                        String.valueOf(metadata.getOrDefault("eventMessage", result.getMessage())),
                        String.valueOf(metadata.get("eventType")));
            }
        }

        if (!result.isSuccess() || result.getQuality() < QualityEnum.WARNING.getCode()) {
            return new EventInfo(null, null, "WARNING",
                    result.getMessage() != null ? result.getMessage() : "quality degraded",
                    "QUALITY");
        }
        return null;
    }

    private String metadataToString(Map<String, Object> metadata, String key) {
        if (metadata == null) {
            return null;
        }
        Object value = metadata.get(key);
        return value != null ? value.toString() : null;
    }

    private Double toDouble(Object value) {
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        if (value instanceof String text) {
            try {
                return Double.parseDouble(text);
            } catch (NumberFormatException ignore) {
                return null;
            }
        }
        return null;
    }

    public DeviceShadow getShadow(String deviceId) {
        return deviceId == null ? null : shadows.get(deviceId);
    }

    public Set<String> getDirtyDevices() {
        return Collections.unmodifiableSet(dirtyDevices);
    }

    public void clearDirty(String deviceId) {
        if (deviceId != null) {
            dirtyDevices.remove(deviceId);
        }
    }

    public void markReported(String deviceId, long windowStart, long windowEnd) {
        DeviceShadow shadow = getShadow(deviceId);
        if (shadow != null) {
            shadow.setLastReportAt(System.currentTimeMillis());
            shadow.setLastWindow(windowStart, windowEnd);
        }
        clearDirty(deviceId);
    }

    public void markReportedValues(String deviceId,
                                   Map<String, Object> properties,
                                   Map<String, Long> propertyTs) {
        DeviceShadow shadow = getShadow(deviceId);
        if (shadow != null) {
            shadow.markReportedValues(properties, propertyTs);
        }
    }

    public void removeShadow(String deviceId) {
        if (deviceId == null) {
            return;
        }
        shadows.remove(deviceId);
        dirtyDevices.remove(deviceId);
    }

    public record ShadowUpdateResult(boolean changeTriggered, EventInfo eventInfo) {
        public static final ShadowUpdateResult EMPTY = new ShadowUpdateResult(false, null);
    }

    public record EventInfo(String ruleId,
                            String ruleName,
                            String level,
                            String message,
                            String eventType) {
    }
}
