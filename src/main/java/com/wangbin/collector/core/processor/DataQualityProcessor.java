package com.wangbin.collector.core.processor;

import com.wangbin.collector.common.domain.entity.AlarmRule;
import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.common.domain.enums.DataQuality;
import com.wangbin.collector.common.enums.QualityEnum;
import com.wangbin.collector.monitor.alert.AlertManager;
import com.wangbin.collector.monitor.alert.AlertNotification;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Validates incoming telemetry and enriches failed samples with alarm metadata so they can be reported.
 */
@Slf4j
@Component
public class DataQualityProcessor extends AbstractDataProcessor {

    private static final String QUALITY_EVENT_TYPE = "QUALITY";
    private final AlertManager alertManager;

    public DataQualityProcessor(AlertManager alertManager) {
        this.alertManager = alertManager;
        this.name = "DataQualityProcessor";
        this.type = "QUALITY";
        this.description = "Data quality processor";
        this.priority = 20;
    }

    @Override
    protected void doInit() throws Exception {
        log.info("DataQualityProcessor initialized: {}", getName());
    }

    @Override
    protected ProcessResult doProcess(ProcessContext context, DataPoint point, Object rawValue) throws Exception {
        if (rawValue == null) {
            return buildQualityError(point, null, "value is null", DataQuality.BAD);
        }

        try {
            String rangeIssue = validateRange(point, rawValue);
            if (rangeIssue != null) {
                return buildQualityError(point, rawValue, rangeIssue, DataQuality.VALUE_INVALID);
            }

            AlarmEvent alarmEvent = null;
            if (point.getAlarmEnabled() != null && point.getAlarmEnabled() == 1) {
                alarmEvent = checkAlarmRules(point, rawValue);
            }

            ProcessResult result = ProcessResult.success(rawValue, rawValue, "quality check passed");
            if (alarmEvent != null) {
                result.setQuality(QualityEnum.WARNING.getCode());
                result.setQualityDescription(QualityEnum.WARNING.getText());
                applyAlarmMetadata(result, point, alarmEvent, context, rawValue);
            }
            return result;

        } catch (Exception e) {
            log.error("Quality check error: point={}, value={}", point.getPointName(), rawValue, e);
            ProcessResult error = ProcessResult.error(rawValue,
                    "quality check error: " + e.getMessage(), DataQuality.PROCESS_ERROR);
            applyAlarmMetadata(error, point,
                    new AlarmEvent(QUALITY_EVENT_TYPE, "ERROR", "quality check error", null, null), context, rawValue);
            return error;
        }
    }

    private ProcessResult buildQualityError(DataPoint point, Object rawValue, String message, DataQuality quality) {
        ProcessResult result = ProcessResult.error(rawValue, message, quality);
        applyAlarmMetadata(result, point,
                new AlarmEvent(QUALITY_EVENT_TYPE, quality.getDescription(), message, null, null), null, rawValue);
        return result;
    }

    private String validateRange(DataPoint point, Object value) {
        if (point == null || (point.getMinValue() == null && point.getMaxValue() == null)) {
            return null;
        }

        double doubleValue = convertToDouble(value);
        if (Double.isNaN(doubleValue)) {
            return "value cannot be parsed";
        }

        if (point.getMinValue() != null && doubleValue < point.getMinValue()) {
            log.warn("Value below min: point={}, value={}, min={}",
                    point.getPointName(), doubleValue, point.getMinValue());
            return "value below configured minimum";
        }

        if (point.getMaxValue() != null && doubleValue > point.getMaxValue()) {
            log.warn("Value above max: point={}, value={}, max={}",
                    point.getPointName(), doubleValue, point.getMaxValue());
            return "value above configured maximum";
        }

        return null;
    }

    private AlarmEvent checkAlarmRules(DataPoint point, Object value) {
        List<AlarmRule> rules = point.getAlarmRule();
        if (rules == null || rules.isEmpty()) {
            return null;
        }

        double doubleValue = convertToDouble(value);
        if (Double.isNaN(doubleValue)) {
            return null;
        }

        for (AlarmRule rule : rules) {
            if (rule == null || (rule.getEnabled() != null && !rule.getEnabled())) {
                continue;
            }

            if (rule.checkAlarm(doubleValue)) {
                String level = rule.getLevel() != null ? rule.getLevel() : "WARNING";
                String message = rule.getDescription() != null ? rule.getDescription() : "alarm triggered";
                log.warn("Alarm triggered: deviceId={}, point={}, rule={}, value={}",
                        point.getDeviceId(), point.getPointName(), rule.getRuleName(), doubleValue);
                return new AlarmEvent("ALARM", level, message, rule.getRuleId(), rule.getRuleName());
            }
        }
        return null;
    }

    private void applyAlarmMetadata(ProcessResult result,
                                    DataPoint point,
                                    AlarmEvent event,
                                    ProcessContext context,
                                    Object rawValue) {
        if (result == null || event == null) {
            return;
        }
        result.addMetadata("eventTriggered", true);
        result.addMetadata("eventType", event.type);
        result.addMetadata("eventLevel", event.level);
        result.addMetadata("eventMessage", event.message);
        if (event.ruleId != null) {
            result.addMetadata("ruleId", event.ruleId);
        }
        if (event.ruleName != null) {
            result.addMetadata("ruleName", event.ruleName);
        }
        if (point != null) {
            if (point.getPointCode() != null) {
                result.addMetadata("pointCode", point.getPointCode());
            }
            if (point.getDeviceId() != null) {
                result.addMetadata("rawDeviceId", point.getDeviceId());
            }
        }
        notifyAlert(point, event, context, result, rawValue);
    }

    private void notifyAlert(DataPoint point,
                             AlarmEvent event,
                             ProcessContext context,
                             ProcessResult result,
                             Object rawValue) {
        if (alertManager == null || event == null) {
            return;
        }
        String contextDeviceId = null;
        if (context != null) {
            contextDeviceId = context.getAttribute("deviceId", null);
            if (contextDeviceId == null && context.getDeviceInfo() != null) {
                contextDeviceId = context.getDeviceInfo().getDeviceId();
            }
        }
        AlertNotification notification =
                AlertNotification.builder()
                        .deviceId(point != null && point.getDeviceId() != null
                                ? point.getDeviceId()
                                : contextDeviceId)
                        .deviceName(point != null ? point.getDeviceName() : null)
                        .pointId(point != null ? point.getPointId() : null)
                        .pointCode(point != null ? point.getPointCode() : null)
                        .ruleId(event.ruleId)
                        .ruleName(event.ruleName)
                        .level(event.level)
                        .eventType(event.type)
                        .message(event.message)
                        .value(result != null ? result.getFinalValue(rawValue) : rawValue)
                        .unit(point != null ? point.getUnit() : null)
                        .timestamp(context != null ? context.getProcessTime() : System.currentTimeMillis())
                        .build();
        alertManager.notifyAlert(notification);
    }

    private double convertToDouble(Object value) {
        if (value == null) {
            return Double.NaN;
        }

        try {
            if (value instanceof Number) {
                return ((Number) value).doubleValue();
            }
            if (value instanceof String) {
                return Double.parseDouble((String) value);
            }
            if (value instanceof Boolean) {
                return (Boolean) value ? 1.0 : 0.0;
            }
        } catch (Exception ignored) {
            return Double.NaN;
        }
        return Double.NaN;
    }

    @Override
    protected void doDestroy() throws Exception {
        log.info("DataQualityProcessor destroyed: {}", getName());
    }

    private static final class AlarmEvent {
        private final String type;
        private final String level;
        private final String message;
        private final String ruleId;
        private final String ruleName;

        private AlarmEvent(String type, String level, String message, String ruleId, String ruleName) {
            this.type = type;
            this.level = level;
            this.message = message;
            this.ruleId = ruleId;
            this.ruleName = ruleName;
        }
    }
}
