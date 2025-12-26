package com.wangbin.collector.core.processor;

import com.wangbin.collector.common.domain.entity.AlarmRule;
import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.common.domain.enums.DataQuality;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 数据质量处理器
 * 描述：负责数据验证、告警检查和质量控制
 */
@Slf4j
@Component
public class DataQualityProcessor extends AbstractDataProcessor {

    public DataQualityProcessor() {
        this.name = "DataQualityProcessor";
        this.type = "QUALITY";
        this.description = "数据质量处理器";
        this.priority = 20;
    }

    @Override
    protected void doInit() throws Exception {
        log.info("数据质量处理器初始化完成: {}", getName());
    }

    @Override
    protected ProcessResult doProcess(ProcessContext context, DataPoint point, Object rawValue) throws Exception {
        if (rawValue == null) {
            return ProcessResult.error(rawValue, "数据值为空", DataQuality.BAD);
        }

        try {
            // 1. 数据范围验证
            if (!validateRange(point, rawValue)) {
                // 记录告警
                if (point.getAlarmEnabled() != null && point.getAlarmEnabled() == 1) {
                    triggerAlarm(point, rawValue, "数据超出有效范围");
                }
                return ProcessResult.error(rawValue, "数据超出有效范围", DataQuality.VALUE_INVALID);
            }

            // 2. 告警规则检查
            if (point.getAlarmEnabled() != null && point.getAlarmEnabled() == 1) {
                checkAlarmRules(point, rawValue);
            }

            return ProcessResult.success(rawValue, rawValue, "数据质量检查通过");

        } catch (Exception e) {
            log.error("数据质量检查异常: point={}, value={}", point.getPointName(), rawValue, e);
            return ProcessResult.error(rawValue, "数据质量检查异常: " + e.getMessage(), DataQuality.PROCESS_ERROR);
        }
    }

    /**
     * 验证数据范围
     */
    private boolean validateRange(DataPoint point, Object value) {
        if (point.getMinValue() == null && point.getMaxValue() == null) {
            return true;
        }

        try {
            double doubleValue = convertToDouble(value);
            if (Double.isNaN(doubleValue)) {
                return false;
            }

            if (point.getMinValue() != null && doubleValue < point.getMinValue()) {
                log.warn("点位值低于最小值: point={}, value={}, minValue={}", 
                        point.getPointName(), doubleValue, point.getMinValue());
                return false;
            }

            if (point.getMaxValue() != null && doubleValue > point.getMaxValue()) {
                log.warn("点位值高于最大值: point={}, value={}, maxValue={}", 
                        point.getPointName(), doubleValue, point.getMaxValue());
                return false;
            }

            return true;
        } catch (Exception e) {
            log.warn("数据范围验证异常: value={}", value, e);
            return false;
        }
    }

    /**
     * 检查告警规则
     */
    private void checkAlarmRules(DataPoint point, Object value) {
        if (point.getAlarmRule() == null || point.getAlarmRule().isEmpty()) {
            return;
        }

        double doubleValue = convertToDouble(value);
        if (Double.isNaN(doubleValue)) {
            return;
        }

        List<AlarmRule> rules = point.getAlarmRule();
        for (AlarmRule rule : rules) {
            if (rule.getEnabled() != null && !rule.getEnabled()) {
                continue;
            }

            if (rule.checkAlarm(doubleValue)) {
                triggerAlarm(point, value, rule.getDescription() != null ? rule.getDescription() : "触发告警");
            }
        }
    }

    /**
     * 触发告警
     */
    private void triggerAlarm(DataPoint point, Object value, String description) {
        // 这里只是记录日志，实际项目中应该调用告警服务
        log.warn("触发告警: deviceId={}, pointName={}, value={}, description={}",
                point.getDeviceId(), point.getPointName(), value, description);
    }

    /**
     * 转换为double
     */
    private double convertToDouble(Object value) {
        if (value == null) {
            return Double.NaN;
        }

        try {
            if (value instanceof Number) {
                return ((Number) value).doubleValue();
            } else if (value instanceof String) {
                return Double.parseDouble((String) value);
            } else if (value instanceof Boolean) {
                return (Boolean) value ? 1.0 : 0.0;
            }
        } catch (Exception e) {
            return Double.NaN;
        }

        return Double.NaN;
    }

    @Override
    protected void doDestroy() throws Exception {
        log.info("数据质量处理器销毁完成: {}", getName());
    }
}