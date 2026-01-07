package com.wangbin.collector.core.processor.converter;

import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.core.processor.AbstractDataProcessor;
import com.wangbin.collector.core.processor.ProcessContext;
import com.wangbin.collector.core.processor.ProcessResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * 单位转换器
 */
@Slf4j
@Component
public class UnitConverter extends AbstractDataProcessor {

    private String contextUnitAttribute = "rawUnit";
    private String additionalConfigUnitKey = "sourceUnit";

    public UnitConverter() {
        this.name = "UnitConverter";
        this.type = "CONVERTER";
        this.description = "单位转换器";
        this.priority = 30;
    }

    @Override
    protected void doInit() throws Exception {
        log.info("单位转换器初始化完成: {}", getName());
    }

    @Override
    protected ProcessResult doProcess(ProcessContext context, DataPoint point, Object rawValue) throws Exception {
        try {
            // 检查是否需要单位转换
            String sourceUnit = getSourceUnit(context, point);
            String targetUnit = point.getUnit();

            if (sourceUnit == null || targetUnit == null ||
                    sourceUnit.equals(targetUnit)) {
                return ProcessResult.success(rawValue, rawValue, "单位一致，无需转换");
            }

            // 执行单位转换
            Object convertedValue = convertUnit(rawValue, sourceUnit, targetUnit);

            return ProcessResult.success(rawValue, convertedValue,
                    String.format("单位转换: %s -> %s", sourceUnit, targetUnit));

        } catch (Exception e) {
            log.error("单位转换异常: point={}, value={}", point.getPointName(), rawValue, e);
            return ProcessResult.error(rawValue, "单位转换异常: " + e.getMessage());
        }
    }

    /**
     * 获取源单位
     */
    private String getSourceUnit(ProcessContext context, DataPoint point) {
        Object ctxUnit = context != null ? context.getAttribute(contextUnitAttribute) : null;
        if (ctxUnit instanceof String ctx && !ctx.isEmpty()) {
            return ctx;
        }

        if (point != null && point.getAdditionalConfig() != null) {
            Object extra = point.getAdditionalConfig().get(additionalConfigUnitKey);
            if (extra instanceof String val && !val.isEmpty()) {
                return val;
            }
        }

        return point != null ? point.getUnit() : null;
    }

    /**
     * 执行单位转换
     */
    private Object convertUnit(Object value, String sourceUnit, String targetUnit) {
        if (!(value instanceof Number)) {
            return value;
        }

        double doubleValue = ((Number) value).doubleValue();
        double convertedValue = doubleValue;

        // 实现常见的单位转换
        if ("°C".equals(sourceUnit) && "°F".equals(targetUnit)) {
            // 摄氏度转华氏度
            convertedValue = doubleValue * 9 / 5 + 32;
        } else if ("°F".equals(sourceUnit) && "°C".equals(targetUnit)) {
            // 华氏度转摄氏度
            convertedValue = (doubleValue - 32) * 5 / 9;
        } else if ("kPa".equals(sourceUnit) && "bar".equals(targetUnit)) {
            // 千帕转巴
            convertedValue = doubleValue / 100;
        } else if ("bar".equals(sourceUnit) && "kPa".equals(targetUnit)) {
            // 巴转千帕
            convertedValue = doubleValue * 100;
        } else if ("m³/h".equals(sourceUnit) && "L/min".equals(targetUnit)) {
            // 立方米/小时转升/分钟
            convertedValue = doubleValue * 1000 / 60;
        }
        // 可以添加更多单位转换...

        // 保持原始数据类型
        if (value instanceof Integer) {
            return (int) convertedValue;
        } else if (value instanceof Long) {
            return (long) convertedValue;
        } else if (value instanceof Float) {
            return (float) convertedValue;
        } else {
            return convertedValue;
        }
    }

    @Override
    protected void doDestroy() throws Exception {
        log.info("单位转换器销毁完成: {}", getName());
    }

    @Override
    protected void loadConfig(Map<String, Object> config) {
        super.loadConfig(config);
        contextUnitAttribute = getStringConfig("contextUnitAttribute", contextUnitAttribute);
        additionalConfigUnitKey = getStringConfig("additionalConfigUnitKey", additionalConfigUnitKey);
    }
}
