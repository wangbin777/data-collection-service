package com.wangbin.collector.core.processor.validator;

import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.core.processor.AbstractDataProcessor;
import com.wangbin.collector.core.processor.ProcessContext;
import com.wangbin.collector.core.processor.ProcessResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * 数据验证器
 */
@Slf4j
@Component
public class DataValidator extends AbstractDataProcessor {

    public DataValidator() {
        this.name = "DataValidator";
        this.type = "VALIDATOR";
        this.description = "数据验证处理器";
        this.priority = 10;
    }

    @Override
    protected void doInit() throws Exception {
        log.info("数据验证器初始化完成: {}", getName());
    }

    @Override
    protected ProcessResult doProcess(ProcessContext context, DataPoint point, Object rawValue) throws Exception {
        if (rawValue == null) {
            return ProcessResult.error(rawValue, "数据值为空");
        }

        try {
            // 1. 数据范围验证
            if (!validateRange(point, rawValue)) {
                return ProcessResult.error(rawValue, "数据超出有效范围");
            }

            // 2. 数据类型验证
            if (!validateDataType(point, rawValue)) {
                return ProcessResult.error(rawValue, "数据类型不匹配");
            }

            // 3. 数据格式验证
            if (!validateFormat(point, rawValue)) {
                return ProcessResult.error(rawValue, "数据格式错误");
            }

            return ProcessResult.success(rawValue, rawValue, "数据验证通过");

        } catch (Exception e) {
            log.error("数据验证异常: point={}, value={}", point.getPointName(), rawValue, e);
            return ProcessResult.error(rawValue, "数据验证异常: " + e.getMessage());
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
            if (doubleValue == Double.NaN) {
                return false;
            }

            if (point.getMinValue() != null && doubleValue < point.getMinValue()) {
                return false;
            }

            if (point.getMaxValue() != null && doubleValue > point.getMaxValue()) {
                return false;
            }

            return true;
        } catch (Exception e) {
            log.warn("数据范围验证异常: value={}", value, e);
            return false;
        }
    }

    /**
     * 验证数据类型
     */
    private boolean validateDataType(DataPoint point, Object value) {
        if (point.getDataType() == null) {
            return true;
        }

        String dataType = point.getDataType().toUpperCase();

        switch (dataType) {
            case "INT":
            case "INTEGER":
                return value instanceof Integer || value instanceof Long ||
                        (value instanceof String && ((String) value).matches("-?\\d+"));

            case "FLOAT":
            case "DOUBLE":
                return value instanceof Float || value instanceof Double ||
                        (value instanceof String && ((String) value).matches("-?\\d+(\\.\\d+)?"));

            case "BOOLEAN":
                return value instanceof Boolean ||
                        (value instanceof String &&
                                ("true".equalsIgnoreCase((String) value) ||
                                        "false".equalsIgnoreCase((String) value) ||
                                        "1".equals(value) || "0".equals(value)));

            case "STRING":
                return value instanceof String;

            default:
                return true;
        }
    }

    /**
     * 验证数据格式
     */
    private boolean validateFormat(DataPoint point, Object value) {
        // 这里可以添加自定义格式验证逻辑
        // 例如：正则表达式、枚举值检查等
        return true;
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
        log.info("数据验证器销毁完成: {}", getName());
    }
}