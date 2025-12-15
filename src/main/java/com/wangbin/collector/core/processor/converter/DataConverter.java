package com.wangbin.collector.core.processor.converter;

import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.core.processor.AbstractDataProcessor;
import com.wangbin.collector.core.processor.ProcessContext;
import com.wangbin.collector.core.processor.ProcessResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * 数据转换器
 */
@Slf4j
@Component
public class DataConverter extends AbstractDataProcessor {

    public DataConverter() {
        this.name = "DataConverter";
        this.type = "CONVERTER";
        this.description = "数据转换器";
        this.priority = 20;
    }

    @Override
    protected void doInit() throws Exception {
        log.info("数据转换器初始化完成: {}", getName());
    }

    @Override
    protected ProcessResult doProcess(ProcessContext context, DataPoint point, Object rawValue) throws Exception {
        try {
            // 获取实际值（考虑缩放因子和偏移量）
            Object processedValue = convertValue(point, rawValue);

            return ProcessResult.success(rawValue, processedValue, "数据转换完成");

        } catch (Exception e) {
            log.error("数据转换异常: point={}, value={}", point.getPointName(), rawValue, e);
            return ProcessResult.error(rawValue, "数据转换异常: " + e.getMessage());
        }
    }

    /**
     * 转换数据值
     */
    private Object convertValue(DataPoint point, Object rawValue) {
        if (rawValue == null) {
            return null;
        }

        // 1. 转换为目标数据类型
        Object converted = convertToTargetType(rawValue, point.getDataType());

        // 2. 应用缩放因子和偏移量
        converted = applyScalingAndOffset(converted, point);

        // 3. 精度处理
        converted = applyPrecision(converted, point);

        return converted;
    }

    /**
     * 转换为目标数据类型
     */
    private Object convertToTargetType(Object value, String targetType) {
        if (targetType == null) {
            return value;
        }

        try {
            switch (targetType.toUpperCase()) {
                case "INT":
                case "INTEGER":
                    if (value instanceof Number) {
                        return ((Number) value).intValue();
                    } else if (value instanceof String) {
                        return Integer.parseInt((String) value);
                    }
                    break;

                case "FLOAT":
                    if (value instanceof Number) {
                        return ((Number) value).floatValue();
                    } else if (value instanceof String) {
                        return Float.parseFloat((String) value);
                    }
                    break;

                case "DOUBLE":
                    if (value instanceof Number) {
                        return ((Number) value).doubleValue();
                    } else if (value instanceof String) {
                        return Double.parseDouble((String) value);
                    }
                    break;

                case "BOOLEAN":
                    if (value instanceof Boolean) {
                        return value;
                    } else if (value instanceof Number) {
                        return ((Number) value).intValue() != 0;
                    } else if (value instanceof String) {
                        String str = (String) value;
                        return "true".equalsIgnoreCase(str) || "1".equals(str);
                    }
                    break;

                case "STRING":
                    return value.toString();
            }
        } catch (Exception e) {
            log.warn("数据类型转换失败: value={}, targetType={}", value, targetType);
        }

        return value;
    }

    /**
     * 应用缩放因子和偏移量
     */
    private Object applyScalingAndOffset(Object value, DataPoint point) {
        if (!(value instanceof Number)) {
            return value;
        }

        double scalingFactor = point.getScalingFactorOrDefault();
        double offset = point.getOffsetOrDefault();

        // 如果都不需要调整，直接返回
        if (scalingFactor == 1.0 && offset == 0.0) {
            return value;
        }

        double doubleValue = ((Number) value).doubleValue();
        double result = doubleValue * scalingFactor + offset;

        // 保持原始数据类型
        if (value instanceof Integer) {
            return (int) result;
        } else if (value instanceof Long) {
            return (long) result;
        } else if (value instanceof Float) {
            return (float) result;
        } else {
            return result;
        }
    }

    /**
     * 应用精度
     */
    private Object applyPrecision(Object value, DataPoint point) {
        if (!(value instanceof Number) || point.getPrecision() == null || point.getPrecision() < 0) {
            return value;
        }

        int precision = point.getPrecision();
        double doubleValue = ((Number) value).doubleValue();

        if (precision == 0) {
            return Math.round(doubleValue);
        }

        double factor = Math.pow(10, precision);
        double result = Math.round(doubleValue * factor) / factor;

        // 保持原始数据类型
        if (value instanceof Integer) {
            return (int) result;
        } else if (value instanceof Long) {
            return (long) result;
        } else if (value instanceof Float) {
            return (float) result;
        } else {
            return result;
        }
    }

    @Override
    protected void doDestroy() throws Exception {
        log.info("数据转换器销毁完成: {}", getName());
    }
}