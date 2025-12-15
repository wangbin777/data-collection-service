package com.wangbin.collector.core.processor.filter;

import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.core.processor.AbstractDataProcessor;
import com.wangbin.collector.core.processor.ProcessContext;
import com.wangbin.collector.core.processor.ProcessResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 死区过滤器
 * 避免数据微小变化频繁上报
 */
@Slf4j
@Component
public class DeadbandFilter extends AbstractDataProcessor {

    /**
     * 存储每个数据点的上一个有效值
     */
    private final Map<String, Object> lastValues = new ConcurrentHashMap<>();

    /**
     * 默认死区值
     */
    private double defaultDeadband = 0.1;

    public DeadbandFilter() {
        this.name = "DeadbandFilter";
        this.type = "FILTER";
        this.description = "死区过滤器";
        this.priority = 40;
    }

    @Override
    protected void doInit() throws Exception {
        log.info("死区过滤器初始化完成: {}", getName());
    }

    @Override
    protected ProcessResult doProcess(ProcessContext context, DataPoint point, Object rawValue) throws Exception {
        if (rawValue == null) {
            return ProcessResult.skip(rawValue, "数据值为空");
        }

        try {
            // 获取数据点的唯一标识
            String pointKey = getPointKey(point);

            // 获取上一个值
            Object lastValue = lastValues.get(pointKey);

            // 如果是第一次处理或者没有上次值，直接通过
            if (lastValue == null) {
                lastValues.put(pointKey, rawValue);
                return ProcessResult.success(rawValue, rawValue, "首次处理");
            }

            // 计算变化量
            double change = calculateChange(lastValue, rawValue);

            // 获取死区阈值
            double deadband = getDeadband(point);

            // 判断是否需要过滤
            if (Math.abs(change) <= deadband) {
                return ProcessResult.skip(rawValue, String.format(
                        "变化量 %.6f 小于死区阈值 %.6f", change, deadband));
            }

            // 更新最后的值
            lastValues.put(pointKey, rawValue);

            return ProcessResult.success(rawValue, rawValue,
                    String.format("变化量 %.6f 超过死区阈值", change));

        } catch (Exception e) {
            log.error("死区过滤异常: point={}, value={}", point.getPointName(), rawValue, e);
            return ProcessResult.error(rawValue, "死区过滤异常: " + e.getMessage());
        }
    }

    /**
     * 获取数据点唯一标识
     */
    private String getPointKey(DataPoint point) {
        return point.getPointId() != null ? point.getPointId() :
                point.getDeviceId() + "." + point.getPointCode();
    }

    /**
     * 计算变化量
     */
    private double calculateChange(Object lastValue, Object currentValue) {
        try {
            double last = convertToDouble(lastValue);
            double current = convertToDouble(currentValue);

            if (Double.isNaN(last) || Double.isNaN(current)) {
                return Double.MAX_VALUE; // 如果无法计算，认为有变化
            }

            return Math.abs(current - last);
        } catch (Exception e) {
            return Double.MAX_VALUE;
        }
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

    /**
     * 获取死区阈值
     */
    private double getDeadband(DataPoint point) {
        // 优先使用数据点配置的死区值
        if (point.getDeadband() != null && point.getDeadband() > 0) {
            return point.getDeadband();
        }

        // 使用默认死区值
        return defaultDeadband;
    }

    @Override
    protected void loadConfig(Map<String, Object> config) {
        super.loadConfig(config);
        defaultDeadband = getDoubleConfig("defaultDeadband", 0.1);
    }

    @Override
    protected void doDestroy() throws Exception {
        lastValues.clear();
        log.info("死区过滤器销毁完成: {}", getName());
    }
}