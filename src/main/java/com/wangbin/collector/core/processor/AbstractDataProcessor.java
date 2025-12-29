package com.wangbin.collector.core.processor;

import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.common.domain.enums.DataQuality;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 抽象数据处理器
 */
@Slf4j
public abstract class AbstractDataProcessor implements DataProcessor {

    protected String name;
    protected String type;
    protected String description;
    protected int priority = 100;
    protected boolean enabled = true;
    protected Map<String, Object> config = new HashMap<>();

    // 统计信息
    protected AtomicLong totalProcessed = new AtomicLong(0);
    protected AtomicLong successfulProcessed = new AtomicLong(0);
    protected AtomicLong failedProcessed = new AtomicLong(0);
    protected AtomicLong totalProcessingTime = new AtomicLong(0);
    protected AtomicLong averageProcessingTime = new AtomicLong(0);

    @Override
    public String getName() {
        return name != null ? name : getClass().getSimpleName();
    }

    @Override
    public String getType() {
        return type != null ? type : "UNKNOWN";
    }

    @Override
    public String getDescription() {
        return description != null ? description : "No description";
    }

    @Override
    public int getPriority() {
        return priority;
    }

    @Override
    public void init(Map<String, Object> config) {
        if (config != null) {
            this.config.putAll(config);
        }

        // 从配置中加载参数
        loadConfig(config);

        try {
            doInit();
            log.info("数据处理器初始化完成: {} [{}]", getName(), getType());
        } catch (Exception e) {
            log.error("数据处理器初始化失败: {}", getName(), e);
            throw new RuntimeException("数据处理器初始化失败: " + getName(), e);
        }
    }

    @Override
    public ProcessResult process(ProcessContext context, DataPoint point, Object rawValue) {
        if (!enabled) {
            return ProcessResult.skip(rawValue, "处理器已禁用");
        }

        long startTime = System.currentTimeMillis();
        ProcessResult result = null;

        try {
            // 执行实际处理逻辑
            result = doProcess(context, point, rawValue);

            // 设置处理器名称
            if (result != null) {
                result.setProcessorName(getName());
                result.setProcessingTime(System.currentTimeMillis() - startTime);
            } else {
                result = ProcessResult.error(rawValue, "处理器返回空结果", DataQuality.PROCESS_ERROR);
                result.setProcessorName(getName());
            }

            // 更新统计
            totalProcessed.incrementAndGet();
            if (result.isSuccess()) {
                successfulProcessed.incrementAndGet();
            } else {
                failedProcessed.incrementAndGet();
            }

            long processingTime = System.currentTimeMillis() - startTime;
            totalProcessingTime.addAndGet(processingTime);

            // 避免除零异常
            long total = totalProcessed.get();
            if (total > 0) {
                averageProcessingTime.set(totalProcessingTime.get() / total);
            }

            log.debug("数据处理完成: processor={}, point={}, rawValu={}，processedValue={}， success={}, time={}ms",
                    getName(), point.getPointName(),result.getRawValue(),result.getProcessedValue(), result.isSuccess(), processingTime);

            return result;
        } catch (Exception e) {
            failedProcessed.incrementAndGet();
            log.error("数据处理异常: processor={}, point={}", getName(), point.getPointName(), e);
            ProcessResult errorResult = ProcessResult.error(rawValue, e.getMessage(), DataQuality.PROCESS_ERROR);
            errorResult.setProcessorName(getName());
            return errorResult;
        }
    }

    @Override
    public Map<String, ProcessResult> batchProcess(ProcessContext context,
                                                   Map<DataPoint, Object> dataPoints) {
        if (!enabled || dataPoints == null || dataPoints.isEmpty()) {
            return new HashMap<>();
        }

        Map<String, ProcessResult> results = new HashMap<>();
        long startTime = System.currentTimeMillis();

        try {
            for (Map.Entry<DataPoint, Object> entry : dataPoints.entrySet()) {
                ProcessResult result = process(context, entry.getKey(), entry.getValue());
                results.put(entry.getKey().getPointId(), result);
            }

            log.debug("批量数据处理完成: processor={}, count={}, time={}ms",
                    getName(), dataPoints.size(), System.currentTimeMillis() - startTime);

            return results;
        } catch (Exception e) {
            log.error("批量数据处理异常: processor={}", getName(), e);
            return results;
        }
    }

    @Override
    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
        log.info("数据处理器{}: {}", enabled ? "启用" : "禁用", getName());
    }

    @Override
    public Map<String, Object> getStatistics() {
        Map<String, Object> stats = new HashMap<>();

        stats.put("name", getName());
        stats.put("type", getType());
        stats.put("enabled", enabled);
        stats.put("priority", priority);
        stats.put("totalProcessed", totalProcessed.get());
        stats.put("successfulProcessed", successfulProcessed.get());
        stats.put("failedProcessed", failedProcessed.get());
        stats.put("successRate", getSuccessRate());
        stats.put("totalProcessingTime", totalProcessingTime.get());
        stats.put("averageProcessingTime", averageProcessingTime.get());

        // 添加具体实现的统计信息
        Map<String, Object> implStats = getImplementationStatistics();
        if (implStats != null) {
            stats.putAll(implStats);
        }

        return stats;
    }

    @Override
    public void resetStatistics() {
        totalProcessed.set(0);
        successfulProcessed.set(0);
        failedProcessed.set(0);
        totalProcessingTime.set(0);
        averageProcessingTime.set(0);
        log.info("数据处理器统计重置: {}", getName());
    }

    @Override
    public void destroy() {
        try {
            doDestroy();
            log.info("数据处理器销毁完成: {}", getName());
        } catch (Exception e) {
            log.error("数据处理器销毁失败: {}", getName(), e);
        }
    }

    // =============== 抽象方法 ===============

    /**
     * 执行初始化
     */
    protected abstract void doInit() throws Exception;

    /**
     * 执行处理逻辑
     */
    protected abstract ProcessResult doProcess(ProcessContext context,
                                               DataPoint point,
                                               Object rawValue) throws Exception;

    /**
     * 执行销毁
     */
    protected abstract void doDestroy() throws Exception;

    // =============== 辅助方法 ===============

    /**
     * 从配置加载参数
     */
    protected void loadConfig(Map<String, Object> config) {
        if (config == null) {
            return;
        }

        if (config.containsKey("name")) {
            name = (String) config.get("name");
        }

        if (config.containsKey("type")) {
            type = (String) config.get("type");
        }

        if (config.containsKey("description")) {
            description = (String) config.get("description");
        }

        if (config.containsKey("priority")) {
            Object priorityObj = config.get("priority");
            if (priorityObj instanceof Number) {
                priority = ((Number) priorityObj).intValue();
            } else if (priorityObj instanceof String) {
                try {
                    priority = Integer.parseInt((String) priorityObj);
                } catch (NumberFormatException e) {
                    log.warn("优先级配置格式错误: {}", priorityObj);
                }
            }
        }

        if (config.containsKey("enabled")) {
            Object enabledObj = config.get("enabled");
            if (enabledObj instanceof Boolean) {
                enabled = (Boolean) enabledObj;
            } else if (enabledObj instanceof String) {
                enabled = Boolean.parseBoolean((String) enabledObj);
            }
        }
    }

    /**
     * 获取成功率
     */
    protected double getSuccessRate() {
        long total = totalProcessed.get();
        if (total == 0) {
            return 0.0;
        }
        return (double) successfulProcessed.get() / total * 100;
    }

    /**
     * 获取实现特定的统计信息
     */
    protected Map<String, Object> getImplementationStatistics() {
        return null;
    }

    /**
     * 获取配置值
     */
    protected Object getConfigValue(String key) {
        return config.get(key);
    }

    /**
     * 获取配置值（带默认值）
     */
    @SuppressWarnings("unchecked")
    protected <T> T getConfigValue(String key, T defaultValue) {
        Object value = config.get(key);
        return value != null ? (T) value : defaultValue;
    }

    /**
     * 获取字符串配置值
     */
    protected String getStringConfig(String key, String defaultValue) {
        Object value = getConfigValue(key);
        return value != null ? value.toString() : defaultValue;
    }

    /**
     * 获取整数配置值
     */
    protected int getIntConfig(String key, int defaultValue) {
        Object value = getConfigValue(key);
        if (value instanceof Number) {
            return ((Number) value).intValue();
        } else if (value instanceof String) {
            try {
                return Integer.parseInt((String) value);
            } catch (NumberFormatException e) {
                return defaultValue;
            }
        }
        return defaultValue;
    }

    /**
     * 获取浮点数配置值
     */
    protected double getDoubleConfig(String key, double defaultValue) {
        Object value = getConfigValue(key);
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        } else if (value instanceof String) {
            try {
                return Double.parseDouble((String) value);
            } catch (NumberFormatException e) {
                return defaultValue;
            }
        }
        return defaultValue;
    }

    /**
     * 获取布尔配置值
     */
    protected boolean getBooleanConfig(String key, boolean defaultValue) {
        Object value = getConfigValue(key);
        if (value instanceof Boolean) {
            return (Boolean) value;
        } else if (value instanceof String) {
            return Boolean.parseBoolean((String) value);
        } else if (value instanceof Number) {
            return ((Number) value).intValue() != 0;
        }
        return defaultValue;
    }
}