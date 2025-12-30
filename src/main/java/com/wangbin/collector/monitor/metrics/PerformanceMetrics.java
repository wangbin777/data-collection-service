package com.wangbin.collector.monitor.metrics;

import lombok.Builder;
import lombok.Data;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 通用性能指标模型，用于描述采集过程中产生的任意数值型指标。
 */
@Data
@Builder
public class PerformanceMetrics {

    /**
     * 指标名称，例如 points.per.second、cache.hit.rate。
     */
    private final String name;

    /**
     * 指标的数值。
     */
    private final double value;

    /**
     * 指标单位，如 ms、ops/s、percentage 等。
     */
    private final String unit;

    /**
     * 附加标签，便于按照设备、协议、线程池等维度聚合分析。
     */
    @Builder.Default
    private final Map<String, String> tags = new LinkedHashMap<>();

    /**
     * 采集时间戳（毫秒）。
     */
    @Builder.Default
    private final long timestamp = Instant.now().toEpochMilli();

    /**
     * 添加额外的标签信息，便于链式使用。
     */
    public PerformanceMetrics withTag(String key, String value) {
        tags.put(key, value);
        return this;
    }
}
