package com.wangbin.collector.monitor.metrics;

import lombok.Builder;
import lombok.Data;

import java.time.Instant;

/**
 * 采集器层面的聚合指标，描述单个设备或任务的运行健康状况。
 */
@Data
@Builder
public class CollectorMetrics {

    private final String deviceId;
    private final String protocol;

    /**
     * 最近一个统计周期内处理的数据点数量。
     */
    private final long processedPoints;

    /**
     * 每秒处理的数据点速率。
     */
    private final double pointsPerSecond;

    /**
     * 采集成功率（0-100）。
     */
    private final double successRate;

    /**
     * 平均采集延迟，单位毫秒。
     */
    private final double averageLatencyMs;

    /**
     * 统计生成时间。
     */
    @Builder.Default
    private final long timestamp = Instant.now().toEpochMilli();

    public static CollectorMetrics idle(String deviceId, String protocol) {
        return CollectorMetrics.builder()
                .deviceId(deviceId)
                .protocol(protocol)
                .processedPoints(0)
                .pointsPerSecond(0.0)
                .successRate(0.0)
                .averageLatencyMs(0.0)
                .build();
    }
}
