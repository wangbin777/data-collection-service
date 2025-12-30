package com.wangbin.collector.monitor.metrics;

import lombok.Builder;
import lombok.Data;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;

/**
 * 缓存系统实时指标快照。
 */
@Data
@Builder
public class CacheMetricsSnapshot {

    private final long totalReads;
    private final long totalWrites;
    private final long totalDeletes;
    private final long totalMisses;
    private final long totalAccess;

    private final double totalHitRate;
    private final double level1HitRate;
    private final double level2HitRate;
    private final double missRate;

    @Builder.Default
    private final Map<String, Map<String, Object>> levelStatistics = Collections.emptyMap();

    @Builder.Default
    private final long generatedAt = Instant.now().toEpochMilli();
}
