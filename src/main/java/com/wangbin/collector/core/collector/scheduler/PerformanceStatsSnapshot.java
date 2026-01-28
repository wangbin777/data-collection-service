package com.wangbin.collector.core.collector.scheduler;

import lombok.Builder;
import lombok.Data;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;

/**
 * 调度与性能监控快照
 */
@Data
@Builder
public class PerformanceStatsSnapshot {

    private final int timeSliceCount;
    private final int timeSliceIntervalMs;

    @Builder.Default
    private final Map<Integer, Long> timeSliceExecutionTimes = Collections.emptyMap();

    @Builder.Default
    private final Map<Integer, Long> overloadedSlices = Collections.emptyMap();

    @Builder.Default
    private final Map<String, Long> slowestDevices = Collections.emptyMap();

    @Builder.Default
    private final Map<String, Map<String, Object>> deviceStats = Collections.emptyMap();

    @Builder.Default
    private final long generatedAt = Instant.now().toEpochMilli();
}
