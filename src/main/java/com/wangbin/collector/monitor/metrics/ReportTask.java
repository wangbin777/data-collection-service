package com.wangbin.collector.monitor.metrics;

import lombok.Builder;
import lombok.Data;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;

/**
 * 统计报表任务的元数据。
 */
@Data
@Builder
public class ReportTask {
    private final String id;
    private final String name;
    private final ReportType type;
    private final long periodSeconds;

    @Builder.Default
    private final Map<String, Object> parameters = Collections.emptyMap();

    @Builder.Default
    private final long lastRun = 0L;

    public enum ReportType {
        DAILY,
        WEEKLY,
        MONTHLY,
        CUSTOM
    }
}
