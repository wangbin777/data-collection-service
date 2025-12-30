package com.wangbin.collector.monitor.metrics;

import lombok.Builder;
import lombok.Data;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;

/**
 * JVM/系统资源指标快照。
 */
@Data
@Builder
public class SystemResourceSnapshot {

    // 内存指标（字节）
    private final long heapUsed;
    private final long heapCommitted;
    private final long heapMax;
    private final long nonHeapUsed;
    private final long nonHeapCommitted;

    // CPU/线程
    private final double processCpuLoad;
    private final double systemCpuLoad;
    private final int threadCount;
    private final int daemonThreadCount;

    // 线程池状态
    @Builder.Default
    private final Map<String, ThreadPoolSnapshot> threadPools = Collections.emptyMap();

    @Builder.Default
    private final long generatedAt = Instant.now().toEpochMilli();

    @Data
    @Builder
    public static class ThreadPoolSnapshot {
        private final int corePoolSize;
        private final int maxPoolSize;
        private final int activeCount;
        private final int queueSize;
        private final long completedTaskCount;
    }
}
