package com.wangbin.collector.monitor.metrics;

import lombok.Builder;
import lombok.Data;

import java.time.Instant;
import java.util.Collections;
import java.util.List;

/**
 * 设备/连接层面的统计汇总。
 */
@Data
@Builder
public class DeviceStatusSnapshot {

    private final int totalConnections;
    private final int activeConnections;

    @Builder.Default
    private final int healthyDevices = 0;

    @Builder.Default
    private final int warningDevices = 0;

    @Builder.Default
    private final int dangerDevices = 0;

    @Builder.Default
    private final List<DeviceConnectionSnapshot> connections = Collections.emptyList();

    @Builder.Default
    private final long generatedAt = Instant.now().toEpochMilli();
}
