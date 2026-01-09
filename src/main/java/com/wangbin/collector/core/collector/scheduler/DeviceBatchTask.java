package com.wangbin.collector.core.collector.scheduler;

import com.wangbin.collector.common.domain.entity.DataPoint;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 设备批次任务
 */
class DeviceBatchTask {

    final String deviceId;
    final List<DataPoint> points;
    final int timeSliceIndex;
    long lastExecutionTime;
    private final AtomicInteger failureCount = new AtomicInteger(0);

    DeviceBatchTask(String deviceId, List<DataPoint> points, int timeSliceIndex) {
        this.deviceId = deviceId;
        this.points = points;
        this.timeSliceIndex = timeSliceIndex;
    }

    boolean shouldSkip() {
        return failureCount.get() > 3;
    }

    void recordFailure() {
        failureCount.incrementAndGet();
    }

    void recordSuccess() {
        failureCount.set(0);
    }
}
