package com.wangbin.collector.core.collector.statistics;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 采集统计管理器
 */
@Slf4j
@Service
public class CollectionStatistics {

    // 设备统计：deviceId -> DeviceStatistics
    private final Map<String, DeviceStatistics> deviceStatistics = new ConcurrentHashMap<>();

    /**
     * 开始采集任务
     */
    public void startCollection(String deviceId, int pointCount) {
        DeviceStatistics stats = deviceStatistics.computeIfAbsent(
                deviceId, k -> new DeviceStatistics(deviceId)
        );
        stats.startTask(pointCount);
    }

    /**
     * 采集成功
     */
    public void collectionSuccess(String deviceId, long executionTime) {
        DeviceStatistics stats = deviceStatistics.get(deviceId);
        if (stats != null) {
            stats.recordSuccess(executionTime);
        }
    }

    /**
     * 采集失败
     */
    public void collectionFailed(String deviceId) {
        DeviceStatistics stats = deviceStatistics.get(deviceId);
        if (stats != null) {
            stats.recordFailed();
        }
    }

    /**
     * 停止采集任务
     */
    public void stopCollection(String deviceId) {
        DeviceStatistics stats = deviceStatistics.get(deviceId);
        if (stats != null) {
            stats.stopTask();
        }
    }

    /**
     * 获取设备统计
     */
    public Map<String, Object> getDeviceStatistics(String deviceId) {
        DeviceStatistics stats = deviceStatistics.get(deviceId);
        if (stats != null) {
            return stats.getStatistics();
        }
        return Collections.emptyMap();
    }

    /**
     * 获取所有设备统计
     */
    public Map<String, Map<String, Object>> getAllStatistics() {
        Map<String, Map<String, Object>> allStats = new HashMap<>();
        for (Map.Entry<String, DeviceStatistics> entry : deviceStatistics.entrySet()) {
            allStats.put(entry.getKey(), entry.getValue().getStatistics());
        }
        return allStats;
    }

    /**
     * 清空设备统计
     */
    public void clearDeviceStatistics(String deviceId) {
        deviceStatistics.remove(deviceId);
    }

    /**
     * 清空所有统计
     */
    public void clearAllStatistics() {
        deviceStatistics.clear();
    }

    /**
     * 设备统计内部类
     */
    @Getter
    private static class DeviceStatistics {
        private final String deviceId;
        private volatile boolean isRunning = false;
        private volatile long startTime = 0;
        private volatile long lastExecutionTime = 0;

        private final AtomicInteger totalExecutions = new AtomicInteger(0);
        private final AtomicInteger successfulExecutions = new AtomicInteger(0);
        private final AtomicInteger failedExecutions = new AtomicInteger(0);
        private final AtomicInteger totalPoints = new AtomicInteger(0);
        private final AtomicLong totalExecutionTime = new AtomicLong(0);

        private int currentTaskPoints = 0;

        public DeviceStatistics(String deviceId) {
            this.deviceId = deviceId;
        }

        public void startTask(int pointCount) {
            this.isRunning = true;
            this.startTime = System.currentTimeMillis();
            this.currentTaskPoints = pointCount;
            this.totalPoints.addAndGet(pointCount);
        }

        public void recordSuccess(long executionTime) {
            totalExecutions.incrementAndGet();
            successfulExecutions.incrementAndGet();
            totalExecutionTime.addAndGet(executionTime);
            lastExecutionTime = System.currentTimeMillis();
        }

        public void recordFailed() {
            totalExecutions.incrementAndGet();
            failedExecutions.incrementAndGet();
            lastExecutionTime = System.currentTimeMillis();
        }

        public void stopTask() {
            this.isRunning = false;
        }

        public Map<String, Object> getStatistics() {
            Map<String, Object> stats = new HashMap<>();
            stats.put("deviceId", deviceId);
            stats.put("isRunning", isRunning);
            stats.put("runningDuration", isRunning ? System.currentTimeMillis() - startTime : 0);
            stats.put("totalExecutions", totalExecutions.get());
            stats.put("successfulExecutions", successfulExecutions.get());
            stats.put("failedExecutions", failedExecutions.get());
            stats.put("totalPoints", totalPoints.get());
            stats.put("currentTaskPoints", currentTaskPoints);
            stats.put("averageExecutionTime", totalExecutions.get() > 0 ?
                    totalExecutionTime.get() / totalExecutions.get() : 0);
            stats.put("successRate", totalExecutions.get() > 0 ?
                    (successfulExecutions.get() * 100.0 / totalExecutions.get()) : 0);
            stats.put("lastExecutionTime", lastExecutionTime);
            return stats;
        }
    }
}