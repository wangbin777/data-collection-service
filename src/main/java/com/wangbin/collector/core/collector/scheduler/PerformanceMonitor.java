package com.wangbin.collector.core.collector.scheduler;

/**
 * 设备调度信息
 */

import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 性能监控器
 */
@Slf4j
public class PerformanceMonitor {
    final Map<String, DevicePerformance> devicePerformance = new ConcurrentHashMap<>();
    private final AtomicLong totalProcessedPoints = new AtomicLong(0);
    private final AtomicLong totalSuccessfulBatches = new AtomicLong(0);
    private final AtomicLong totalFailedBatches = new AtomicLong(0);
    private final Map<Integer, Long> timeSliceExecutionTimes = new ConcurrentHashMap<>();

    // 系统资源监控
    private final AtomicLong peakMemoryUsage = new AtomicLong(0);
    private final AtomicLong cpuUsage = new AtomicLong(0);

    // 瓶颈分析
    private final Map<String, Long> slowestDevices = new ConcurrentHashMap<>(); // 最慢的设备列表
    private final Map<Integer, Long> overloadedSlices = new ConcurrentHashMap<>(); // 过载的时间片
    private final AtomicBoolean recentTimeSliceTimeout = new AtomicBoolean(false);

    // 统计周期
    private static final long STATISTICS_CYCLE = 60000; // 60秒统计周期
    private long lastStatisticsTime = System.currentTimeMillis();

    void recordTimeSliceExecution(int sliceIndex, long executionTime, AtomicInteger TIME_SLICE_INTERVAL) {
        timeSliceExecutionTimes.put(sliceIndex, executionTime);

        // 检查时间片是否过载（执行时间超过时间片间隔）
        if (executionTime > TIME_SLICE_INTERVAL.get()) {
            overloadedSlices.put(sliceIndex, executionTime);
            recentTimeSliceTimeout.set(true);
        }
    }

    void recordBatchSuccess(String deviceId, int pointCount, long executionTime) {
        totalProcessedPoints.addAndGet(pointCount);
        totalSuccessfulBatches.incrementAndGet();

        DevicePerformance perf = devicePerformance.computeIfAbsent(
                deviceId, k -> new DevicePerformance(deviceId)
        );
        perf.recordSuccess(pointCount, executionTime);

        // 记录最慢设备（执行时间超过200ms）
        if (executionTime > 200) {
            slowestDevices.put(deviceId, executionTime);
        }
    }

    void recordBatchFailure(String deviceId) {
        totalFailedBatches.incrementAndGet();

        DevicePerformance perf = devicePerformance.computeIfAbsent(
                deviceId, k -> new DevicePerformance(deviceId)
        );
        perf.recordFailure();
    }

    void recordDataProcessed(String deviceId) {
        DevicePerformance perf = devicePerformance.get(deviceId);
        if (perf != null) {
            perf.recordDataProcessed();
        }
    }

    void adjustBatchSize(String deviceId, int percentChange) {
        DevicePerformance perf = devicePerformance.get(deviceId);
        if (perf != null) {
            perf.adjustBatchSize(percentChange);
        }
    }

    void logStatistics(AtomicInteger TIME_SLICE_INTERVAL) {
        long currentTime = System.currentTimeMillis();
        long elapsedTime = currentTime - lastStatisticsTime;
        lastStatisticsTime = currentTime;

        long totalPoints = totalProcessedPoints.getAndSet(0);
        long successfulBatches = totalSuccessfulBatches.getAndSet(0);
        long failedBatches = totalFailedBatches.getAndSet(0);

        double pointsPerSecond = elapsedTime > 0 ? totalPoints / (elapsedTime / 1000.0) : 0;
        double batchSuccessRate = successfulBatches + failedBatches > 0 ?
                successfulBatches * 100.0 / (successfulBatches + failedBatches) : 0;

        log.info("性能统计 - 处理点总数：{}, 平均每秒点数: {}/秒, 成功率: {}%, 活跃设备: {}",
                totalPoints,
                String.format("%.2f", pointsPerSecond),
                String.format("%.2f", batchSuccessRate),
                devicePerformance.size()
        );

        // 输出时间片执行情况
        StringBuilder sliceInfo = new StringBuilder("时间片执行时间: ");
        for (Map.Entry<Integer, Long> entry : timeSliceExecutionTimes.entrySet()) {
            sliceInfo.append(String.format("[%d:%dms]", entry.getKey(), entry.getValue()));
            if (entry.getValue() > TIME_SLICE_INTERVAL.get()) {
                sliceInfo.append("(OVERLOAD)");
            }
            sliceInfo.append(", ");
        }
        if (!timeSliceExecutionTimes.isEmpty()) {
            sliceInfo.setLength(sliceInfo.length() - 2); // 移除最后一个逗号和空格
        }
        log.debug(sliceInfo.toString());

        // 输出瓶颈分析
        analyzeBottlenecks();

        // 输出设备健康状态
        reportDeviceHealth();
    }

    /**
     * 瓶颈分析
     */
    private void analyzeBottlenecks() {
        // 1. 检查慢设备
        if (!slowestDevices.isEmpty()) {
            // 按执行时间排序，取前5个最慢设备
            List<Map.Entry<String, Long>> sortedSlowest = slowestDevices.entrySet().stream()
                    .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                    .limit(5)
                    .toList();

            StringBuilder slowDeviceInfo = new StringBuilder("慢速设备（执行时间>200ms）: ");
            for (Map.Entry<String, Long> entry : sortedSlowest) {
                slowDeviceInfo.append(String.format("%s:%dms, ", entry.getKey(), entry.getValue()));
            }
            slowDeviceInfo.setLength(slowDeviceInfo.length() - 2);
            log.warn(slowDeviceInfo.toString());

            // 清空慢速设备列表，准备下一轮统计
            slowestDevices.clear();
        }

        // 2. 检查过载时间片
        if (!overloadedSlices.isEmpty()) {
            StringBuilder overloadInfo = new StringBuilder("过载时间片（执行时间>片间隔）: ");
            for (Map.Entry<Integer, Long> entry : overloadedSlices.entrySet()) {
                overloadInfo.append(String.format("%d:%dms, ", entry.getKey(), entry.getValue()));
            }
            overloadInfo.setLength(overloadInfo.length() - 2);
            log.warn(overloadInfo.toString());

            // 清空过载时间片列表
            overloadedSlices.clear();
        }

        // 3. 检查系统资源
        Runtime runtime = Runtime.getRuntime();
        long currentMemory = runtime.totalMemory() - runtime.freeMemory();
        if (currentMemory > peakMemoryUsage.get()) {
            peakMemoryUsage.set(currentMemory);
        }

        log.debug("系统资源: 当前内存占用={}MB, 峰值内存={}MB, 可用处理器数={}",
                currentMemory / (1024 * 1024),
                peakMemoryUsage.get() / (1024 * 1024),
                Runtime.getRuntime().availableProcessors());
    }

    /**
     * 设备健康报告
     */
    private void reportDeviceHealth() {
        // 统计不同健康状态的设备数量
        long healthyDevices = 0;
        long warningDevices = 0;
        long criticalDevices = 0;

        for (DevicePerformance perf : devicePerformance.values()) {
            double healthScore = perf.calculateHealthScore();
            String risk = perf.predictFailureRisk();

            if (healthScore > 80 && "NONE".equals(risk)) {
                healthyDevices++;
            } else if (healthScore > 60 || "LOW".equals(risk)) {
                warningDevices++;
            } else {
                criticalDevices++;
            }

            // 记录高风险设备
            if ("HIGH".equals(risk) || healthScore < 50) {
                log.warn("设备 {} 健康度低: 健康分={}%, 故障风险={}, 连续失败={}次",
                        perf.deviceId,
                        String.format("%.1f", healthScore),
                        risk,
                        perf.consecutiveFailureCount);
            }
        }

        log.info("设备健康统计: 健康={}, 警告={}, 危险={}", healthyDevices, warningDevices, criticalDevices);
    }

    Map<String, Object> getDevicePerformance(String deviceId) {
        DevicePerformance perf = devicePerformance.get(deviceId);
        if (perf != null) {
            return perf.getStatistics();
        }
        return Collections.emptyMap();
    }

    long getAverageTimeSliceExecution() {
        if (timeSliceExecutionTimes.isEmpty()) {
            return 0;
        }
        long sum = 0;
        int count = 0;
        for (Long value : timeSliceExecutionTimes.values()) {
            if (value != null && value > 0) {
                sum += value;
                count++;
            }
        }
        return count == 0 ? 0 : sum / count;
    }

    boolean consumeTimeSliceTimeout() {
        return recentTimeSliceTimeout.getAndSet(false);
    }

    Map<Integer, Long> getTimeSliceExecutionTimesSnapshot() {
        return new ConcurrentHashMap<>(timeSliceExecutionTimes);
    }

    Map<Integer, Long> getOverloadedSlicesSnapshot() {
        return new ConcurrentHashMap<>(overloadedSlices);
    }

    Map<String, Long> getSlowestDevicesSnapshot() {
        return new ConcurrentHashMap<>(slowestDevices);
    }

    Map<String, Map<String, Object>> getAllDevicePerformance() {
        Map<String, Map<String, Object>> stats = new ConcurrentHashMap<>();
        devicePerformance.forEach((deviceId, perf) -> stats.put(deviceId, perf.getStatistics()));
        return stats;
    }
}
