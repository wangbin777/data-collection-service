package com.wangbin.collector.monitor.metrics;

import com.wangbin.collector.core.collector.manager.CollectionManager;
import com.wangbin.collector.core.collector.protocol.base.ProtocolCollector;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 采集性能指标监控服务。
 */
@Service
@RequiredArgsConstructor
public class PerformanceMonitorService {

    private final CollectionManager collectionManager;

    public List<CollectorMetrics> getCollectorMetrics() {
        List<CollectorMetrics> result = new ArrayList<>();
        for (String deviceId : collectionManager.getAllDeviceIds()) {
            ProtocolCollector collector = collectionManager.getCollector(deviceId);
            if (collector == null) {
                continue;
            }
            Map<String, Object> stats = collector.getStatistics();
            long processed = longValue(stats, "totalReadCount");
            long errors = longValue(stats, "totalErrorCount");
            long totalReadTime = longValue(stats, "totalReadTime");
            long connectionDuration = longValue(stats, "connectionDuration");

            double pointsPerSecond = connectionDuration > 0
                    ? processed / Math.max(1d, connectionDuration / 1000d)
                    : processed;
            long totalOperations = processed + errors;
            double successRate = totalOperations > 0
                    ? (double) processed * 100.0 / totalOperations
                    : 100.0;
            double averageLatency = processed > 0
                    ? (double) totalReadTime / processed
                    : 0.0;

            result.add(CollectorMetrics.builder()
                    .deviceId(deviceId)
                    .protocol(collector.getProtocolType())
                    .processedPoints(processed)
                    .pointsPerSecond(pointsPerSecond)
                    .successRate(successRate)
                    .averageLatencyMs(averageLatency)
                    .build());
        }
        return result;
    }

    private long longValue(Map<String, Object> stats, String key) {
        Object value = stats.get(key);
        if (value instanceof Number number) {
            return number.longValue();
        }
        if (value instanceof String text && !text.isEmpty()) {
            try {
                return Long.parseLong(text);
            } catch (NumberFormatException ignored) {
            }
        }
        return 0L;
    }

}
