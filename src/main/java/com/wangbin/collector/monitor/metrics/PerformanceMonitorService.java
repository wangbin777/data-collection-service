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
            result.add(CollectorMetrics.builder()
                    .deviceId(deviceId)
                    .protocol(collector.getProtocolType())
                    .processedPoints(longValue(stats, "totalReadCount"))
                    .pointsPerSecond(doubleValue(stats, "pointsPerSecond"))
                    .successRate(doubleValue(stats, "successRate"))
                    .averageLatencyMs(doubleValue(stats, "averageReadTime"))
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

    private double doubleValue(Map<String, Object> stats, String key) {
        Object value = stats.get(key);
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        if (value instanceof String text && !text.isEmpty()) {
            try {
                return Double.parseDouble(text);
            } catch (NumberFormatException ignored) {
            }
        }
        return 0.0;
    }
}
