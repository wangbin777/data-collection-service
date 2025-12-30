package com.wangbin.collector.monitor.metrics;

import com.wangbin.collector.core.cache.manager.MultiLevelCacheManager;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 缓存指标采集服务。
 */
@Service
@RequiredArgsConstructor
public class CacheMonitorService {

    private final MultiLevelCacheManager multiLevelCacheManager;

    public CacheMetricsSnapshot getCacheMetrics() {
        Map<String, Object> stats = multiLevelCacheManager.getStatistics();
        return CacheMetricsSnapshot.builder()
                .totalReads(longValue(stats, "totalReads"))
                .totalWrites(longValue(stats, "totalWrites"))
                .totalDeletes(longValue(stats, "totalDeletes"))
                .totalMisses(longValue(stats, "totalMisses"))
                .totalAccess(longValue(stats, "totalAccess"))
                .totalHitRate(percentValue(stats, "totalHitRate"))
                .level1HitRate(percentValue(stats, "level1HitRate"))
                .level2HitRate(percentValue(stats, "level2HitRate"))
                .missRate(percentValue(stats, "missRate"))
                .levelStatistics(levelStats(stats.get("levelStatistics")))
                .build();
    }

    private long longValue(Map<String, Object> source, String key) {
        Object value = source.get(key);
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

    private double percentValue(Map<String, Object> source, String key) {
        Object value = source.get(key);
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        if (value instanceof String text && !text.isEmpty()) {
            String normalized = text.replace("%", "").trim();
            try {
                return Double.parseDouble(normalized);
            } catch (NumberFormatException ignored) {
            }
        }
        return 0.0;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Map<String, Object>> levelStats(Object value) {
        if (!(value instanceof Map<?, ?> map)) {
            return Collections.emptyMap();
        }
        Map<String, Map<String, Object>> result = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (!(entry.getKey() instanceof String key)) {
                continue;
            }
            Object val = entry.getValue();
            if (val instanceof Map<?, ?> inner) {
                Map<String, Object> safeMap = new LinkedHashMap<>();
                inner.forEach((innerKey, innerValue) -> {
                    if (innerKey != null) {
                        safeMap.put(innerKey.toString(), innerValue);
                    }
                });
                result.put(key, safeMap);
            }
        }
        return result;
    }
}
