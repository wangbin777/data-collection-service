package com.wangbin.collector.monitor.metrics;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

/**
 * 异常&错误监控服务。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ExceptionMonitorService {

    private static final int MAX_RECENT = 100;

    private final LongAdder totalCounter = new LongAdder();
    private final Map<String, LongAdder> categoryCounter = new ConcurrentHashMap<>();
    private final Map<String, LongAdder> deviceCounter = new ConcurrentHashMap<>();
    private final ArrayDeque<ExceptionSummary> recent = new ArrayDeque<>(MAX_RECENT);

    public void record(Throwable throwable, String deviceId, String pointId) {
        String category = categorize(throwable);
        totalCounter.increment();
        categoryCounter.computeIfAbsent(category, k -> new LongAdder()).increment();
        if (deviceId != null && !deviceId.isEmpty()) {
            deviceCounter.computeIfAbsent(deviceId, k -> new LongAdder()).increment();
        }

        ExceptionSummary summary = ExceptionSummary.builder()
                .deviceId(deviceId)
                .pointId(pointId)
                .category(category)
                .message(throwable != null ? throwable.getMessage() : null)
                .timestamp(Instant.now().toEpochMilli())
                .build();

        synchronized (recent) {
            if (recent.size() >= MAX_RECENT) {
                recent.removeFirst();
            }
            recent.addLast(summary);
        }
    }

    public ExceptionStatsSnapshot getStats() {
        Map<String, Long> categoryStats = categoryCounter.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                        entry -> entry.getValue().longValue()));

        Map<String, Long> deviceStats = deviceCounter.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                        entry -> entry.getValue().longValue()));

        List<ExceptionSummary> recentCopy;
        synchronized (recent) {
            recentCopy = new ArrayList<>(recent);
        }

        return ExceptionStatsSnapshot.builder()
                .totalExceptions(totalCounter.longValue())
                .byCategory(categoryStats)
                .byDevice(deviceStats)
                .recent(Collections.unmodifiableList(recentCopy))
                .build();
    }

    private String categorize(Throwable throwable) {
        if (throwable == null) {
            return "UNKNOWN";
        }
        if (throwable instanceof ConnectException) {
            return "CONNECTION";
        }
        if (throwable instanceof SocketTimeoutException
                || throwable.getMessage() != null && throwable.getMessage().toLowerCase().contains("timeout")) {
            return "TIMEOUT";
        }
        if (throwable.getMessage() != null && throwable.getMessage().toLowerCase().contains("parse")) {
            return "PARSE";
        }
        if (throwable.getMessage() != null && throwable.getMessage().toLowerCase().contains("auth")) {
            return "AUTH";
        }
        return throwable.getClass().getSimpleName().toUpperCase();
    }
}
