package com.wangbin.collector.monitor.health;

import com.wangbin.collector.core.cache.manager.MultiLevelCacheManager;
import com.wangbin.collector.core.connection.adapter.ConnectionAdapter;
import com.wangbin.collector.core.connection.manager.ConnectionManager;
import com.wangbin.collector.monitor.health.HealthStatus.Status;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 聚合系统级健康信息的服务。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SystemHealthService {

    private final MultiLevelCacheManager multiLevelCacheManager;
    private final ConnectionManager connectionManager;

    public HealthStatus getSystemHealth() {
        Map<String, ComponentHealth> components = new LinkedHashMap<>();
        components.put("cache", buildCacheHealth());
        components.put("connections", buildConnectionHealth());
        components.put("application", ComponentHealth.builder()
                .name("application")
                .status(Status.UP)
                .message("Application is running")
                .build());

        Status overall = HealthStatus.aggregate(components.values());
        return HealthStatus.builder()
                .status(overall)
                .components(components)
                .build();
    }

    private ComponentHealth buildCacheHealth() {
        try {
            Map<String, Object> cacheHealth = multiLevelCacheManager.getHealthStatus();
            Object overallStatus = cacheHealth.getOrDefault("overallStatus", "UNKNOWN");
            Status status = parseStatus(overallStatus);

            Map<String, Object> details = new LinkedHashMap<>(cacheHealth);
            details.remove("overallStatus");

            return ComponentHealth.builder()
                    .name("multiLevelCache")
                    .status(status)
                    .message("Multi level cache health")
                    .details(details)
                    .build();
        } catch (Exception e) {
            log.warn("获取缓存健康状态失败", e);
            return ComponentHealth.builder()
                    .name("multiLevelCache")
                    .status(Status.UNKNOWN)
                    .message("Failed to read cache health: " + e.getMessage())
                    .build();
        }
    }

    private ComponentHealth buildConnectionHealth() {
        List<ConnectionAdapter> allConnections = connectionManager.getAllConnections();
        List<ConnectionAdapter> activeConnections = connectionManager.getActiveConnections();
        List<String> offlineDevices = allConnections.stream()
                .filter(connection -> !connection.isConnected())
                .map(ConnectionAdapter::getDeviceId)
                .collect(Collectors.toList());

        int total = allConnections.size();
        int active = activeConnections.size();

        Status status;
        if (total == 0) {
            status = Status.UNKNOWN;
        } else if (active == total) {
            status = Status.UP;
        } else if (active == 0) {
            status = Status.DOWN;
        } else {
            status = Status.DEGRADED;
        }

        Map<String, Object> details = new LinkedHashMap<>();
        details.put("totalConnections", total);
        details.put("activeConnections", active);
        details.put("offlineConnections", total - active);
        details.put("offlineDevices", offlineDevices);

        return ComponentHealth.builder()
                .name("connections")
                .status(status)
                .message("Connection state snapshot")
                .details(details)
                .build();
    }

    private Status parseStatus(Object value) {
        if (value == null) {
            return Status.UNKNOWN;
        }

        if (value instanceof Status status) {
            return status;
        }

        if (value instanceof Boolean bool) {
            return bool ? Status.UP : Status.DOWN;
        }

        String normalized = value.toString().trim().toUpperCase();
        switch (normalized) {
            case "UP":
            case "HEALTHY":
            case "OK":
            case "RUNNING":
                return Status.UP;
            case "DOWN":
            case "UNHEALTHY":
            case "FAILED":
            case "ERROR":
            case "CRITICAL":
                return Status.DOWN;
            case "DEGRADED":
            case "WARN":
            case "WARNING":
                return Status.DEGRADED;
            default:
                try {
                    return Status.valueOf(normalized);
                } catch (IllegalArgumentException ignored) {
                    return Status.UNKNOWN;
                }
        }
    }
}
