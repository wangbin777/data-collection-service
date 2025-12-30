package com.wangbin.collector.monitor.health;

import lombok.Builder;
import lombok.Data;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 描述单个监控组件（例如缓存、连接、数据库等）的健康状态。
 */
@Data
@Builder
public class ComponentHealth {

    private final String name;
    private final HealthStatus.Status status;
    private final String message;

    @Builder.Default
    private final Map<String, Object> details = new LinkedHashMap<>();

    @Builder.Default
    private final long checkedAt = Instant.now().toEpochMilli();

    public boolean isUp() {
        return status == HealthStatus.Status.UP;
    }

    public boolean isDown() {
        return status == HealthStatus.Status.DOWN;
    }
}
