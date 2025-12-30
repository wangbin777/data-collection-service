package com.wangbin.collector.monitor.health;

import lombok.Builder;
import lombok.Data;

import java.time.Instant;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 系统整体健康状态。
 */
@Data
@Builder
public class HealthStatus {

    private final Status status;

    @Builder.Default
    private final long timestamp = Instant.now().toEpochMilli();

    @Builder.Default
    private final Map<String, ComponentHealth> components = new LinkedHashMap<>();

    public enum Status {
        UP,
        DOWN,
        DEGRADED,
        UNKNOWN
    }

    /**
     * 根据组件状态推导整体状态：优先级 DOWN > DEGRADED > UNKNOWN > UP。
     */
    public static Status aggregate(Collection<ComponentHealth> componentHealths) {
        boolean hasDegraded = false;
        boolean hasUnknown = false;
        for (ComponentHealth component : componentHealths) {
            if (component == null) {
                continue;
            }
            if (component.isDown()) {
                return Status.DOWN;
            }
            if (component.getStatus() == Status.DEGRADED) {
                hasDegraded = true;
            } else if (component.getStatus() == Status.UNKNOWN) {
                hasUnknown = true;
            }
        }
        if (hasDegraded) {
            return Status.DEGRADED;
        }
        if (hasUnknown) {
            return Status.UNKNOWN;
        }
        return Status.UP;
    }
}
