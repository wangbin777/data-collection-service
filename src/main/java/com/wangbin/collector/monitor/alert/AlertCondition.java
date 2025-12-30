package com.wangbin.collector.monitor.alert;

import lombok.Builder;
import lombok.Data;

/**
 * 告警条件定义。
 */
@Data
@Builder
public class AlertCondition {
    private final String metric;
    private final double threshold;
    private final Comparator comparator;

    public enum Comparator {
        GREATER_THAN,
        LESS_THAN,
        EQUALS
    }
}
