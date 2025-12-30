package com.wangbin.collector.monitor.alert;

import lombok.Builder;
import lombok.Data;

import java.util.Collections;
import java.util.List;

@Data
@Builder
public class AlertRule {
    private final String id;
    private final String name;
    private final AlertLevel level;

    @Builder.Default
    private final List<AlertCondition> conditions = Collections.emptyList();

    private final String notificationChannel;
}
