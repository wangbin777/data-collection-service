package com.wangbin.collector.monitor.alert;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class AlertNotification {
    String deviceId;
    String deviceName;
    String pointId;
    String pointCode;
    String ruleId;
    String ruleName;
    String level;
    String message;
    String eventType;
    Object value;
    String unit;
    long timestamp;
}
