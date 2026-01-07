package com.wangbin.collector.monitor.alert;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

@Slf4j
@Component
public class AlertManager {

    private final Map<String, AlertRule> rules = new ConcurrentHashMap<>();
    private final Queue<AlertNotification> recentAlerts = new ConcurrentLinkedQueue<>();
    private final int maxHistorySize = 1000;

    public void register(AlertRule rule) {
        rules.put(rule.getId(), rule);
        log.info("Registered alert rule {}", rule.getName());
    }

    public Collection<AlertRule> getRules() {
        return rules.values();
    }

    public void remove(String ruleId) {
        rules.remove(ruleId);
    }

    public void notifyAlert(AlertNotification notification) {
        if (notification == null) {
            return;
        }
        recentAlerts.add(notification);
        while (recentAlerts.size() > maxHistorySize) {
            recentAlerts.poll();
        }
        log.warn("Alert triggered: device={}, point={}, level={}, message={}",
                notification.getDeviceId(),
                notification.getPointCode() != null ? notification.getPointCode() : notification.getPointId(),
                notification.getLevel(),
                notification.getMessage());
    }

    public List<AlertNotification> getRecentAlerts() {
        return List.copyOf(recentAlerts);
    }

    /**
     * Evaluate metric-based rules and push notifications when triggered.
     */
    public void evaluate(String metric, double value) {
        for (AlertRule rule : rules.values()) {
            boolean triggered = rule.getConditions().stream()
                    .allMatch(condition -> compare(condition, metric, value));
            if (triggered) {
                notifyAlert(AlertNotification.builder()
                        .eventType("METRIC")
                        .ruleId(rule.getId())
                        .ruleName(rule.getName())
                        .level(rule.getLevel() != null ? rule.getLevel().name() : "WARNING")
                        .message("Metric " + metric + " threshold reached")
                        .value(value)
                        .timestamp(System.currentTimeMillis())
                        .build());
            }
        }
    }

    private boolean compare(AlertCondition condition, String metric, double value) {
        if (!condition.getMetric().equals(metric)) {
            return true;
        }
        return switch (condition.getComparator()) {
            case GREATER_THAN -> value > condition.getThreshold();
            case LESS_THAN -> value < condition.getThreshold();
            case EQUALS -> value == condition.getThreshold();
        };
    }
}
