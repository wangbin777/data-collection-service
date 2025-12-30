package com.wangbin.collector.monitor.alert;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
public class AlertManager {

    private final Map<String, AlertRule> rules = new ConcurrentHashMap<>();

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

    public void evaluate(String metric, double value) {
        for (AlertRule rule : rules.values()) {
            boolean triggered = rule.getConditions().stream()
                    .allMatch(condition -> compare(condition, metric, value));
            if (triggered) {
                log.warn("Alert triggered: {} value {}", rule.getName(), value);
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
