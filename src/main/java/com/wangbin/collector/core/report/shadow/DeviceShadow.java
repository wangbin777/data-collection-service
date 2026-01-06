package com.wangbin.collector.core.report.shadow;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 设备影子，缓存设备最新属性值。
 */
@Data
public class DeviceShadow {

    private final String deviceId;
    private final Map<String, ValueMeta> latest = new ConcurrentHashMap<>();
    private final Map<String, Object> lastReportedValues = new ConcurrentHashMap<>();
    private final Map<String, Long> lastReportedTs = new ConcurrentHashMap<>();
    private final Map<String, Long> lastChangeTriggerAt = new ConcurrentHashMap<>();
    private final Map<String, Long> lastEventTriggerAt = new ConcurrentHashMap<>();
    private final Map<String, Long> eventSignatureTimes = new ConcurrentHashMap<>();
    private final AtomicLong sequence = new AtomicLong(0);
    private volatile long lastReportAt;
    private volatile long lastWindowStart;
    private volatile long lastWindowEnd;

    public DeviceShadow(String deviceId) {
        this.deviceId = deviceId;
        this.lastReportAt = System.currentTimeMillis();
    }

    public void update(String field, ValueMeta valueMeta) {
        latest.put(field, valueMeta);
    }

    public ValueMeta getLatest(String field) {
        return latest.get(field);
    }

    public Map<String, ValueMeta> snapshot() {
        return Collections.unmodifiableMap(new LinkedHashMap<>(latest));
    }

    public boolean isEmpty() {
        return latest.isEmpty();
    }

    public long nextSeq() {
        return sequence.incrementAndGet();
    }


    public void setLastWindow(long start, long end) {
        this.lastWindowStart = start;
        this.lastWindowEnd = end;
    }

    public Object getLastReportedValue(String field) {
        return lastReportedValues.get(field);
    }

    public Long getLastReportedTimestamp(String field) {
        return lastReportedTs.get(field);
    }

    public void markReportedValues(Map<String, Object> values, Map<String, Long> propertyTs) {
        if (values == null) {
            return;
        }
        values.forEach((field, value) -> {
            if (field != null) {
                lastReportedValues.put(field, value);
                if (propertyTs != null && propertyTs.containsKey(field)) {
                    lastReportedTs.put(field, propertyTs.get(field));
                }
            }
        });
    }

    public long getLastChangeTriggerAt(String field) {
        return lastChangeTriggerAt.getOrDefault(field, 0L);
    }

    public void markChangeTrigger(String field, long timestamp) {
        if (field != null) {
            lastChangeTriggerAt.put(field, timestamp);
        }
    }

    public long getLastEventTriggerAt(String field) {
        return lastEventTriggerAt.getOrDefault(field, 0L);
    }

    public void markEventTrigger(String field, long timestamp) {
        if (field != null) {
            lastEventTriggerAt.put(field, timestamp);
        }
    }

    public long getLastEventSignatureAt(String signature) {
        return eventSignatureTimes.getOrDefault(signature, 0L);
    }

    public void markEventSignature(String signature, long timestamp) {
        if (signature != null) {
            eventSignatureTimes.put(signature, timestamp);
        }
    }
}
