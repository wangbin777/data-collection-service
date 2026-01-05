package com.wangbin.collector.core.report.shadow;

/**
 * 属性值元数据
 */
public class ValueMeta {

    private final Object value;
    private final long timestamp;
    private final String quality;
    private final long updatedAt;

    public ValueMeta(Object value, long timestamp, String quality) {
        this.value = value;
        this.timestamp = timestamp;
        this.quality = quality;
        this.updatedAt = System.currentTimeMillis();
    }

    public Object getValue() {
        return value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getQuality() {
        return quality;
    }

    public long getUpdatedAt() {
        return updatedAt;
    }
}
