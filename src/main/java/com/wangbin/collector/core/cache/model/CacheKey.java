package com.wangbin.collector.core.cache.model;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 缓存键
 */
@Data
public class CacheKey implements Serializable {

    private static final long serialVersionUID = 1L;

    // 缓存键前缀
    public static final String PREFIX_DEVICE = "device:";
    public static final String PREFIX_DATA = "data:";
    public static final String PREFIX_CONFIG = "config:";
    public static final String PREFIX_CONNECTION = "connection:";
    public static final String PREFIX_TASK = "task:";
    public static final String PREFIX_REALTIME = "realtime:";
    public static final String PREFIX_HISTORY = "history:";
    public static final String PREFIX_ALARM = "alarm:";
    public static final String PREFIX_METRICS = "metrics:";

    // 默认过期时间（毫秒）
    public static final long EXPIRE_NEVER = -1; // 永不过期
    public static final long EXPIRE_1_SECOND = 1000;
    public static final long EXPIRE_5_SECONDS = 5000;
    public static final long EXPIRE_30_SECONDS = 30000;
    public static final long EXPIRE_1_MINUTE = 60000;
    public static final long EXPIRE_5_MINUTES = 300000;
    public static final long EXPIRE_10_MINUTES = 600000;
    public static final long EXPIRE_30_MINUTES = 1800000;
    public static final long EXPIRE_1_HOUR = 3600000;
    public static final long EXPIRE_6_HOURS = 21600000;
    public static final long EXPIRE_12_HOURS = 43200000;
    public static final long EXPIRE_1_DAY = 86400000;
    public static final long EXPIRE_7_DAYS = 604800000;

    // 键值
    private final String key;

    // 过期时间（毫秒）
    private final long expireTime;

    // 缓存层级
    private final int cacheLevel;

    // 附加信息
    private String description;
    private String dataType;
    private String source;
    private long createTime;
    private long updateTime;

    public CacheKey(String key) {
        this(key, EXPIRE_1_HOUR, 1);
    }

    public CacheKey(String key, long expireTime) {
        this(key, expireTime, 1);
    }

    public CacheKey(String key, long expireTime, int cacheLevel) {
        if (key == null || key.trim().isEmpty()) {
            throw new IllegalArgumentException("缓存键不能为空");
        }
        this.key = key.trim();
        this.expireTime = expireTime;
        this.cacheLevel = cacheLevel;
        this.createTime = System.currentTimeMillis();
        this.updateTime = this.createTime;
    }

    /**
     * 获取完整键名
     */
    public String getFullKey() {
        return key;
    }

    /**
     * 判断是否永不过期
     */
    public boolean isNeverExpire() {
        return expireTime == EXPIRE_NEVER;
    }

    /**
     * 判断是否已过期
     */
    public boolean isExpired() {
        if (isNeverExpire()) {
            return false;
        }
        long currentTime = System.currentTimeMillis();
        return (currentTime - createTime) > expireTime;
    }

    /**
     * 获取剩余过期时间
     */
    public long getRemainingTime() {
        if (isNeverExpire()) {
            return EXPIRE_NEVER;
        }
        long currentTime = System.currentTimeMillis();
        long elapsed = currentTime - createTime;
        return Math.max(0, expireTime - elapsed);
    }

    /**
     * 更新最后访问时间
     */
    public void updateAccessTime() {
        this.updateTime = System.currentTimeMillis();
    }

    /**
     * 获取缓存键前缀
     */
    public String getPrefix() {
        int colonIndex = key.indexOf(':');
        if (colonIndex > 0) {
            return key.substring(0, colonIndex + 1);
        }
        return "";
    }

    /**
     * 获取缓存键后缀
     */
    public String getSuffix() {
        int colonIndex = key.indexOf(':');
        if (colonIndex >= 0 && colonIndex < key.length() - 1) {
            return key.substring(colonIndex + 1);
        }
        return key;
    }

    /**
     * 判断是否匹配模式
     */
    public boolean matches(String pattern) {
        if (pattern == null || pattern.isEmpty()) {
            return true;
        }

        // 支持简单的通配符匹配
        if (pattern.contains("*")) {
            String regex = pattern.replace(".", "\\.")
                    .replace("*", ".*")
                    .replace("?", ".");
            return key.matches(regex);
        }

        return key.startsWith(pattern);
    }

    /**
     * 创建设备缓存键
     */
    public static CacheKey deviceKey(String deviceId) {
        return new CacheKey(PREFIX_DEVICE + deviceId, EXPIRE_30_MINUTES, 1);
    }

    public static CacheKey deviceKey(String deviceId, long expireTime) {
        return new CacheKey(PREFIX_DEVICE + deviceId, expireTime, 1);
    }

    /**
     * 创建数据缓存键
     */
    public static CacheKey dataKey(String deviceId, String pointId) {
        return new CacheKey(PREFIX_DATA + deviceId + ":" + pointId, EXPIRE_5_SECONDS, 1);
    }

    public static CacheKey dataKey(String deviceId, String pointId, long expireTime) {
        return new CacheKey(PREFIX_DATA + deviceId + ":" + pointId, expireTime, 1);
    }

    /**
     * 创建实时数据缓存键
     */
    public static CacheKey realtimeDataKey(String deviceId) {
        return new CacheKey(PREFIX_REALTIME + deviceId, EXPIRE_5_SECONDS, 1);
    }

    public static CacheKey realtimeDataKey(String deviceId, String pointId) {
        return new CacheKey(PREFIX_REALTIME + deviceId + ":" + pointId, EXPIRE_5_SECONDS, 1);
    }

    /**
     * 创建配置缓存键
     */
    public static CacheKey configKey(String configType, String configId) {
        return new CacheKey(PREFIX_CONFIG + configType + ":" + configId, EXPIRE_1_HOUR, 2);
    }

    /**
     * 创建连接缓存键
     */
    public static CacheKey connectionKey(String deviceId) {
        return new CacheKey(PREFIX_CONNECTION + deviceId, EXPIRE_30_SECONDS, 1);
    }

    /**
     * 创建任务缓存键
     */
    public static CacheKey taskKey(String taskId) {
        return new CacheKey(PREFIX_TASK + taskId, EXPIRE_10_MINUTES, 2);
    }

    /**
     * 创建历史数据缓存键
     */
    public static CacheKey historyDataKey(String deviceId, String pointId, long timestamp) {
        return new CacheKey(PREFIX_HISTORY + deviceId + ":" + pointId + ":" + timestamp,
                EXPIRE_1_DAY, 3);
    }

    /**
     * 创建告警缓存键
     */
    public static CacheKey alarmKey(String deviceId, String alarmId) {
        return new CacheKey(PREFIX_ALARM + deviceId + ":" + alarmId, EXPIRE_30_MINUTES, 2);
    }

    /**
     * 创建指标缓存键
     */
    public static CacheKey metricsKey(String metricType, String metricId) {
        return new CacheKey(PREFIX_METRICS + metricType + ":" + metricId,
                EXPIRE_5_MINUTES, 2);
    }

    /**
     * 创建带时间范围的缓存键
     */
    public static CacheKey rangeKey(String prefix, String id, long startTime, long endTime) {
        return new CacheKey(prefix + id + ":range:" + startTime + "-" + endTime,
                EXPIRE_10_MINUTES, 2);
    }

    /**
     * 创建批量缓存键
     */
    public static List<CacheKey> batchKeys(String prefix, List<String> ids, long expireTime) {
        List<CacheKey> keys = new ArrayList<>();
        for (String id : ids) {
            keys.add(new CacheKey(prefix + id, expireTime, 1));
        }
        return keys;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CacheKey cacheKey = (CacheKey) o;
        return Objects.equals(key, cacheKey.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key);
    }

    @Override
    public String toString() {
        return "CacheKey{" +
                "key='" + key + '\'' +
                ", expireTime=" + expireTime +
                ", cacheLevel=" + cacheLevel +
                ", remainingTime=" + getRemainingTime() +
                ", expired=" + isExpired() +
                '}';
    }
}
