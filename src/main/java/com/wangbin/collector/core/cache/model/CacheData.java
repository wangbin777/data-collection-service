package com.wangbin.collector.core.cache.model;

import lombok.Data;

import java.io.Serializable;

/**
 * 缓存数据
 */
@Data
public class CacheData<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    // 缓存键
    private CacheKey key;

    // 缓存值
    private T value;

    // 缓存时间
    private long cacheTime;

    // 过期时间
    private long expireTime;

    // 剩余时间
    private long remainingTime;

    // 缓存层级
    private int cacheLevel;

    // 缓存类型
    private String cacheType;

    // 数据质量
    private int quality = 100;

    // 版本号
    private int version = 1;

    // 标签信息
    private String tags;

    // 元数据
    private String metadata;

    public CacheData() {
        this.cacheTime = System.currentTimeMillis();
    }

    public CacheData(CacheKey key, T value) {
        this();
        this.key = key;
        this.value = value;
        this.expireTime = key.getExpireTime();
        this.remainingTime = key.getRemainingTime();
        this.cacheLevel = key.getCacheLevel();
    }

    /**
     * 判断是否过期
     */
    public boolean isExpired() {
        if (expireTime == CacheKey.EXPIRE_NEVER) {
            return false;
        }
        return System.currentTimeMillis() - cacheTime > expireTime;
    }

    /**
     * 更新剩余时间
     */
    public void updateRemainingTime() {
        if (expireTime == CacheKey.EXPIRE_NEVER) {
            remainingTime = CacheKey.EXPIRE_NEVER;
        } else {
            remainingTime = Math.max(0, expireTime - (System.currentTimeMillis() - cacheTime));
        }
    }

    /**
     * 更新缓存时间
     */
    public void refresh() {
        this.cacheTime = System.currentTimeMillis();
        updateRemainingTime();
    }

    /**
     * 获取数据摘要
     */
    public String getDataSummary() {
        if (value == null) {
            return "null";
        }

        String valueStr;
        if (value instanceof String) {
            String str = (String) value;
            valueStr = str.length() > 50 ? str.substring(0, 50) + "..." : str;
        } else if (value instanceof byte[]) {
            valueStr = "byte[" + ((byte[]) value).length + "]";
        } else if (value instanceof Iterable) {
            int size = 0;
            for (@SuppressWarnings("unused") Object item : (Iterable<?>) value) {
                size++;
            }
            valueStr = "collection[" + size + "]";
        } else {
            valueStr = value.toString();
        }

        return String.format("CacheData{key=%s, value=%s, quality=%d, expired=%b}",
                key != null ? key.getKey() : "null", valueStr, quality, isExpired());
    }

    /**
     * 复制数据
     */
    public CacheData<T> copy() {
        CacheData<T> copy = new CacheData<>();
        copy.key = this.key;
        copy.value = this.value;
        copy.cacheTime = this.cacheTime;
        copy.expireTime = this.expireTime;
        copy.remainingTime = this.remainingTime;
        copy.cacheLevel = this.cacheLevel;
        copy.cacheType = this.cacheType;
        copy.quality = this.quality;
        copy.version = this.version;
        copy.tags = this.tags;
        copy.metadata = this.metadata;
        return copy;
    }
}
