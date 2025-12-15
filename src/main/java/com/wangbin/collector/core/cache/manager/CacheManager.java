package com.wangbin.collector.core.cache.manager;

import com.wangbin.collector.core.cache.model.CacheData;
import com.wangbin.collector.core.cache.model.CacheKey;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 缓存管理器接口
 */
public interface CacheManager {

    /**
     * 初始化缓存管理器
     */
    void init();

    /**
     * 销毁缓存管理器
     */
    void destroy();

    /**
     * 存入缓存
     */
    <T> boolean put(CacheKey key, T value);

    /**
     * 存入缓存（带过期时间）
     */
    <T> boolean put(CacheKey key, T value, long expireTime);

    /**
     * 批量存入缓存
     */
    <T> boolean putAll(Map<CacheKey, T> dataMap);

    /**
     * 获取缓存
     */
    <T> T get(CacheKey key);

    /**
     * 获取缓存（带类型）
     */
    <T> T get(CacheKey key, Class<T> type);

    /**
     * 获取缓存数据（包含元数据）
     */
    <T> CacheData<T> getWithMetadata(CacheKey key);

    /**
     * 批量获取缓存
     */
    <T> Map<CacheKey, T> getAll(List<CacheKey> keys);

    /**
     * 获取缓存数据列表
     */
    <T> List<CacheData<T>> getListWithMetadata(List<CacheKey> keys);

    /**
     * 删除缓存
     */
    boolean delete(CacheKey key);

    /**
     * 批量删除缓存
     */
    boolean deleteAll(List<CacheKey> keys);

    /**
     * 删除模式匹配的缓存
     */
    boolean deleteByPattern(String pattern);

    /**
     * 判断缓存是否存在
     */
    boolean exists(CacheKey key);

    /**
     * 设置过期时间
     */
    boolean expire(CacheKey key, long expireTime);

    /**
     * 获取剩余过期时间
     */
    long getExpire(CacheKey key);

    /**
     * 清除所有缓存
     */
    void clear();

    /**
     * 获取缓存大小
     */
    long size();

    /**
     * 获取缓存键集合
     */
    Set<CacheKey> keys();

    /**
     * 获取模式匹配的缓存键
     */
    Set<CacheKey> keys(String pattern);

    /**
     * 获取缓存统计信息
     */
    Map<String, Object> getStatistics();

    /**
     * 重置统计信息
     */
    void resetStatistics();

    /**
     * 获取缓存层级
     */
    int getCacheLevel();

    /**
     * 获取缓存类型
     */
    String getCacheType();
}
