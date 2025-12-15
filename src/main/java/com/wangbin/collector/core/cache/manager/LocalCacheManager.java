package com.wangbin.collector.core.cache.manager;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.wangbin.collector.core.cache.model.CacheData;
import com.wangbin.collector.core.cache.model.CacheKey;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * 本地缓存管理器（基于Caffeine）
 */
@Slf4j
@Component("localCacheManager")
public class LocalCacheManager extends AbstractCacheManager {

    @Value("${collector.cache.local.max-size:10000}")
    private long maxSize;

    @Value("${collector.cache.local.expire-after-write:300}")
    private long expireAfterWrite; // 秒

    @Value("${collector.cache.local.expire-after-access:60}")
    private long expireAfterAccess; // 秒

    @Value("${collector.cache.local.initial-capacity:1000}")
    private int initialCapacity;

    private Cache<String, CacheData<?>> cache;
    private final Map<String, CacheKey> keyMapping = new ConcurrentHashMap<>();

    public LocalCacheManager() {
        super("LOCAL_CAFFEINE", 1);
    }

    // 移除监听器声明
    private final RemovalListener<String, CacheData<?>> removalListener =
            (key, value, cause) -> {
                if (key != null) {
                    keyMapping.remove(key);
                    log.debug("本地缓存条目被移除: key={}, cause={}, value={}",
                            key, cause, value != null ? value.getDataSummary() : "null");
                }
            };

    @Override
    protected void doInit() throws Exception {
        // 使用具体的泛型类型
        Caffeine<String, CacheData<?>> caffeine = Caffeine.newBuilder()
                .initialCapacity(initialCapacity)
                .maximumSize(maxSize)
                .expireAfterWrite(expireAfterWrite, TimeUnit.SECONDS)
                .expireAfterAccess(expireAfterAccess, TimeUnit.SECONDS)
                .recordStats()
                .removalListener(removalListener);

        cache = caffeine.build();

        log.info("本地缓存管理器初始化完成: maxSize={}, expireAfterWrite={}s, expireAfterAccess={}s",
                maxSize, expireAfterWrite, expireAfterAccess);
    }

    @Override
    protected void doDestroy() throws Exception {
        if (cache != null) {
            cache.invalidateAll();
            cache.cleanUp();
            keyMapping.clear();
        }
        log.info("本地缓存管理器已销毁");
    }

    @Override
    protected <T> boolean doPut(CacheKey key, T value, long expireTime) throws Exception {
        String cacheKey = key.getFullKey();

        CacheData<T> cacheData = new CacheData<>(key, value);
        cacheData.setCacheType(getCacheType());
        cacheData.setCacheLevel(getCacheLevel());
        cacheData.setExpireTime(expireTime);
        cacheData.updateRemainingTime();

        cache.put(cacheKey, cacheData);
        keyMapping.put(cacheKey, key);

        return true;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <T> T doGet(CacheKey key) throws Exception {
        String cacheKey = key.getFullKey();
        CacheData<?> cacheData = cache.getIfPresent(cacheKey);

        if (cacheData == null) {
            return null;
        }

        // 检查是否过期
        if (cacheData.isExpired()) {
            cache.invalidate(cacheKey);
            keyMapping.remove(cacheKey);
            return null;
        }

        // 更新访问时间
        cacheData.refresh();

        try {
            return (T) cacheData.getValue();
        } catch (ClassCastException e) {
            log.warn("缓存值类型转换失败: key={}, expected={}, actual={}",
                    key.getKey(), cacheData.getValue().getClass().getName(),
                    e.getMessage());
            return null;
        }
    }

    @Override
    protected boolean doDelete(CacheKey key) throws Exception {
        String cacheKey = key.getFullKey();
        cache.invalidate(cacheKey);
        keyMapping.remove(cacheKey);
        return true;
    }

    @Override
    protected int doDeleteByPattern(String pattern) throws Exception {
        int deletedCount = 0;
        List<String> keysToDelete = new ArrayList<>();

        for (String key : keyMapping.keySet()) {
            if (pattern == null || pattern.isEmpty() || key.startsWith(pattern)) {
                keysToDelete.add(key);
            }
        }

        for (String key : keysToDelete) {
            cache.invalidate(key);
            keyMapping.remove(key);
            deletedCount++;
        }

        return deletedCount;
    }

    @Override
    protected boolean doExists(CacheKey key) throws Exception {
        String cacheKey = key.getFullKey();
        CacheData<?> cacheData = cache.getIfPresent(cacheKey);
        return cacheData != null && !cacheData.isExpired();
    }

    @Override
    protected boolean doExpire(CacheKey key, long expireTime) throws Exception {
        String cacheKey = key.getFullKey();
        CacheData<?> cacheData = cache.getIfPresent(cacheKey);

        if (cacheData == null) {
            return false;
        }

        // Caffeine不支持直接修改过期时间，需要重新放入
        cacheData.setExpireTime(expireTime);
        cacheData.refresh();
        cache.put(cacheKey, cacheData);

        return true;
    }

    @Override
    protected long doGetExpire(CacheKey key) throws Exception {
        String cacheKey = key.getFullKey();
        CacheData<?> cacheData = cache.getIfPresent(cacheKey);

        if (cacheData == null) {
            return -1;
        }

        return cacheData.getRemainingTime();
    }

    @Override
    protected void doClear() throws Exception {
        cache.invalidateAll();
        keyMapping.clear();
    }

    @Override
    protected long doSize() throws Exception {
        return cache.estimatedSize();
    }

    @Override
    protected Set<CacheKey> doKeys() throws Exception {
        return new HashSet<>(keyMapping.values());
    }

    @Override
    protected Set<CacheKey> doKeys(String pattern) throws Exception {
        Set<CacheKey> result = new HashSet<>();
        for (Map.Entry<String, CacheKey> entry : keyMapping.entrySet()) {
            if (pattern == null || pattern.isEmpty() || entry.getKey().startsWith(pattern)) {
                result.add(entry.getValue());
            }
        }
        return result;
    }

    @Override
    protected Map<String, Object> getImplementationStatistics() {
        CacheStats stats = cache.stats();

        Map<String, Object> implStats = new HashMap<>();
        implStats.put("estimatedSize", cache.estimatedSize());
        implStats.put("hitCount", stats.hitCount());
        implStats.put("missCount", stats.missCount());
        implStats.put("loadSuccessCount", stats.loadSuccessCount());
        implStats.put("loadFailureCount", stats.loadFailureCount());
        implStats.put("totalLoadTime", stats.totalLoadTime());
        implStats.put("evictionCount", stats.evictionCount());
        implStats.put("evictionWeight", stats.evictionWeight());

        double hitRate = stats.hitCount() + stats.missCount() > 0 ?
                (double) stats.hitCount() / (stats.hitCount() + stats.missCount()) * 100 : 0.0;
        implStats.put("caffeineHitRate", String.format("%.2f%%", hitRate));

        double missRate = stats.hitCount() + stats.missCount() > 0 ?
                (double) stats.missCount() / (stats.hitCount() + stats.missCount()) * 100 : 0.0;
        implStats.put("caffeineMissRate", String.format("%.2f%%", missRate));

        return implStats;
    }

    /**
     * 获取缓存条目的详细信息
     */
    public Map<String, CacheData<?>> getAllEntries() {
        Map<String, CacheData<?>> entries = new HashMap<>();
        for (Map.Entry<String, CacheKey> entry : keyMapping.entrySet()) {
            CacheData<?> cacheData = cache.getIfPresent(entry.getKey());
            if (cacheData != null) {
                entries.put(entry.getKey(), cacheData);
            }
        }
        return entries;
    }

    /**
     * 获取缓存条目信息（只包含元数据）
     */
    public List<Map<String, Object>> getEntriesInfo() {
        List<Map<String, Object>> entries = new ArrayList<>();
        for (Map.Entry<String, CacheKey> entry : keyMapping.entrySet()) {
            CacheData<?> cacheData = cache.getIfPresent(entry.getKey());
            if (cacheData != null) {
                Map<String, Object> info = new HashMap<>();
                info.put("key", entry.getKey());
                info.put("cacheKey", cacheData.getKey());
                info.put("cacheTime", cacheData.getCacheTime());
                info.put("expireTime", cacheData.getExpireTime());
                info.put("remainingTime", cacheData.getRemainingTime());
                info.put("cacheLevel", cacheData.getCacheLevel());
                info.put("quality", cacheData.getQuality());
                info.put("expired", cacheData.isExpired());

                Object value = cacheData.getValue();
                if (value != null) {
                    info.put("valueType", value.getClass().getName());
                    info.put("valueSize", getValueSize(value));
                }

                entries.add(info);
            }
        }
        return entries;
    }

    /**
     * 估算值的大小
     */
    private long getValueSize(Object value) {
        if (value == null) {
            return 0;
        }

        if (value instanceof String) {
            return ((String) value).getBytes().length;
        } else if (value instanceof byte[]) {
            return ((byte[]) value).length;
        } else if (value instanceof Collection) {
            long size = 0;
            for (Object item : (Collection<?>) value) {
                size += getValueSize(item);
            }
            return size;
        } else if (value instanceof Map) {
            long size = 0;
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                size += getValueSize(entry.getKey());
                size += getValueSize(entry.getValue());
            }
            return size;
        }

        // 简单估算
        return 16; // 对象头的估算大小
    }

    /**
     * 手动清理过期条目
     */
    public void cleanup() {
        cache.cleanUp();
        log.debug("本地缓存清理完成，当前大小: {}", cache.estimatedSize());
    }

    /**
     * 获取Caffeine缓存实例
     */
    public Cache<String, CacheData<?>> getCache() {
        return cache;
    }
}
