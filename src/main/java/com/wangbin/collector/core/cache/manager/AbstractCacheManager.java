package com.wangbin.collector.core.cache.manager;

import com.wangbin.collector.core.cache.model.CacheData;
import com.wangbin.collector.core.cache.model.CacheKey;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 抽象缓存管理器
 */
@Slf4j
public abstract class AbstractCacheManager implements CacheManager {

    protected final String cacheType;
    protected final int cacheLevel;
    protected volatile boolean initialized = false;

    // 统计信息
    protected AtomicLong totalPuts = new AtomicLong(0);
    protected AtomicLong totalGets = new AtomicLong(0);
    protected AtomicLong totalHits = new AtomicLong(0);
    protected AtomicLong totalMisses = new AtomicLong(0);
    protected AtomicLong totalDeletes = new AtomicLong(0);
    protected AtomicLong totalExpires = new AtomicLong(0);
    protected AtomicLong totalErrors = new AtomicLong(0);

    // 缓存统计锁
    protected final Object statsLock = new Object();

    public AbstractCacheManager(String cacheType, int cacheLevel) {
        this.cacheType = cacheType;
        this.cacheLevel = cacheLevel;
    }

    @Override
    public void init() {
        if (initialized) {
            log.warn("缓存管理器已经初始化: {}", cacheType);
            return;
        }

        try {
            doInit();
            initialized = true;
            log.info("缓存管理器初始化完成: {} [Level: {}]", cacheType, cacheLevel);
        } catch (Exception e) {
            log.error("缓存管理器初始化失败: {}", cacheType, e);
            throw new RuntimeException("缓存管理器初始化失败: " + cacheType, e);
        }
    }

    @Override
    public void destroy() {
        if (!initialized) {
            return;
        }

        try {
            doDestroy();
            initialized = false;
            log.info("缓存管理器销毁完成: {}", cacheType);
        } catch (Exception e) {
            log.error("缓存管理器销毁失败: {}", cacheType, e);
        }
    }

    @Override
    public <T> boolean put(CacheKey key, T value) {
        return put(key, value, key.getExpireTime());
    }

    @Override
    public <T> boolean put(CacheKey key, T value, long expireTime) {
        checkInitialized();

        if (key == null || value == null) {
            log.warn("缓存键或值为空，跳过缓存: {}", key);
            return false;
        }

        long startTime = System.currentTimeMillis();
        try {
            boolean success = doPut(key, value, expireTime);

            if (success) {
                totalPuts.incrementAndGet();
                log.debug("缓存写入成功: {} -> {}", key, getValueInfo(value));
            } else {
                totalErrors.incrementAndGet();
                log.warn("缓存写入失败: {}", key);
            }

            return success;
        } catch (Exception e) {
            totalErrors.incrementAndGet();
            log.error("缓存写入异常: {}", key, e);
            return false;
        } finally {
            long cost = System.currentTimeMillis() - startTime;
            if (cost > 100) {
                log.warn("缓存写入耗时过长: {}ms, key: {}", cost, key);
            }
        }
    }

    @Override
    public <T> boolean putAll(Map<CacheKey, T> dataMap) {
        checkInitialized();

        if (dataMap == null || dataMap.isEmpty()) {
            return true;
        }

        long startTime = System.currentTimeMillis();
        int successCount = 0;

        try {
            for (Map.Entry<CacheKey, T> entry : dataMap.entrySet()) {
                boolean success = put(entry.getKey(), entry.getValue());
                if (success) {
                    successCount++;
                }
            }

            log.debug("批量缓存写入完成: 总数={}, 成功={}", dataMap.size(), successCount);
            return successCount == dataMap.size();
        } finally {
            long cost = System.currentTimeMillis() - startTime;
            if (cost > 500) {
                log.warn("批量缓存写入耗时过长: {}ms, 数量={}", cost, dataMap.size());
            }
        }
    }

    @Override
    public <T> T get(CacheKey key) {
        checkInitialized();

        if (key == null) {
            return null;
        }

        long startTime = System.currentTimeMillis();
        try {
            T value = doGet(key);

            totalGets.incrementAndGet();
            if (value != null) {
                totalHits.incrementAndGet();
                log.debug("缓存命中: {} -> {}", key, getValueInfo(value));
            } else {
                totalMisses.incrementAndGet();
                log.debug("缓存未命中: {}", key);
            }

            return value;
        } catch (Exception e) {
            totalErrors.incrementAndGet();
            log.error("缓存读取异常: {}", key, e);
            return null;
        } finally {
            long cost = System.currentTimeMillis() - startTime;
            if (cost > 50) {
                log.warn("缓存读取耗时过长: {}ms, key: {}", cost, key);
            }
        }
    }

    @Override
    public <T> T get(CacheKey key, Class<T> type) {
        T value = get(key);
        if (value != null && type != null && !type.isInstance(value)) {
            log.warn("缓存值类型不匹配: 期望={}, 实际={}",
                    type.getName(), value.getClass().getName());
            return null;
        }
        return value;
    }

    @Override
    public <T> CacheData<T> getWithMetadata(CacheKey key) {
        T value = get(key);
        if (value == null) {
            return null;
        }

        CacheData<T> cacheData = new CacheData<>();
        cacheData.setKey(key);
        cacheData.setValue(value);
        cacheData.setCacheTime(System.currentTimeMillis());
        cacheData.setCacheLevel(cacheLevel);
        cacheData.setCacheType(cacheType);

        // 获取过期时间
        long expire = getExpire(key);
        cacheData.setExpireTime(expire);
        cacheData.setRemainingTime(expire > 0 ? expire - System.currentTimeMillis() : -1);

        return cacheData;
    }

    @Override
    public <T> Map<CacheKey, T> getAll(List<CacheKey> keys) {
        checkInitialized();

        if (keys == null || keys.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<CacheKey, T> result = new HashMap<>();
        for (CacheKey key : keys) {
            T value = get(key);
            if (value != null) {
                result.put(key, value);
            }
        }

        return result;
    }

    @Override
    public <T> List<CacheData<T>> getListWithMetadata(List<CacheKey> keys) {
        List<CacheData<T>> result = new ArrayList<>();
        for (CacheKey key : keys) {
            CacheData<T> data = getWithMetadata(key);
            if (data != null) {
                result.add(data);
            }
        }
        return result;
    }

    @Override
    public boolean delete(CacheKey key) {
        checkInitialized();

        if (key == null) {
            return false;
        }

        long startTime = System.currentTimeMillis();
        try {
            boolean success = doDelete(key);

            if (success) {
                totalDeletes.incrementAndGet();
                log.debug("缓存删除成功: {}", key);
            } else {
                log.debug("缓存删除失败或不存在: {}", key);
            }

            return success;
        } catch (Exception e) {
            totalErrors.incrementAndGet();
            log.error("缓存删除异常: {}", key, e);
            return false;
        } finally {
            long cost = System.currentTimeMillis() - startTime;
            if (cost > 50) {
                log.warn("缓存删除耗时过长: {}ms, key: {}", cost, key);
            }
        }
    }

    @Override
    public boolean deleteAll(List<CacheKey> keys) {
        checkInitialized();

        if (keys == null || keys.isEmpty()) {
            return true;
        }

        long startTime = System.currentTimeMillis();
        int successCount = 0;

        try {
            for (CacheKey key : keys) {
                boolean success = delete(key);
                if (success) {
                    successCount++;
                }
            }

            log.debug("批量缓存删除完成: 总数={}, 成功={}", keys.size(), successCount);
            return successCount == keys.size();
        } finally {
            long cost = System.currentTimeMillis() - startTime;
            if (cost > 500) {
                log.warn("批量缓存删除耗时过长: {}ms, 数量={}", cost, keys.size());
            }
        }
    }

    @Override
    public boolean deleteByPattern(String pattern) {
        checkInitialized();

        if (pattern == null || pattern.isEmpty()) {
            return false;
        }

        long startTime = System.currentTimeMillis();
        try {
            int deletedCount = doDeleteByPattern(pattern);

            if (deletedCount > 0) {
                totalDeletes.addAndGet(deletedCount);
                log.debug("模式删除缓存成功: {}, 数量={}", pattern, deletedCount);
                return true;
            } else {
                log.debug("模式删除缓存未找到匹配项: {}", pattern);
                return false;
            }
        } catch (Exception e) {
            totalErrors.incrementAndGet();
            log.error("模式删除缓存异常: {}", pattern, e);
            return false;
        } finally {
            long cost = System.currentTimeMillis() - startTime;
            if (cost > 1000) {
                log.warn("模式删除缓存耗时过长: {}ms, pattern: {}", cost, pattern);
            }
        }
    }

    @Override
    public boolean exists(CacheKey key) {
        checkInitialized();

        if (key == null) {
            return false;
        }

        try {
            return doExists(key);
        } catch (Exception e) {
            totalErrors.incrementAndGet();
            log.error("检查缓存存在异常: {}", key, e);
            return false;
        }
    }

    @Override
    public boolean expire(CacheKey key, long expireTime) {
        checkInitialized();

        if (key == null || expireTime <= 0) {
            return false;
        }

        long startTime = System.currentTimeMillis();
        try {
            boolean success = doExpire(key, expireTime);

            if (success) {
                totalExpires.incrementAndGet();
                log.debug("缓存过期时间设置成功: {}, expireTime={}", key, expireTime);
            } else {
                log.debug("缓存过期时间设置失败: {}", key);
            }

            return success;
        } catch (Exception e) {
            totalErrors.incrementAndGet();
            log.error("设置缓存过期时间异常: {}", key, e);
            return false;
        } finally {
            long cost = System.currentTimeMillis() - startTime;
            if (cost > 50) {
                log.warn("设置缓存过期时间耗时过长: {}ms, key: {}", cost, key);
            }
        }
    }

    @Override
    public long getExpire(CacheKey key) {
        checkInitialized();

        if (key == null) {
            return -1;
        }

        try {
            return doGetExpire(key);
        } catch (Exception e) {
            totalErrors.incrementAndGet();
            log.error("获取缓存过期时间异常: {}", key, e);
            return -1;
        }
    }

    @Override
    public void clear() {
        checkInitialized();

        long startTime = System.currentTimeMillis();
        try {
            doClear();
            log.info("缓存清空完成: {}", cacheType);
        } catch (Exception e) {
            totalErrors.incrementAndGet();
            log.error("清空缓存异常", e);
        } finally {
            long cost = System.currentTimeMillis() - startTime;
            if (cost > 1000) {
                log.warn("清空缓存耗时过长: {}ms", cost);
            }
        }
    }

    @Override
    public long size() {
        checkInitialized();

        try {
            return doSize();
        } catch (Exception e) {
            totalErrors.incrementAndGet();
            log.error("获取缓存大小异常", e);
            return 0;
        }
    }

    @Override
    public Set<CacheKey> keys() {
        checkInitialized();

        try {
            return doKeys();
        } catch (Exception e) {
            totalErrors.incrementAndGet();
            log.error("获取缓存键集合异常", e);
            return Collections.emptySet();
        }
    }

    @Override
    public Set<CacheKey> keys(String pattern) {
        checkInitialized();

        if (pattern == null || pattern.isEmpty()) {
            return keys();
        }

        try {
            return doKeys(pattern);
        } catch (Exception e) {
            totalErrors.incrementAndGet();
            log.error("获取模式匹配缓存键异常: {}", pattern, e);
            return Collections.emptySet();
        }
    }

    @Override
    public Map<String, Object> getStatistics() {
        synchronized (statsLock) {
            Map<String, Object> stats = new HashMap<>();

            stats.put("cacheType", cacheType);
            stats.put("cacheLevel", cacheLevel);
            stats.put("initialized", initialized);
            stats.put("totalPuts", totalPuts.get());
            stats.put("totalGets", totalGets.get());
            stats.put("totalHits", totalHits.get());
            stats.put("totalMisses", totalMisses.get());
            stats.put("totalDeletes", totalDeletes.get());
            stats.put("totalExpires", totalExpires.get());
            stats.put("totalErrors", totalErrors.get());

            long totalAccess = totalGets.get();
            double hitRate = totalAccess > 0 ?
                    (double) totalHits.get() / totalAccess * 100 : 0.0;
            double missRate = totalAccess > 0 ?
                    (double) totalMisses.get() / totalAccess * 100 : 0.0;

            stats.put("hitRate", String.format("%.2f%%", hitRate));
            stats.put("missRate", String.format("%.2f%%", missRate));
            stats.put("cacheSize", size());

            // 添加具体实现的统计信息
            Map<String, Object> implStats = getImplementationStatistics();
            if (implStats != null) {
                stats.putAll(implStats);
            }

            return stats;
        }
    }

    @Override
    public void resetStatistics() {
        synchronized (statsLock) {
            totalPuts.set(0);
            totalGets.set(0);
            totalHits.set(0);
            totalMisses.set(0);
            totalDeletes.set(0);
            totalExpires.set(0);
            totalErrors.set(0);
            log.info("缓存统计重置完成: {}", cacheType);
        }
    }

    @Override
    public int getCacheLevel() {
        return cacheLevel;
    }

    @Override
    public String getCacheType() {
        return cacheType;
    }

    // =============== 抽象方法 ===============

    protected abstract void doInit() throws Exception;
    protected abstract void doDestroy() throws Exception;
    protected abstract <T> boolean doPut(CacheKey key, T value, long expireTime) throws Exception;
    protected abstract <T> T doGet(CacheKey key) throws Exception;
    protected abstract boolean doDelete(CacheKey key) throws Exception;
    protected abstract int doDeleteByPattern(String pattern) throws Exception;
    protected abstract boolean doExists(CacheKey key) throws Exception;
    protected abstract boolean doExpire(CacheKey key, long expireTime) throws Exception;
    protected abstract long doGetExpire(CacheKey key) throws Exception;
    protected abstract void doClear() throws Exception;
    protected abstract long doSize() throws Exception;
    protected abstract Set<CacheKey> doKeys() throws Exception;
    protected abstract Set<CacheKey> doKeys(String pattern) throws Exception;

    /**
     * 获取实现特定的统计信息
     */
    protected Map<String, Object> getImplementationStatistics() {
        return null;
    }

    // =============== 辅助方法 ===============

    /**
     * 检查是否初始化
     */
    protected void checkInitialized() {
        if (!initialized) {
            throw new IllegalStateException("缓存管理器未初始化: " + cacheType);
        }
    }

    /**
     * 获取值的信息（用于日志）
     */
    protected String getValueInfo(Object value) {
        if (value == null) {
            return "null";
        }

        if (value instanceof String) {
            String str = (String) value;
            return str.length() > 50 ? str.substring(0, 50) + "..." : str;
        } else if (value instanceof byte[]) {
            byte[] bytes = (byte[]) value;
            return "bytes[" + bytes.length + "]";
        } else if (value instanceof Collection) {
            return "collection[" + ((Collection<?>) value).size() + "]";
        } else if (value instanceof Map) {
            return "map[" + ((Map<?, ?>) value).size() + "]";
        } else {
            return value.toString();
        }
    }

    /**
     * 生成缓存统计报告
     */
    public String getStatisticsReport() {
        Map<String, Object> stats = getStatistics();
        StringBuilder report = new StringBuilder();
        report.append("=== 缓存统计报告 ===\n");
        report.append("缓存类型: ").append(stats.get("cacheType")).append("\n");
        report.append("缓存层级: ").append(stats.get("cacheLevel")).append("\n");
        report.append("缓存大小: ").append(stats.get("cacheSize")).append("\n");
        report.append("总写入次数: ").append(stats.get("totalPuts")).append("\n");
        report.append("总读取次数: ").append(stats.get("totalGets")).append("\n");
        report.append("命中次数: ").append(stats.get("totalHits")).append("\n");
        report.append("未命中次数: ").append(stats.get("totalMisses")).append("\n");
        report.append("命中率: ").append(stats.get("hitRate")).append("\n");
        report.append("未命中率: ").append(stats.get("missRate")).append("\n");
        report.append("总删除次数: ").append(stats.get("totalDeletes")).append("\n");
        report.append("总错误次数: ").append(stats.get("totalErrors")).append("\n");
        return report.toString();
    }
}
