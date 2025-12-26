package com.wangbin.collector.core.cache.manager;

import com.wangbin.collector.core.cache.model.CacheData;
import com.wangbin.collector.core.cache.model.CacheKey;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 多级缓存管理器
 */
@Slf4j
@Component("multiLevelCacheManager")
public class MultiLevelCacheManager implements CacheManager {

    @Autowired
    @Qualifier("localCacheManager")
    private LocalCacheManager localCacheManager;

    @Autowired
    @Qualifier("redisCacheManager")
    private RedisCacheManager redisCacheManager;

    // 缓存管理器列表（按层级排序）
    private final List<CacheManager> cacheManagers = new CopyOnWriteArrayList<>();

    // 配置
    private boolean enabled = true;
    private boolean writeThrough = true; // 写穿透
    private boolean readThrough = true;  // 读穿透
    private boolean cacheAside = true;   // 旁路缓存
    private int maxLevel = 2;

    // 统计信息
    private AtomicLong totalReads = new AtomicLong(0);
    private AtomicLong totalWrites = new AtomicLong(0);
    private AtomicLong totalDeletes = new AtomicLong(0);
    private AtomicLong level1Hits = new AtomicLong(0);
    private AtomicLong level2Hits = new AtomicLong(0);
    private AtomicLong totalMisses = new AtomicLong(0);

    // 异步执行器
    private ExecutorService asyncExecutor;

    // 缓存同步锁（防止缓存击穿）
    private final Map<String, Object> cacheLocks = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        if (!enabled) {
            log.warn("多级缓存管理器已禁用");
            return;
        }

        // 初始化缓存管理器
        cacheManagers.add(localCacheManager);  // Level 1
        cacheManagers.add(redisCacheManager);  // Level 2

        // 按缓存层级排序
        cacheManagers.sort(Comparator.comparingInt(CacheManager::getCacheLevel));

        // 初始化所有缓存管理器
        for (CacheManager manager : cacheManagers) {
            try {
                manager.init();
                log.info("缓存管理器初始化完成: {} [Level: {}]",
                        manager.getCacheType(), manager.getCacheLevel());
            } catch (Exception e) {
                log.error("缓存管理器初始化失败: {}", manager.getCacheType(), e);
            }
        }

        // 初始化异步执行器
        asyncExecutor = Executors.newFixedThreadPool(
                Runtime.getRuntime().availableProcessors(),
                r -> {
                    Thread thread = new Thread(r);
                    thread.setName("cache-async-" + thread.getId());
                    thread.setDaemon(true);
                    return thread;
                }
        );

        log.info("多级缓存管理器初始化完成，层级数: {}", cacheManagers.size());
    }

    @PreDestroy
    public void destroy() {
        if (!enabled) {
            return;
        }

        // 关闭异步执行器
        if (asyncExecutor != null) {
            asyncExecutor.shutdown();
        }

        // 销毁所有缓存管理器
        for (CacheManager manager : cacheManagers) {
            try {
                manager.destroy();
            } catch (Exception e) {
                log.error("缓存管理器销毁失败: {}", manager.getCacheType(), e);
            }
        }

        cacheManagers.clear();
        cacheLocks.clear();

        log.info("多级缓存管理器已销毁");
    }

    @Override
    public <T> boolean put(CacheKey key, T value) {
        return put(key, value, key.getExpireTime());
    }

    @Override
    public <T> boolean put(CacheKey key, T value, long expireTime) {
        if (!enabled || key == null || value == null) {
            return false;
        }

        // 数据变化检测：检查新值与现有缓存是否相同，如果相同则跳过更新
        T existingValue = get(key);
        if (existingValue != null && existingValue.equals(value)) {
            log.debug("缓存数据未变化，跳过更新: key={}", key);
            // 只更新过期时间
            expire(key, expireTime);
            return true;
        }

        totalWrites.incrementAndGet();
        String lockKey = key.getFullKey();

        synchronized (getCacheLock(lockKey)) {
            try {
                boolean allSuccess = true;

                // 根据策略写入各级缓存
                if (writeThrough) {
                    // 写穿透：从高级别向低级别写入
                    for (int i = cacheManagers.size() - 1; i >= 0; i--) {
                        CacheManager manager = cacheManagers.get(i);
                        boolean success = manager.put(key, value, expireTime);
                        if (!success) {
                            allSuccess = false;
                            log.warn("缓存写入失败: {} [Level: {}]",
                                    manager.getCacheType(), manager.getCacheLevel());
                        }
                    }
                } else if (cacheAside) {
                    // 旁路缓存：只写入高级别缓存
                    CacheManager primaryManager = getPrimaryCacheManager(key);
                    boolean success = primaryManager.put(key, value, expireTime);
                    if (!success) {
                        allSuccess = false;
                        log.warn("主缓存写入失败: {}", primaryManager.getCacheType());
                    }

                    // 异步清除低级别缓存
                    asyncRemoveLowerLevels(key, primaryManager.getCacheLevel());
                } else {
                    // 只写入本地缓存
                    boolean success = localCacheManager.put(key, value, expireTime);
                    if (!success) {
                        allSuccess = false;
                        log.warn("本地缓存写入失败");
                    }
                }

                log.debug("多级缓存写入完成: key={}, levels={}, success={}",
                        key, cacheManagers.size(), allSuccess);

                return allSuccess;
            } finally {
                releaseCacheLock(lockKey);
            }
        }
    }

    @Override
    public <T> boolean putAll(Map<CacheKey, T> dataMap) {
        if (!enabled || dataMap == null || dataMap.isEmpty()) {
            return true;
        }

        boolean allSuccess = true;
        int updatedCount = 0;

        for (Map.Entry<CacheKey, T> entry : dataMap.entrySet()) {
            CacheKey key = entry.getKey();
            T value = entry.getValue();

            // 数据变化检测：检查新值与现有缓存是否相同，如果相同则跳过更新
            T existingValue = get(key);
            if (existingValue != null && existingValue.equals(value)) {
                log.debug("缓存数据未变化，跳过更新: key={}", key);
                // 只更新过期时间
                expire(key, key.getExpireTime());
                continue;
            }

            updatedCount++;
            boolean success = put(key, value);
            if (!success) {
                allSuccess = false;
            }
        }

        log.debug("批量缓存写入完成: 总数={}, 更新={}, 跳过={}, 全部成功={}",
                dataMap.size(), updatedCount, dataMap.size() - updatedCount, allSuccess);

        return allSuccess;
    }

    @Override
    public <T> T get(CacheKey key) {
        return get(key, null);
    }

    @Override
    public <T> T get(CacheKey key, Class<T> type) {
        if (!enabled || key == null) {
            return null;
        }

        totalReads.incrementAndGet();
        String lockKey = key.getFullKey();

        synchronized (getCacheLock(lockKey)) {
            try {
                // 逐级查找
                T value = null;
                int hitLevel = -1;

                for (CacheManager manager : cacheManagers) {
                    if (manager.getCacheLevel() > maxLevel) {
                        continue;
                    }

                    T foundValue = manager.get(key, type);
                    if (foundValue != null) {
                        value = foundValue;
                        hitLevel = manager.getCacheLevel();

                        // 更新命中统计
                        updateHitStatistics(hitLevel);

                        // 如果读穿透开启且不是最高级别命中，则更新低级别缓存
                        if (readThrough && hitLevel > 1) {
                            asyncUpdateLowerLevels(key, value, hitLevel);
                        }

                        break;
                    }
                }

                if (value == null) {
                    totalMisses.incrementAndGet();
                    log.debug("多级缓存未命中: key={}", key);

                    // 缓存未命中，可以在这里加载数据
                    // value = loadData(key);
                    // if (value != null && cacheAside) {
                    //     asyncExecutor.submit(() -> put(key, value));
                    // }
                } else {
                    log.debug("多级缓存命中: key={}, level={}", key, hitLevel);
                }

                return value;
            } finally {
                releaseCacheLock(lockKey);
            }
        }
    }

    @Override
    public <T> CacheData<T> getWithMetadata(CacheKey key) {
        T value = get(key);
        if (value == null) {
            return null;
        }

        // 从本地缓存获取元数据（因为本地缓存速度最快）
        CacheData<T> cacheData = localCacheManager.getWithMetadata(key);
        if (cacheData == null) {
            cacheData = new CacheData<>();
            cacheData.setKey(key);
            cacheData.setValue(value);
            cacheData.setCacheTime(System.currentTimeMillis());
        }

        return cacheData;
    }

    @Override
    public <T> Map<CacheKey, T> getAll(List<CacheKey> keys) {
        if (!enabled || keys == null || keys.isEmpty()) {
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
        if (!enabled || keys == null || keys.isEmpty()) {
            return Collections.emptyList();
        }

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
        if (!enabled || key == null) {
            return false;
        }

        totalDeletes.incrementAndGet();
        String lockKey = key.getFullKey();

        synchronized (getCacheLock(lockKey)) {
            try {
                boolean allSuccess = true;

                // 删除所有级别的缓存
                for (CacheManager manager : cacheManagers) {
                    boolean success = manager.delete(key);
                    if (!success) {
                        allSuccess = false;
                        log.warn("缓存删除失败: {} [Level: {}]",
                                manager.getCacheType(), manager.getCacheLevel());
                    }
                }

                return allSuccess;
            } finally {
                releaseCacheLock(lockKey);
            }
        }
    }

    @Override
    public boolean deleteAll(List<CacheKey> keys) {
        if (!enabled || keys == null || keys.isEmpty()) {
            return false;
        }

        boolean allSuccess = true;
        for (CacheKey key : keys) {
            boolean success = delete(key);
            if (!success) {
                allSuccess = false;
                log.warn("批量缓存删除失败: key={}", key);
            }
        }

        return allSuccess;
    }

    @Override
    public boolean deleteByPattern(String pattern) {
        if (!enabled || pattern == null || pattern.isEmpty()) {
            return false;
        }

        boolean allSuccess = true;
        for (CacheManager manager : cacheManagers) {
            boolean success = manager.deleteByPattern(pattern);
            if (!success) {
                allSuccess = false;
                log.warn("模式删除缓存失败: {} [Level: {}]",
                        manager.getCacheType(), manager.getCacheLevel());
            }
        }

        return allSuccess;
    }

    @Override
    public boolean exists(CacheKey key) {
        if (!enabled || key == null) {
            return false;
        }

        // 检查所有级别，任一存在即返回true
        for (CacheManager manager : cacheManagers) {
            if (manager.exists(key)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean expire(CacheKey key, long expireTime) {
        if (!enabled || key == null || expireTime <= 0) {
            return false;
        }

        boolean allSuccess = true;
        for (CacheManager manager : cacheManagers) {
            boolean success = manager.expire(key, expireTime);
            if (!success) {
                allSuccess = false;
                log.warn("设置缓存过期时间失败: {} [Level: {}]",
                        manager.getCacheType(), manager.getCacheLevel());
            }
        }

        return allSuccess;
    }

    @Override
    public long getExpire(CacheKey key) {
        if (!enabled || key == null) {
            return -1;
        }

        // 返回最低级别（最快）的过期时间
        for (CacheManager manager : cacheManagers) {
            long expire = manager.getExpire(key);
            if (expire != -1) {
                return expire;
            }
        }

        return -1;
    }

    @Override
    public void clear() {
        if (!enabled) {
            return;
        }

        for (CacheManager manager : cacheManagers) {
            try {
                manager.clear();
                log.info("缓存清空完成: {}", manager.getCacheType());
            } catch (Exception e) {
                log.error("缓存清空失败: {}", manager.getCacheType(), e);
            }
        }
    }

    @Override
    public long size() {
        if (!enabled) {
            return 0;
        }

        // 返回主缓存的大小
        CacheManager primaryManager = getPrimaryCacheManager(null);
        return primaryManager != null ? primaryManager.size() : 0;
    }

    @Override
    public Set<CacheKey> keys() {
        if (!enabled) {
            return Collections.emptySet();
        }

        // 返回主缓存的键集合
        CacheManager primaryManager = getPrimaryCacheManager(null);
        return primaryManager != null ? primaryManager.keys() : Collections.emptySet();
    }

    @Override
    public Set<CacheKey> keys(String pattern) {
        if (!enabled) {
            return Collections.emptySet();
        }

        // 返回主缓存的键集合
        CacheManager primaryManager = getPrimaryCacheManager(null);
        return primaryManager != null ? primaryManager.keys(pattern) : Collections.emptySet();
    }

    @Override
    public Map<String, Object> getStatistics() {
        Map<String, Object> stats = new HashMap<>();

        stats.put("enabled", enabled);
        stats.put("writeThrough", writeThrough);
        stats.put("readThrough", readThrough);
        stats.put("cacheAside", cacheAside);
        stats.put("maxLevel", maxLevel);

        stats.put("totalReads", totalReads.get());
        stats.put("totalWrites", totalWrites.get());
        stats.put("totalDeletes", totalDeletes.get());
        stats.put("level1Hits", level1Hits.get());
        stats.put("level2Hits", level2Hits.get());
        stats.put("totalMisses", totalMisses.get());

        long totalHits = level1Hits.get() + level2Hits.get();
        long totalAccess = totalReads.get();
        double totalHitRate = totalAccess > 0 ?
                (double) totalHits / totalAccess * 100 : 0.0;
        double level1HitRate = totalHits > 0 ?
                (double) level1Hits.get() / totalHits * 100 : 0.0;
        double level2HitRate = totalHits > 0 ?
                (double) level2Hits.get() / totalHits * 100 : 0.0;
        double missRate = totalAccess > 0 ?
                (double) totalMisses.get() / totalAccess * 100 : 0.0;

        stats.put("totalHitRate", String.format("%.2f%%", totalHitRate));
        stats.put("level1HitRate", String.format("%.2f%%", level1HitRate));
        stats.put("level2HitRate", String.format("%.2f%%", level2HitRate));
        stats.put("missRate", String.format("%.2f%%", missRate));
        stats.put("totalAccess", totalAccess);

        // 收集各级缓存的统计信息
        Map<String, Map<String, Object>> levelStats = new HashMap<>();
        for (CacheManager manager : cacheManagers) {
            levelStats.put(manager.getCacheType(), manager.getStatistics());
        }
        stats.put("levelStatistics", levelStats);

        return stats;
    }

    @Override
    public void resetStatistics() {
        totalReads.set(0);
        totalWrites.set(0);
        totalDeletes.set(0);
        level1Hits.set(0);
        level2Hits.set(0);
        totalMisses.set(0);

        for (CacheManager manager : cacheManagers) {
            manager.resetStatistics();
        }

        log.info("多级缓存统计重置完成");
    }

    @Override
    public int getCacheLevel() {
        return 0; // 多级缓存管理器的级别为0
    }

    @Override
    public String getCacheType() {
        return "MULTI_LEVEL_CACHE";
    }

    // =============== 缓存策略方法 ===============

    /**
     * 根据键获取主缓存管理器
     */
    private CacheManager getPrimaryCacheManager(CacheKey key) {
        // 默认返回最高级别的缓存管理器
        for (int i = cacheManagers.size() - 1; i >= 0; i--) {
            CacheManager manager = cacheManagers.get(i);
            if (manager.getCacheLevel() <= maxLevel) {
                return manager;
            }
        }
        return cacheManagers.isEmpty() ? null : cacheManagers.get(0);
    }

    /**
     * 更新命中统计
     */
    private void updateHitStatistics(int hitLevel) {
        switch (hitLevel) {
            case 1:
                level1Hits.incrementAndGet();
                break;
            case 2:
                level2Hits.incrementAndGet();
                break;
            default:
                log.debug("未知的缓存层级命中: level={}", hitLevel);
                break;
        }
    }

    /**
     * 异步更新低级别缓存
     */
    private <T> void asyncUpdateLowerLevels(CacheKey key, T value, int currentLevel) {
        if (!readThrough || currentLevel <= 1) {
            return;
        }

        asyncExecutor.submit(() -> {
            try {
                for (CacheManager manager : cacheManagers) {
                    if (manager.getCacheLevel() < currentLevel) {
                        manager.put(key, value);
                        log.debug("缓存回写完成: key={}, level={} -> {}",
                                key, currentLevel, manager.getCacheLevel());
                    }
                }
            } catch (Exception e) {
                log.error("缓存回写失败: key={}", key, e);
            }
        });
    }

    /**
     * 异步移除低级别缓存
     */
    private void asyncRemoveLowerLevels(CacheKey key, int currentLevel) {
        if (currentLevel <= 1) {
            return;
        }

        asyncExecutor.submit(() -> {
            try {
                for (CacheManager manager : cacheManagers) {
                    if (manager.getCacheLevel() < currentLevel) {
                        manager.delete(key);
                        log.debug("缓存清除完成: key={}, level={}",
                                key, manager.getCacheLevel());
                    }
                }
            } catch (Exception e) {
                log.error("缓存清除失败: key={}", key, e);
            }
        });
    }

    /**
     * 获取缓存锁（防止缓存击穿）
     */
    private Object getCacheLock(String key) {
        return cacheLocks.computeIfAbsent(key, k -> new Object());
    }

    /**
     * 释放缓存锁
     */
    private void releaseCacheLock(String key) {
        cacheLocks.remove(key);
    }

    // =============== 公共方法 ===============

    /**
     * 预热缓存
     */
    public <T> void warmUp(CacheKey key, T value) {
        if (!enabled || key == null || value == null) {
            return;
        }

        log.info("开始预热缓存: key={}", key);

        // 直接写入所有级别的缓存
        for (CacheManager manager : cacheManagers) {
            try {
                manager.put(key, value);
                log.debug("缓存预热完成: {} [Level: {}]",
                        manager.getCacheType(), manager.getCacheLevel());
            } catch (Exception e) {
                log.error("缓存预热失败: {}", manager.getCacheType(), e);
            }
        }

        log.info("缓存预热完成: key={}", key);
    }

    /**
     * 批量预热缓存
     */
    public <T> void warmUpAll(Map<CacheKey, T> dataMap) {
        if (!enabled || dataMap == null || dataMap.isEmpty()) {
            return;
        }

        log.info("开始批量预热缓存，数量: {}", dataMap.size());

        int successCount = 0;
        for (Map.Entry<CacheKey, T> entry : dataMap.entrySet()) {
            try {
                warmUp(entry.getKey(), entry.getValue());
                successCount++;
            } catch (Exception e) {
                log.error("批量缓存预热失败: key={}", entry.getKey(), e);
            }
        }

        log.info("批量缓存预热完成: 总数={}, 成功={}", dataMap.size(), successCount);
    }

    /**
     * 刷新缓存
     */
    public <T> boolean refresh(CacheKey key, T newValue) {
        if (!enabled || key == null) {
            return false;
        }

        log.info("开始刷新缓存: key={}", key);

        // 先删除，再写入
        delete(key);
        boolean success = put(key, newValue);

        log.info("缓存刷新完成: key={}, success={}", key, success);
        return success;
    }

    /**
     * 获取缓存健康状态
     */
    public Map<String, Object> getHealthStatus() {
        Map<String, Object> health = new HashMap<>();

        health.put("enabled", enabled);
        health.put("totalLevels", cacheManagers.size());
        health.put("maxLevel", maxLevel);

        // 检查各级缓存状态
        List<Map<String, Object>> levelHealth = new ArrayList<>();
        for (CacheManager manager : cacheManagers) {
            Map<String, Object> levelStatus = new HashMap<>();
            levelStatus.put("type", manager.getCacheType());
            levelStatus.put("level", manager.getCacheLevel());
            levelStatus.put("size", manager.size());

            try {
                // 简单测试连接/状态
                if (manager instanceof LocalCacheManager) {
                    levelStatus.put("status", "HEALTHY");
                } else if (manager instanceof RedisCacheManager) {
                    // Redis连接测试
                    RedisCacheManager redisManager = (RedisCacheManager) manager;
                    try {
                        redisManager.getRedisTemplate().opsForValue().get("health:test");
                        levelStatus.put("status", "HEALTHY");
                    } catch (Exception e) {
                        levelStatus.put("status", "UNHEALTHY");
                        levelStatus.put("error", e.getMessage());
                    }
                } else {
                    levelStatus.put("status", "UNKNOWN");
                }
            } catch (Exception e) {
                levelStatus.put("status", "ERROR");
                levelStatus.put("error", e.getMessage());
            }

            levelHealth.add(levelStatus);
        }

        health.put("levels", levelHealth);

        // 总体状态
        boolean allHealthy = levelHealth.stream()
                .allMatch(level -> "HEALTHY".equals(level.get("status")));
        health.put("overallStatus", allHealthy ? "HEALTHY" : "DEGRADED");

        return health;
    }

    /**
     * 获取缓存性能报告
     */
    public String getPerformanceReport() {
        Map<String, Object> stats = getStatistics();

        StringBuilder report = new StringBuilder();
        report.append("=== 多级缓存性能报告 ===\n");
        report.append("总访问次数: ").append(stats.get("totalAccess")).append("\n");
        report.append("总命中率: ").append(stats.get("totalHitRate")).append("\n");
        report.append("一级缓存命中率: ").append(stats.get("level1HitRate")).append("\n");
        report.append("二级缓存命中率: ").append(stats.get("level2HitRate")).append("\n");
        report.append("未命中率: ").append(stats.get("missRate")).append("\n");
        report.append("总写入次数: ").append(stats.get("totalWrites")).append("\n");
        report.append("总删除次数: ").append(stats.get("totalDeletes")).append("\n");

        @SuppressWarnings("unchecked")
        Map<String, Map<String, Object>> levelStats =
                (Map<String, Map<String, Object>>) stats.get("levelStatistics");

        if (levelStats != null) {
            report.append("\n=== 各级缓存详情 ===\n");
            for (Map.Entry<String, Map<String, Object>> entry : levelStats.entrySet()) {
                report.append("缓存类型: ").append(entry.getKey()).append("\n");
                Map<String, Object> levelStat = entry.getValue();
                for (Map.Entry<String, Object> statEntry : levelStat.entrySet()) {
                    report.append("  ").append(statEntry.getKey()).append(": ")
                            .append(statEntry.getValue()).append("\n");
                }
                report.append("\n");
            }
        }

        return report.toString();
    }

    /**
     * 设置缓存策略
     */
    public void setCacheStrategy(boolean writeThrough, boolean readThrough, boolean cacheAside) {
        this.writeThrough = writeThrough;
        this.readThrough = readThrough;
        this.cacheAside = cacheAside;
        log.info("缓存策略设置完成: writeThrough={}, readThrough={}, cacheAside={}",
                writeThrough, readThrough, cacheAside);
    }

    /**
     * 设置最大缓存层级
     */
    public void setMaxLevel(int maxLevel) {
        this.maxLevel = maxLevel;
        log.info("最大缓存层级设置完成: {}", maxLevel);
    }

    /**
     * 启用/禁用缓存
     */
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
        log.info("缓存管理器已{}", enabled ? "启用" : "禁用");
    }
}