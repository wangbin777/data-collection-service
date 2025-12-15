package com.wangbin.collector.core.cache.manager;

import com.wangbin.collector.core.cache.model.CacheKey;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.*;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Redis缓存管理器
 */
@Slf4j
@Component("redisCacheManager")
public class RedisCacheManager extends AbstractCacheManager {

    @Autowired
    @Qualifier("cacheRedisTemplate")
    private RedisTemplate<String, Object> redisTemplate;

    @Value("${collector.cache.redis.key-prefix:collector:}")
    private String keyPrefix;

    @Value("${collector.cache.redis.default-expire:3600}")
    private long defaultExpire; // 秒

    @Value("${collector.cache.redis.connection-timeout:3000}")
    private long connectionTimeout;

    public RedisCacheManager() {
        super("REDIS", 2);
    }

    @Override
    protected void doInit() throws Exception {
        // 测试Redis连接
        testConnection();

        log.info("Redis缓存管理器初始化完成: keyPrefix={}, defaultExpire={}s",
                keyPrefix, defaultExpire);
    }

    @Override
    protected void doDestroy() throws Exception {
        log.info("Redis缓存管理器已销毁");
    }

    @Override
    protected <T> boolean doPut(CacheKey key, T value, long expireTime) throws Exception {
        String redisKey = buildRedisKey(key);

        try {
            // 序列化值
            Object serializedValue = serializeValue(value);

            if (expireTime > 0) {
                // 带过期时间
                long expireSeconds = TimeUnit.MILLISECONDS.toSeconds(expireTime);
                redisTemplate.opsForValue().set(redisKey, serializedValue,
                        expireSeconds, TimeUnit.SECONDS);
            } else if (expireTime == CacheKey.EXPIRE_NEVER) {
                // 永不过期
                redisTemplate.opsForValue().set(redisKey, serializedValue);
            } else {
                // 使用默认过期时间
                redisTemplate.opsForValue().set(redisKey, serializedValue,
                        defaultExpire, TimeUnit.SECONDS);
            }

            return true;
        } catch (Exception e) {
            log.error("Redis缓存写入失败: key={}", redisKey, e);
            throw e;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <T> T doGet(CacheKey key) throws Exception {
        String redisKey = buildRedisKey(key);

        try {
            Object value = redisTemplate.opsForValue().get(redisKey);

            if (value == null) {
                return null;
            }

            // 反序列化值
            return (T) deserializeValue(value, key);
        } catch (Exception e) {
            log.error("Redis缓存读取失败: key={}", redisKey, e);
            throw e;
        }
    }

    @Override
    protected boolean doDelete(CacheKey key) throws Exception {
        String redisKey = buildRedisKey(key);

        try {
            Boolean deleted = redisTemplate.delete(redisKey);
            return deleted != null && deleted;
        } catch (Exception e) {
            log.error("Redis缓存删除失败: key={}", redisKey, e);
            throw e;
        }
    }

    @Override
    protected int doDeleteByPattern(String pattern) throws Exception {
        try {
            Set<String> keys = redisTemplate.keys(buildRedisPattern(pattern));
            if (keys == null || keys.isEmpty()) {
                return 0;
            }

            Long deleted = redisTemplate.delete(keys);
            return deleted != null ? deleted.intValue() : 0;
        } catch (Exception e) {
            log.error("Redis模式删除失败: pattern={}", pattern, e);
            throw e;
        }
    }

    @Override
    protected boolean doExists(CacheKey key) throws Exception {
        String redisKey = buildRedisKey(key);

        try {
            Boolean exists = redisTemplate.hasKey(redisKey);
            return exists != null && exists;
        } catch (Exception e) {
            log.error("Redis检查缓存存在失败: key={}", redisKey, e);
            throw e;
        }
    }

    @Override
    protected boolean doExpire(CacheKey key, long expireTime) throws Exception {
        String redisKey = buildRedisKey(key);

        try {
            if (expireTime > 0) {
                long expireSeconds = TimeUnit.MILLISECONDS.toSeconds(expireTime);
                Boolean success = redisTemplate.expire(redisKey, expireSeconds, TimeUnit.SECONDS);
                return success != null && success;
            } else if (expireTime == CacheKey.EXPIRE_NEVER) {
                // 移除过期时间
                Boolean success = redisTemplate.persist(redisKey);
                return success != null && success;
            } else {
                return false;
            }
        } catch (Exception e) {
            log.error("Redis设置缓存过期时间失败: key={}", redisKey, e);
            throw e;
        }
    }

    @Override
    protected long doGetExpire(CacheKey key) throws Exception {
        String redisKey = buildRedisKey(key);

        try {
            Long expireSeconds = redisTemplate.getExpire(redisKey, TimeUnit.SECONDS);

            if (expireSeconds == null) {
                return -1;
            }

            if (expireSeconds == -1) {
                return CacheKey.EXPIRE_NEVER;
            }

            if (expireSeconds == -2) {
                return -1; // 键不存在
            }

            return TimeUnit.SECONDS.toMillis(expireSeconds);
        } catch (Exception e) {
            log.error("Redis获取缓存过期时间失败: key={}", redisKey, e);
            throw e;
        }
    }

    @Override
    protected void doClear() throws Exception {
        try {
            String pattern = keyPrefix + "*";
            Set<String> keys = redisTemplate.keys(pattern);
            if (keys != null && !keys.isEmpty()) {
                redisTemplate.delete(keys);
            }
        } catch (Exception e) {
            log.error("Redis清空缓存失败", e);
            throw e;
        }
    }

    @Override
    protected long doSize() throws Exception {
        try {
            String pattern = keyPrefix + "*";
            Set<String> keys = redisTemplate.keys(pattern);
            return keys != null ? keys.size() : 0;
        } catch (Exception e) {
            log.error("Redis获取缓存大小失败", e);
            throw e;
        }
    }

    @Override
    protected Set<CacheKey> doKeys() throws Exception {
        return doKeys("*");
    }

    @Override
    protected Set<CacheKey> doKeys(String pattern) throws Exception {
        try {
            Set<String> redisKeys = redisTemplate.keys(buildRedisPattern(pattern));
            if (redisKeys == null || redisKeys.isEmpty()) {
                return Collections.emptySet();
            }

            Set<CacheKey> cacheKeys = new HashSet<>();
            for (String redisKey : redisKeys) {
                // 移除前缀，还原为原始key
                String originalKey = redisKey.substring(keyPrefix.length());
                CacheKey cacheKey = new CacheKey(originalKey, 0);
                cacheKeys.add(cacheKey);
            }

            return cacheKeys;
        } catch (Exception e) {
            log.error("Redis获取缓存键失败: pattern={}", pattern, e);
            throw e;
        }
    }

    @Override
    protected Map<String, Object> getImplementationStatistics() {
        try {
            Map<String, Object> stats = new HashMap<>();

            // 获取Redis信息

            try (RedisConnection connection = Objects.requireNonNull(
                    redisTemplate.getConnectionFactory()).getConnection()) {
                Properties info = connection.info();
                assert info != null;
                stats.put("redisVersion", info.getProperty("redis_version"));
                stats.put("usedMemory", info.getProperty("used_memory_human"));
                stats.put("connectedClients", info.getProperty("connected_clients"));
                stats.put("totalCommandsProcessed", info.getProperty("total_commands_processed"));
                stats.put("keyspaceHits", info.getProperty("keyspace_hits"));
                stats.put("keyspaceMisses", info.getProperty("keyspace_misses"));

                // 计算命中率
                long hits = Long.parseLong(info.getProperty("keyspace_hits", "0"));
                long misses = Long.parseLong(info.getProperty("keyspace_misses", "0"));
                long total = hits + misses;
                double hitRate = total > 0 ? (double) hits / total * 100 : 0.0;
                double missRate = total > 0 ? (double) misses / total * 100 : 0.0;

                stats.put("redisHitRate", String.format("%.2f%%", hitRate));
                stats.put("redisMissRate", String.format("%.2f%%", missRate));
            }

            return stats;
        } catch (Exception e) {
            log.error("获取Redis统计信息失败", e);
            return Collections.emptyMap();
        }
    }

    // =============== Redis特定方法 ===============

    /**
     * 批量获取缓存值
     */
    public <T> Map<String, T> batchGet(List<String> keys, Class<T> type) {
        if (keys == null || keys.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, T> result = new HashMap<>();
        for (String key : keys) {
            try {
                T value = (T) redisTemplate.opsForValue().get(buildRedisKey(new CacheKey(key)));
                if (value != null) {
                    result.put(key, value);
                }
            } catch (Exception e) {
                log.error("Redis批量获取失败: key={}", key, e);
            }
        }

        return result;
    }

    /**
     * 批量设置缓存值
     */
    public <T> boolean batchSet(Map<String, T> dataMap, long expireTime) {
        if (dataMap == null || dataMap.isEmpty()) {
            return true;
        }

        boolean allSuccess = true;
        for (Map.Entry<String, T> entry : dataMap.entrySet()) {
            try {
                boolean success = put(new CacheKey(entry.getKey()), entry.getValue(), expireTime);
                if (!success) {
                    allSuccess = false;
                    log.warn("Redis批量设置失败: key={}", entry.getKey());
                }
            } catch (Exception e) {
                allSuccess = false;
                log.error("Redis批量设置异常: key={}", entry.getKey(), e);
            }
        }

        return allSuccess;
    }

    /**
     * 使用Pipeline批量操作
     */
    public <T> List<T> pipelineGet(List<String> keys, Class<T> type) {
        if (keys == null || keys.isEmpty()) {
            return Collections.emptyList();
        }

        List<Object> results = redisTemplate.executePipelined(new SessionCallback<Object>() {
            @Override
            public <K, V> Object execute(RedisOperations<K, V> operations) {
                for (String key : keys) {
                    operations.opsForValue().get((K) buildRedisKey(new CacheKey(key)));
                }
                return null;
            }
        });

        List<T> typedResults = new ArrayList<>();
        for (Object result : results) {
            try {
                @SuppressWarnings("unchecked")
                T typedResult = (T) deserializeValue(result, null);
                typedResults.add(typedResult);
            } catch (Exception e) {
                typedResults.add(null);
                log.warn("Redis Pipeline结果类型转换失败", e);
            }
        }

        return typedResults;
    }

    /**
     * 使用Hash存储
     */
    public <T> boolean hashPut(String hashKey, String field, T value) {
        try {
            redisTemplate.opsForHash().put(hashKey, field, serializeValue(value));
            return true;
        } catch (Exception e) {
            log.error("Redis Hash写入失败: hashKey={}, field={}", hashKey, field, e);
            return false;
        }
    }

    /**
     * 从Hash获取
     */
    @SuppressWarnings("unchecked")
    public <T> T hashGet(String hashKey, String field, Class<T> type) {
        try {
            Object value = redisTemplate.opsForHash().get(hashKey, field);
            return value != null ? (T) deserializeValue(value, null) : null;
        } catch (Exception e) {
            log.error("Redis Hash读取失败: hashKey={}, field={}", hashKey, field, e);
            return null;
        }
    }

    /**
     * 获取整个Hash
     */
    public <T> Map<String, T> hashGetAll(String hashKey, Class<T> type) {
        try {
            Map<Object, Object> entries = redisTemplate.opsForHash().entries(hashKey);
            Map<String, T> result = new HashMap<>();

            for (Map.Entry<Object, Object> entry : entries.entrySet()) {
                try {
                    String field = entry.getKey().toString();
                    @SuppressWarnings("unchecked")
                    T value = (T) deserializeValue(entry.getValue(), null);
                    result.put(field, value);
                } catch (Exception e) {
                    log.warn("Redis Hash条目转换失败: hashKey={}", hashKey, e);
                }
            }

            return result;
        } catch (Exception e) {
            log.error("Redis Hash获取全部失败: hashKey={}", hashKey, e);
            return Collections.emptyMap();
        }
    }

    /**
     * 使用List存储
     */
    public <T> boolean listPush(String listKey, T value, boolean left) {
        try {
            if (left) {
                redisTemplate.opsForList().leftPush(listKey, serializeValue(value));
            } else {
                redisTemplate.opsForList().rightPush(listKey, serializeValue(value));
            }
            return true;
        } catch (Exception e) {
            log.error("Redis List写入失败: listKey={}", listKey, e);
            return false;
        }
    }

    /**
     * 从List获取
     */
    @SuppressWarnings("unchecked")
    public <T> T listPop(String listKey, boolean left, Class<T> type) {
        try {
            Object value;
            if (left) {
                value = redisTemplate.opsForList().leftPop(listKey);
            } else {
                value = redisTemplate.opsForList().rightPop(listKey);
            }
            return value != null ? (T) deserializeValue(value, null) : null;
        } catch (Exception e) {
            log.error("Redis List读取失败: listKey={}", listKey, e);
            return null;
        }
    }

    /**
     * 使用Set存储
     */
    public <T> boolean setAdd(String setKey, T value) {
        try {
            redisTemplate.opsForSet().add(setKey, serializeValue(value));
            return true;
        } catch (Exception e) {
            log.error("Redis Set写入失败: setKey={}", setKey, e);
            return false;
        }
    }

    /**
     * 从Set获取成员
     */
    public boolean setIsMember(String setKey, Object value) {
        try {
            return Boolean.TRUE.equals(
                    redisTemplate.opsForSet().isMember(setKey, serializeValue(value))
            );
        } catch (Exception e) {
            log.error("Redis Set检查成员失败: setKey={}", setKey, e);
            return false;
        }
    }

    /**
     * 获取Set所有成员
     */
    @SuppressWarnings("unchecked")
    public <T> Set<T> setMembers(String setKey, Class<T> type) {
        try {
            Set<Object> members = redisTemplate.opsForSet().members(setKey);
            Set<T> result = new HashSet<>();

            for (Object member : members) {
                try {
                    result.add((T) deserializeValue(member, null));
                } catch (Exception e) {
                    log.warn("Redis Set成员转换失败: setKey={}", setKey, e);
                }
            }

            return result;
        } catch (Exception e) {
            log.error("Redis Set获取成员失败: setKey={}", setKey, e);
            return Collections.emptySet();
        }
    }

    // =============== 辅助方法 ===============

    /**
     * 构建Redis键
     */
    private String buildRedisKey(CacheKey key) {
        return keyPrefix + key.getFullKey();
    }

    /**
     * 构建Redis模式
     */
    private String buildRedisPattern(String pattern) {
        if (pattern == null || pattern.isEmpty()) {
            return keyPrefix + "*";
        }
        return keyPrefix + pattern;
    }

    /**
     * 序列化值
     */
    private Object serializeValue(Object value) {
        // RedisTemplate已经配置了序列化器，这里直接返回
        return value;
    }

    /**
     * 反序列化值
     */
    private Object deserializeValue(Object value, CacheKey key) {
        // RedisTemplate已经配置了反序列化器，这里直接返回
        return value;
    }

    /**
     * 测试Redis连接
     */
    private void testConnection() throws Exception {
        try {
            String testKey = keyPrefix + "test:connection";
            redisTemplate.opsForValue().set(testKey, "test", 10, TimeUnit.SECONDS);
            Object result = redisTemplate.opsForValue().get(testKey);

            if (!"test".equals(result)) {
                throw new Exception("Redis连接测试失败: 返回值不匹配");
            }

            redisTemplate.delete(testKey);
            log.debug("Redis连接测试成功");
        } catch (Exception e) {
            log.error("Redis连接测试失败", e);
            throw new Exception("Redis连接失败: " + e.getMessage(), e);
        }
    }

    /**
     * 获取RedisTemplate
     */
    public RedisTemplate<String, Object> getRedisTemplate() {
        return redisTemplate;
    }
}
