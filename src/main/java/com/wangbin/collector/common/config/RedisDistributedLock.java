package com.wangbin.collector.common.config;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class RedisDistributedLock implements DistributedLock {

    private final RedisTemplate<String, Object> redisTemplate;
    private final DefaultRedisScript<Long> unlockScript;
    private final ThreadLocal<Map<String, String>> lockHolder = ThreadLocal.withInitial(HashMap::new);

    public RedisDistributedLock(
            RedisTemplate<String, Object> redisTemplate,
            DefaultRedisScript<Long> unlockScript) {
        this.redisTemplate = redisTemplate;
        this.unlockScript = unlockScript;
    }

    @Override
    public boolean tryLock(String lockKey, long expireTime, TimeUnit timeUnit) {
        String lockValue = UUID.randomUUID().toString();

        Boolean success = redisTemplate.opsForValue().setIfAbsent(
                lockKey,
                lockValue,
                expireTime,
                timeUnit
        );

        if (Boolean.TRUE.equals(success)) {
            Map<String, String> locks = lockHolder.get();
            locks.put(lockKey, lockValue);
            return true;
        }

        return false;
    }

    @Override
    public boolean unlock(String lockKey) {
        Map<String, String> locks = lockHolder.get();
        if (!locks.containsKey(lockKey)) {
            return false;
        }

        String lockValue = locks.get(lockKey);

        Long result = redisTemplate.execute(
                unlockScript,
                Collections.singletonList(lockKey),
                lockValue
        );

        if (result != null && result > 0) {
            locks.remove(lockKey);
            if (locks.isEmpty()) {
                lockHolder.remove();
            }
            return true;
        }

        return false;
    }

    @Override
    public boolean isLocked(String lockKey) {
        Boolean hasKey = redisTemplate.hasKey(lockKey);
        return Boolean.TRUE.equals(hasKey);
    }
}