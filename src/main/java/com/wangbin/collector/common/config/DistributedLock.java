package com.wangbin.collector.common.config;

import java.util.concurrent.TimeUnit;

/**
 * 分布式锁接口
 */
public interface DistributedLock {

    /**
     * 尝试获取锁
     * @param lockKey 锁key
     * @param expireTime 过期时间
     * @param timeUnit 时间单位
     * @return 是否获取成功
     */
    boolean tryLock(String lockKey, long expireTime, TimeUnit timeUnit);

    /**
     * 释放锁
     * @param lockKey 锁key
     * @return 是否释放成功
     */
    boolean unlock(String lockKey);

    /**
     * 检查锁是否被持有
     * @param lockKey 锁key
     * @return 是否被持有
     */
    boolean isLocked(String lockKey);
}