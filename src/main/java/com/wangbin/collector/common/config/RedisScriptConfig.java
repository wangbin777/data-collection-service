package com.wangbin.collector.common.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.script.DefaultRedisScript;

@Configuration
public class RedisScriptConfig {

    /**
     * 分布式锁解锁脚本
     * 使用 Lua 脚本保证解锁的原子性
     */
    @Bean
    public DefaultRedisScript<Long> unlockScript() {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setScriptText(
                // 先检查锁的值是否匹配，再删除，保证原子性
                "if redis.call('get', KEYS[1]) == ARGV[1] then " +
                        "    return redis.call('del', KEYS[1]) " +
                        "else " +
                        "    return 0 " +
                        "end"
        );
        script.setResultType(Long.class);
        return script;
    }

    /**
     * 分布式锁加锁脚本（可选）
     */
    @Bean
    public DefaultRedisScript<Long> lockScript() {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setScriptText(
                // SET key value NX PX timeout
                "if redis.call('setnx', KEYS[1], ARGV[1]) == 1 then " +
                        "    redis.call('pexpire', KEYS[1], ARGV[2]) " +
                        "    return 1 " +
                        "else " +
                        "    return 0 " +
                        "end"
        );
        script.setResultType(Long.class);
        return script;
    }
}