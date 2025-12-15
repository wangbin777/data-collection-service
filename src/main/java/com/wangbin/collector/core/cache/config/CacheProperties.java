package com.wangbin.collector.core.cache.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties; /**
 * 缓存配置属性
 */
@ConfigurationProperties(prefix = "collector.cache")
@Data
public class CacheProperties {

    private LocalCache local = new LocalCache();
    private RedisCache redis = new RedisCache();


    @Data
    public static class LocalCache {
        private long maxSize = 10000;
        private long expireAfterWrite = 300; // 秒
        private long expireAfterAccess = 60; // 秒
        private int initialCapacity = 1000;
        // getters and setters...
    }

    @Data
    public static class RedisCache {
        private String keyPrefix = "collector:";
        private long defaultExpire = 3600; // 秒
        private long connectionTimeout = 3000; // 毫秒
    }
}
