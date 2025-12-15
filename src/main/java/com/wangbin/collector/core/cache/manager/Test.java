package com.wangbin.collector.core.cache.manager;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

public class Test {

    /*
    直接使用多级缓存
    @Autowired
    @Qualifier("cacheManager")
    private CacheManager cacheManager;

    public User getUserById(Long userId) {
        CacheKey key = new CacheKey("user:" + userId, 3600000); // 1小时
        User user = cacheManager.get(key, User.class);

        if (user == null) {
            user = userRepository.findById(userId).orElse(null);
            if (user != null) {
                cacheManager.put(key, user);
            }
        }

        return user;
    }*/

/*  使用预热功能
    @Component
    public class CacheWarmUp {

        @Autowired
        @Qualifier("cacheManager")
        private MultiLevelCacheManager cacheManager;

        @PostConstruct
        public void warmUp() {
            Map<CacheKey, Object> warmData = new HashMap<>();
            warmData.put(new CacheKey("system:config", CacheKey.EXPIRE_NEVER), loadSystemConfig());
            warmData.put(new CacheKey("constants:all", 86400000), loadConstants()); // 24小时

            cacheManager.warmUpAll(warmData);
        }
    }*/



}
