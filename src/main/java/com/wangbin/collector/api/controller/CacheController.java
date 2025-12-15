package com.wangbin.collector.api.controller;

import com.wangbin.collector.core.cache.manager.MultiLevelCacheManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/cache")
public class CacheController {

    @Autowired
    @Qualifier("multiLevelCacheManager")
    private MultiLevelCacheManager cacheManager;

    @GetMapping("/stats")
    public Map<String, Object> getStats() {
        return cacheManager.getStatistics();
    }

    @GetMapping("/health")
    public Map<String, Object> getHealth() {
        return cacheManager.getHealthStatus();
    }
}
