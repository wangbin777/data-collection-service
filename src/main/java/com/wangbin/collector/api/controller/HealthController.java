package com.wangbin.collector.api.controller;

import com.wangbin.collector.monitor.health.HealthStatus;
import com.wangbin.collector.monitor.health.SystemHealthService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 系统健康检查接口。
 */
@RestController
@RequiredArgsConstructor
public class HealthController {

    private final SystemHealthService systemHealthService;

    @GetMapping("/health")
    public HealthStatus health() {
        return systemHealthService.getSystemHealth();
    }
}
