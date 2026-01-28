package com.wangbin.collector.api.controller;

import com.wangbin.collector.monitor.metrics.*;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * 监控相关接口。
 */
@RestController
@RequestMapping("/monitor")
@RequiredArgsConstructor
public class MonitorController {

    private final CacheMonitorService cacheMonitorService;
    private final DeviceMonitorService deviceMonitorService;
    private final PerformanceMonitorService performanceMonitorService;
    private final SystemResourceMonitorService systemResourceMonitorService;
    private final ExceptionMonitorService exceptionMonitorService;

    @GetMapping("/cache")
    public CacheMetricsSnapshot cacheMetrics() {
        return cacheMonitorService.getCacheMetrics();
    }

    @GetMapping("/devices")
    public DeviceStatusSnapshot deviceStatus() {
        return deviceMonitorService.getDeviceStatus();
    }

    @GetMapping("/performance")
    public List<CollectorMetrics> collectorPerformance() {
        return performanceMonitorService.getCollectorMetrics();
    }

    @GetMapping("/system")
    public SystemResourceSnapshot systemResources() {
        return systemResourceMonitorService.getResources();
    }


    /**
     * MonitorController.exceptionStats() 只是把 ExceptionMonitorService.getStats() 的快照直接返回，没有额外处理，所以接口里有没有错误信息完全取决于服务内部是否曾记录异常（src/main/java/com/wangbin/collector/api/controller/
     *     MonitorController.java:24-47）。
     *   - ExceptionMonitorService.record(...) 只有在调用方显式上报异常时才会把分类统计、设备统计和 recent 列表更新；如果一直没人调用，它的计数器会保持 0，recent 也会是空集合（src/main/java/com/wangbin/collector/monitor/metrics/
     *     ExceptionMonitorService.java:33-69）。
     *   - 目前触发记录的入口主要有两类：
     *       1. 连接相关：在创建设备连接、连接/断开/重连、收发数据或心跳检测异常时，ConnectionManager 会调用 recordException(deviceId, e)，最终进到监控服务（src/main/java/com/wangbin/collector/core/connection/manager/
     *          ConnectionManager.java:106-215,368-541）。只有这些流程抛出了异常，/monitor/errors 才能看到对应的 deviceId/category。
     *       2. 采集流程：所有 BaseCollector 的子类在连接、读写点位、批量读写、订阅、执行命令等环节出错时，都会调用受保护的 recordException(e, point)，带上当前设备和点位 ID（src/main/java/com/wangbin/collector/core/collector/protocol/
     *          base/BaseCollector.java:101-505,756-763）。
     *   - 如果你环境里“错误信息一直没有”，通常只有两种情况：要么这些连接/采集逻辑还没真正跑过或一直正常，没有抛异常；要么某些组件没有注入 ExceptionMonitorService（虽然被声明为 @Service，但注入点都标了 @Autowired(required = false)，如
     *     果被裁剪掉这个 bean，记录逻辑会被直接跳过）。只要触发上述任一异常流程，接口就会立刻展示统计与最近异常明细。
     * @return
     */
    @GetMapping("/errors")
    public ExceptionStatsSnapshot exceptionStats() {
        return exceptionMonitorService.getStats();
    }
}
