package com.wangbin.collector.monitor.metrics;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.ThreadMXBean;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * JVM/系统资源监控服务。
 */
@Service
@RequiredArgsConstructor
public class SystemResourceMonitorService {

    private final BeanFactory beanFactory;

    private MemoryMXBean memoryMXBean;
    private ThreadMXBean threadMXBean;
    private OperatingSystemMXBean operatingSystemMXBean;

    @PostConstruct
    public void init() {
        memoryMXBean = ManagementFactory.getMemoryMXBean();
        threadMXBean = ManagementFactory.getThreadMXBean();
        operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();
    }

    public SystemResourceSnapshot getResources() {
        Map<String, SystemResourceSnapshot.ThreadPoolSnapshot> threadPools = collectThreadPoolStats();
        return SystemResourceSnapshot.builder()
                .heapUsed(memoryMXBean.getHeapMemoryUsage().getUsed())
                .heapCommitted(memoryMXBean.getHeapMemoryUsage().getCommitted())
                .heapMax(memoryMXBean.getHeapMemoryUsage().getMax())
                .nonHeapUsed(memoryMXBean.getNonHeapMemoryUsage().getUsed())
                .nonHeapCommitted(memoryMXBean.getNonHeapMemoryUsage().getCommitted())
                .processCpuLoad(cpuLoad("ProcessCpuLoad"))
                .systemCpuLoad(cpuLoad("SystemCpuLoad"))
                .threadCount(threadMXBean.getThreadCount())
                .daemonThreadCount(threadMXBean.getDaemonThreadCount())
                .threadPools(threadPools)
                .build();
    }

    private Map<String, SystemResourceSnapshot.ThreadPoolSnapshot> collectThreadPoolStats() {
        Map<String, SystemResourceSnapshot.ThreadPoolSnapshot> result = new LinkedHashMap<>();
        String[] poolBeanNames = new String[]{
                "collectionTaskExecutor",
                "dataProcessExecutor",
                "reportExecutor",
                "connectionExecutor"
        };
        for (String beanName : poolBeanNames) {
            if (!beanFactory.containsBean(beanName)) {
                continue;
            }
            ThreadPoolTaskExecutor executor = beanFactory.getBean(beanName, ThreadPoolTaskExecutor.class);
            result.put(beanName, SystemResourceSnapshot.ThreadPoolSnapshot.builder()
                    .corePoolSize(executor.getCorePoolSize())
                    .maxPoolSize(executor.getMaxPoolSize())
                    .activeCount(executor.getActiveCount())
                    .queueSize(executor.getThreadPoolExecutor().getQueue().size())
                    .completedTaskCount(executor.getThreadPoolExecutor().getCompletedTaskCount())
                    .build());
        }
        return result;
    }

    private double cpuLoad(String attribute) {
        try {
            java.lang.management.OperatingSystemMXBean platformBean = ManagementFactory.getPlatformMXBean(
                    java.lang.management.OperatingSystemMXBean.class);
            Object value = platformBean.getClass()
                    .getMethod("get" + attribute)
                    .invoke(platformBean);
            if (value instanceof Number number) {
                double load = number.doubleValue();
                if (load >= 0) {
                    return load * 100.0;
                }
            }
        } catch (Exception ignored) {
        }
        return -1.0;
    }
}
