package com.wangbin.collector.common.config;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.util.concurrent.*;

@Configuration
public class ThreadPoolConfig {

    private final int cpuCores = Runtime.getRuntime().availableProcessors();

    private ThreadFactory buildNamedThreadFactory(String prefix, boolean daemon) {
        return new ThreadFactoryBuilder()
                .setNameFormat(prefix + "-%d")
                .setDaemon(daemon)
                .setPriority(Thread.NORM_PRIORITY)
                .build();
    }

    /**
     * 时间片调度线程池（调度器核心任务）
     */
    @Bean(name = "timeSliceScheduler", destroyMethod = "shutdown")
    public ScheduledExecutorService timeSliceScheduler() {
        int poolSize = Math.max(2, cpuCores / 4);
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(
                poolSize,
                buildNamedThreadFactory("time-slice-scheduler", true)
        );
        executor.setRemoveOnCancelPolicy(true);
        return executor;
    }

    /**
     * 批量任务分发线程池
     */
    @Bean(name = "batchDispatcherExecutor", destroyMethod = "shutdown")
    public ThreadPoolExecutor batchDispatcherExecutor() {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                cpuCores,
                cpuCores * 2,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(1000),
                buildNamedThreadFactory("batch-dispatcher", true)
        );
        return executor;
    }

    /**
     * 异步采集线程池（IO密集型）
     */
    @Bean(name = "asyncCollectorExecutor", destroyMethod = "shutdown")
    public ThreadPoolExecutor asyncCollectorExecutor() {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                cpuCores * 4,
                cpuCores * 8,
                30L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(10000),
                buildNamedThreadFactory("async-collector", true)
        );
        return executor;
    }

    /**
     * 数据处理线程池（CPU密集型）
     */
    @Bean(name = "dataProcessorExecutor", destroyMethod = "shutdown")
    public ThreadPoolExecutor dataProcessorExecutor() {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                cpuCores,
                cpuCores * 2,
                30L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(5000),
                buildNamedThreadFactory("data-processor", true)
        );
        return executor;
    }

    /**
     * 采集任务线程池
     */
    @Bean("collectionTaskExecutor")
    public Executor collectionTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        // 核心线程数
        executor.setCorePoolSize(20);
        // 最大线程数
        executor.setMaxPoolSize(100);
        // 队列容量
        executor.setQueueCapacity(1000);
        // 线程活跃时间（秒）
        executor.setKeepAliveSeconds(60);
        // 线程名前缀
        executor.setThreadNamePrefix("collection-task-");
        // 拒绝策略：由调用线程处理
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        // 等待所有任务结束后再关闭线程池
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(60);
        executor.initialize();
        return executor;
    }

    /**
     * 数据处理线程池
     */
    @Bean("dataProcessExecutor")
    public Executor dataProcessExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(10);
        executor.setMaxPoolSize(50);
        executor.setQueueCapacity(2000);
        executor.setKeepAliveSeconds(60);
        executor.setThreadNamePrefix("data-process-");
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.DiscardOldestPolicy());
        executor.initialize();
        return executor;
    }

    /**
     * 数据上报线程池
     */
    @Bean("reportExecutor")
    public Executor reportExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(10);
        executor.setMaxPoolSize(30);
        executor.setQueueCapacity(5000);
        executor.setKeepAliveSeconds(60);
        executor.setThreadNamePrefix("report-task-");
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.DiscardPolicy());
        executor.initialize();
        return executor;
    }

    /**
     * 连接管理线程池
     */
    @Bean("connectionExecutor")
    public Executor connectionExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(5);
        executor.setMaxPoolSize(20);
        executor.setQueueCapacity(500);
        executor.setKeepAliveSeconds(60);
        executor.setThreadNamePrefix("connection-");
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
        executor.initialize();
        return executor;
    }

    /**
     * 定时任务线程池
     */
    @Bean("taskScheduler")
    public ThreadPoolTaskScheduler taskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(10);
        scheduler.setThreadNamePrefix("scheduled-task-");
        scheduler.setWaitForTasksToCompleteOnShutdown(true);
        scheduler.setAwaitTerminationSeconds(30);
        return scheduler;
    }

    /**
     * 监控线程池
     */
    @Bean("monitorExecutor")
    public ScheduledExecutorService monitorExecutor() {
        return Executors.newScheduledThreadPool(
                2,
                new ThreadFactoryBuilder()
                        .setNameFormat("monitor-thread-%d")
                        .setDaemon(true)
                        .build()
        );
    }

    /**
     * 通用IO密集型任务线程池
     */
    @Bean("ioIntensiveExecutor")
    public ExecutorService ioIntensiveExecutor() {
        int corePoolSize = Runtime.getRuntime().availableProcessors() * 2;
        int maxPoolSize = corePoolSize * 4;

        return new ThreadPoolExecutor(
                corePoolSize,
                maxPoolSize,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(1000),
                new ThreadFactoryBuilder()
                        .setNameFormat("io-task-%d")
                        .build(),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
    }

    /**
     * 通用CPU密集型任务线程池
     */
    @Bean("cpuIntensiveExecutor")
    public ExecutorService cpuIntensiveExecutor() {
        int corePoolSize = Runtime.getRuntime().availableProcessors();

        return new ThreadPoolExecutor(
                corePoolSize,
                corePoolSize,
                0L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(1000),
                new ThreadFactoryBuilder()
                        .setNameFormat("cpu-task-%d")
                        .build(),
                new ThreadPoolExecutor.AbortPolicy()
        );
    }
}
