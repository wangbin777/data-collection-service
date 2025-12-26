package com.wangbin.collector.core.cache.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 缓存异步配置
 * 配置异步任务执行器，用于异步保存缓存
 */
@Configuration
@EnableAsync
public class CacheAsyncConfig {

    /**
     * 创建缓存异步执行器
     */
    @Bean("cacheAsyncExecutor")
    public Executor cacheAsyncExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        
        // 核心线程数
        executor.setCorePoolSize(5);
        
        // 最大线程数
        executor.setMaxPoolSize(10);
        
        // 队列容量
        executor.setQueueCapacity(100);
        
        // 线程名称前缀
        executor.setThreadNamePrefix("CacheAsync-");
        
        // 线程空闲超时时间
        executor.setKeepAliveSeconds(60);
        
        // 拒绝策略：当线程池和队列都满时，使用调用线程执行
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        
        // 初始化线程池
        executor.initialize();
        
        return executor;
    }
}