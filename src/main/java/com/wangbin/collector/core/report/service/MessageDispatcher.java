package com.wangbin.collector.core.report.service;

import com.wangbin.collector.core.report.model.ReportData;
import com.wangbin.collector.core.report.model.ReportResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.*;

/**
 * 消息分发器 - 用于异步消息分发
 */
@Slf4j
@Service
public class MessageDispatcher {

    private ExecutorService dispatchExecutor;
    private Map<String, Queue<ReportData>> messageQueues = new ConcurrentHashMap<>();
    private ScheduledExecutorService flushExecutor;

    @PostConstruct
    public void init() {
        log.info("初始化消息分发器...");

        // 初始化分发线程池
        int corePoolSize = Runtime.getRuntime().availableProcessors();
        dispatchExecutor = new ThreadPoolExecutor(
                corePoolSize,
                corePoolSize * 2,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(1000),
                new ThreadFactory() {
                    private int counter = 0;
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread thread = new Thread(r, "dispatch-" + (counter++));
                        thread.setDaemon(true);
                        return thread;
                    }
                });

        // 初始化定时刷新任务
        flushExecutor = Executors.newSingleThreadScheduledExecutor();
        flushExecutor.scheduleAtFixedRate(this::flushAllQueues,
                5, 5, TimeUnit.SECONDS);

        log.info("消息分发器初始化完成");
    }

    /**
     * 分发单条消息
     */
    public CompletableFuture<ReportResult> dispatch(ReportData data,
                                                    ReportManager reportManager,
                                                    Map<String, Object> reportConfig) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                // 这里调用ReportManager进行实际上报
                // 简化实现，实际需要构建ReportConfig
                return reportManager.report(data, null);
            } catch (Exception e) {
                log.error("消息分发失败: {}", data.getPointCode(), e);
                return ReportResult.error(data.getPointCode(), e.getMessage(), "dispatch");
            }
        }, dispatchExecutor);
    }

    /**
     * 批量分发消息
     */
    public List<CompletableFuture<ReportResult>> batchDispatch(List<ReportData> dataList,
                                                               ReportManager reportManager,
                                                               Map<String, Object> reportConfig) {
        List<CompletableFuture<ReportResult>> futures = new ArrayList<>();

        for (ReportData data : dataList) {
            futures.add(dispatch(data, reportManager, reportConfig));
        }

        return futures;
    }

    /**
     * 异步入队消息
     */
    public void enqueue(String queueName, ReportData data) {
        Queue<ReportData> queue = messageQueues.computeIfAbsent(queueName,
                k -> new ConcurrentLinkedQueue<>());
        queue.offer(data);
        log.debug("消息入队: {} -> {}", data.getPointCode(), queueName);
    }

    /**
     * 批量入队消息
     */
    public void batchEnqueue(String queueName, List<ReportData> dataList) {
        Queue<ReportData> queue = messageQueues.computeIfAbsent(queueName,
                k -> new ConcurrentLinkedQueue<>());

        for (ReportData data : dataList) {
            queue.offer(data);
        }

        log.debug("批量消息入队: {} -> {}条", queueName, dataList.size());
    }

    /**
     * 刷新指定队列
     */
    public List<ReportResult> flushQueue(String queueName,
                                         ReportManager reportManager,
                                         Map<String, Object> reportConfig) {
        Queue<ReportData> queue = messageQueues.get(queueName);
        if (queue == null || queue.isEmpty()) {
            return Collections.emptyList();
        }

        List<ReportData> dataList = new ArrayList<>();
        ReportData data;
        int batchSize = 50;

        while ((data = queue.poll()) != null && dataList.size() < batchSize) {
            dataList.add(data);
        }

        if (!dataList.isEmpty()) {
            log.debug("刷新队列: {}，处理{}条消息", queueName, dataList.size());
            return reportManager.batchReport(dataList, null); // 简化
        }

        return Collections.emptyList();
    }

    /**
     * 刷新所有队列
     */
    private void flushAllQueues() {
        for (String queueName : messageQueues.keySet()) {
            try {
                Queue<ReportData> queue = messageQueues.get(queueName);
                if (queue != null && queue.size() >= 10) { // 队列达到一定数量才刷新
                    log.debug("定时刷新队列: {}，大小: {}", queueName, queue.size());
                    // 这里可以调用实际的刷新逻辑
                }
            } catch (Exception e) {
                log.error("刷新队列失败: {}", queueName, e);
            }
        }
    }

    /**
     * 获取队列统计信息
     */
    public Map<String, Integer> getQueueStats() {
        Map<String, Integer> stats = new HashMap<>();
        for (Map.Entry<String, Queue<ReportData>> entry : messageQueues.entrySet()) {
            stats.put(entry.getKey(), entry.getValue().size());
        }
        return stats;
    }

    @PreDestroy
    public void destroy() {
        log.info("销毁消息分发器...");

        // 关闭线程池
        if (dispatchExecutor != null) {
            dispatchExecutor.shutdown();
            try {
                if (!dispatchExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                    dispatchExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                dispatchExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        // 关闭定时任务
        if (flushExecutor != null) {
            flushExecutor.shutdown();
            try {
                if (!flushExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    flushExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                flushExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        // 清空队列
        messageQueues.clear();

        log.info("消息分发器销毁完成");
    }
}