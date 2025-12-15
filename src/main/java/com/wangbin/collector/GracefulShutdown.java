package com.wangbin.collector;

import org.apache.catalina.connector.Connector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.web.embedded.tomcat.TomcatConnectorCustomizer;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class GracefulShutdown implements TomcatConnectorCustomizer,
        ApplicationListener<ContextClosedEvent> {

    private static final Logger log = LoggerFactory.getLogger(GracefulShutdown.class);
    private static final int TIMEOUT = 30;

    private volatile Connector connector;

    @Override
    public void customize(Connector connector) {
        this.connector = connector;
    }

    @Override
    public void onApplicationEvent(ContextClosedEvent event) {
        if (this.connector == null) {
            return;
        }

        this.connector.pause();

        Executor executor = this.connector.getProtocolHandler().getExecutor();
        if (executor instanceof ThreadPoolExecutor threadPoolExecutor) {

            try {
                log.info("开始优雅停机，等待活动请求完成...");

                // 停止接收新请求
                threadPoolExecutor.shutdown();

                // 等待现有请求完成
                if (!threadPoolExecutor.awaitTermination(TIMEOUT, TimeUnit.SECONDS)) {
                    log.warn("等待超时，强制关闭线程池");
                    threadPoolExecutor.shutdownNow();

                    // 再次等待
                    if (!threadPoolExecutor.awaitTermination(TIMEOUT, TimeUnit.SECONDS)) {
                        log.error("线程池无法正常关闭");
                    }
                }

                // 等待其他服务停止
                stopOtherServices();

                log.info("优雅停机完成");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("优雅停机被中断", e);
                threadPoolExecutor.shutdownNow();
            }
        }
    }

    private void stopOtherServices() {
        // 停止采集任务
        stopCollectionTasks();

        // 关闭所有连接
        closeAllConnections();

        // 刷新缓存数据
        flushCacheData();

        // 保存当前状态
        saveCurrentState();
    }

    private void stopCollectionTasks() {
        try {
            // 实现停止采集任务的逻辑
            log.info("停止所有采集任务...");
            // collectionManager.stopAllTasks();
        } catch (Exception e) {
            log.error("停止采集任务失败", e);
        }
    }

    private void closeAllConnections() {
        try {
            log.info("关闭所有连接...");
            // connectionManager.closeAllConnections();
        } catch (Exception e) {
            log.error("关闭连接失败", e);
        }
    }

    private void flushCacheData() {
        try {
            log.info("刷新缓存数据...");
            // cacheManager.flushAll();
        } catch (Exception e) {
            log.error("刷新缓存失败", e);
        }
    }

    private void saveCurrentState() {
        try {
            log.info("保存当前状态...");
            // stateManager.saveState();
        } catch (Exception e) {
            log.error("保存状态失败", e);
        }
    }
}