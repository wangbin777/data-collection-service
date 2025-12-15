package com.wangbin.collector.core.report.service;

import com.wangbin.collector.common.constant.ProtocolConstant;
import com.wangbin.collector.core.report.handler.*;
import com.wangbin.collector.core.report.model.ReportConfig;
import com.wangbin.collector.core.report.model.ReportData;
import com.wangbin.collector.core.report.model.ReportResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.*;

/**
 * 上报管理器 - 支持多种协议
 */
@Slf4j
@Service
public class ReportManager {

    private final Map<String, ReportHandler> handlers = new ConcurrentHashMap<>();
    private final IoTProtocolService protocolService;
    private ExecutorService reportExecutor;

    @Autowired
    public ReportManager(IoTProtocolService protocolService) {
        this.protocolService = protocolService;
    }

    @PostConstruct
    public void init() {
        log.info("初始化上报管理器...");

        // 初始化线程池
        int poolSize = Math.max(2, Runtime.getRuntime().availableProcessors());
        reportExecutor = Executors.newFixedThreadPool(poolSize,
                new ThreadFactory() {
                    private int counter = 0;
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread thread = new Thread(r, "report-thread-" + (counter++));
                        thread.setDaemon(true);
                        return thread;
                    }
                });

        // 注册处理器
        registerHandlers();

        log.info("上报管理器初始化完成，支持协议: {}", handlers.keySet());
    }

    private void registerHandlers() {
        try {
            // HTTP处理器
            if (shouldRegisterHandler(ProtocolConstant.PROTOCOL_HTTP)) {
                HttpReportHandler httpHandler = new HttpReportHandler();
                httpHandler.init();
                handlers.put(ProtocolConstant.PROTOCOL_HTTP, httpHandler);
                log.info("注册HTTP上报处理器");
            }

            // MQTT处理器
            if (shouldRegisterHandler(ProtocolConstant.PROTOCOL_MQTT)) {
                MqttReportHandler mqttHandler = new MqttReportHandler();
                mqttHandler.init();
                handlers.put(ProtocolConstant.PROTOCOL_MQTT, mqttHandler);
                log.info("注册MQTT上报处理器");
            }

            // TCP处理器（物联网协议）
            if (shouldRegisterHandler(ProtocolConstant.PROTOCOL_TCP)) {
                TcpReportHandler tcpHandler = new TcpReportHandler(protocolService);
                tcpHandler.init();
                handlers.put(ProtocolConstant.PROTOCOL_TCP, tcpHandler);
                log.info("注册TCP上报处理器（支持物联网协议）");
            }

            // WebSocket处理器
            if (shouldRegisterHandler(ProtocolConstant.PROTOCOL_WEBSOCKET)) {
                log.info("WebSocket处理器暂未实现");
            }

        } catch (Exception e) {
            log.error("注册上报处理器失败", e);
        }
    }

    private boolean shouldRegisterHandler(String protocol) {
        // 这里可以根据配置决定是否注册某个处理器
        // 简化实现，默认全部注册
        return true;
    }

    /**
     * 异步上报数据
     */
    public CompletableFuture<ReportResult> reportAsync(ReportData data, ReportConfig config) {
        return CompletableFuture.supplyAsync(() -> report(data, config), reportExecutor);
    }

    /**
     * 同步上报数据
     */
    public ReportResult report(ReportData data, ReportConfig config) {
        if (data == null || config == null) {
            return ReportResult.error(
                    data != null ? data.getPointCode() : "unknown",
                    "参数不能为空",
                    config != null ? config.getTargetId() : "unknown"
            );
        }

        if (!config.validate()) {
            return ReportResult.error(data.getPointCode(),
                    "上报配置无效", config.getTargetId());
        }

        try {
            ReportHandler handler = getHandler(config.getProtocol());
            if (handler == null) {
                return ReportResult.error(data.getPointCode(),
                        "不支持的上报协议: " + config.getProtocol(), config.getTargetId());
            }

            return handler.report(data, config);

        } catch (Exception e) {
            log.error("上报数据失败: {} -> {}", data.getPointCode(), config.getTargetId(), e);
            return ReportResult.error(data.getPointCode(),
                    "上报异常: " + e.getMessage(), config.getTargetId());
        }
    }

    /**
     * 批量上报
     */
    public List<ReportResult> batchReport(List<ReportData> dataList, ReportConfig config) {
        if (dataList == null || dataList.isEmpty() || config == null) {
            return Collections.emptyList();
        }

        if (!config.validate()) {
            List<ReportResult> errors = new ArrayList<>();
            for (ReportData data : dataList) {
                errors.add(ReportResult.error(data.getPointCode(),
                        "上报配置无效", config.getTargetId()));
            }
            return errors;
        }

        try {
            ReportHandler handler = getHandler(config.getProtocol());
            if (handler == null) {
                List<ReportResult> errors = new ArrayList<>();
                for (ReportData data : dataList) {
                    errors.add(ReportResult.error(data.getPointCode(),
                            "不支持的上报协议: " + config.getProtocol(), config.getTargetId()));
                }
                return errors;
            }

            return handler.batchReport(dataList, config);

        } catch (Exception e) {
            log.error("批量上报失败: {}", config.getTargetId(), e);
            List<ReportResult> errors = new ArrayList<>();
            for (ReportData data : dataList) {
                errors.add(ReportResult.error(data.getPointCode(),
                        "批量上报异常: " + e.getMessage(), config.getTargetId()));
            }
            return errors;
        }
    }

    /**
     * 根据上报模式选择上报方式
     */
    public List<ReportResult> reportWithMode(List<ReportData> dataList, ReportConfig config, String reportMode) {
        if (reportMode == null) {
            reportMode = ProtocolConstant.REPORT_MODE_REALTIME;
        }

        switch (reportMode.toUpperCase()) {
            case ProtocolConstant.REPORT_MODE_REALTIME:
                // 实时上报，逐个上报
                List<ReportResult> results = new ArrayList<>();
                for (ReportData data : dataList) {
                    results.add(report(data, config));
                }
                return results;

            case ProtocolConstant.REPORT_MODE_BATCH:
                // 批量上报
                return batchReport(dataList, config);

            case ProtocolConstant.REPORT_MODE_COMPRESSED:
                // 压缩模式上报
                return reportCompressed(dataList, config);

            default:
                return batchReport(dataList, config);
        }
    }

    /**
     * 压缩模式上报（数据压缩后上报）
     */
    private List<ReportResult> reportCompressed(List<ReportData> dataList, ReportConfig config) {
        // 简化实现，实际需要压缩数据
        log.debug("使用压缩模式上报，数据量: {}", dataList.size());
        return batchReport(dataList, config);
    }

    private ReportHandler getHandler(String protocol) {
        if (protocol == null) {
            return null;
        }
        return handlers.get(protocol.toUpperCase());
    }

    /**
     * 获取支持的协议列表
     */
    public List<String> getSupportedProtocols() {
        return new ArrayList<>(handlers.keySet());
    }

    /**
     * 更新上报配置
     */
    public void updateConfig(ReportConfig config) {
        if (config == null || !config.validate()) {
            log.warn("更新配置失败，配置无效");
            return;
        }

        try {
            ReportHandler handler = getHandler(config.getProtocol());
            if (handler != null) {
                handler.onConfigUpdate(config);
                log.info("上报配置已更新: {}", config.getTargetId());
            } else {
                log.warn("找不到对应的处理器: {}", config.getProtocol());
            }
        } catch (Exception e) {
            log.error("更新上报配置失败: {}", config.getTargetId(), e);
        }
    }

    /**
     * 移除上报配置
     */
    public void removeConfig(ReportConfig config) {
        if (config == null) {
            return;
        }

        try {
            ReportHandler handler = getHandler(config.getProtocol());
            if (handler != null) {
                handler.onConfigRemove(config);
                log.info("上报配置已移除: {}", config.getTargetId());
            }
        } catch (Exception e) {
            log.error("移除上报配置失败: {}", config.getTargetId(), e);
        }
    }

    /**
     * 获取处理器状态
     */
    public Map<String, Map<String, Object>> getHandlersStatus() {
        Map<String, Map<String, Object>> status = new HashMap<>();
        for (Map.Entry<String, ReportHandler> entry : handlers.entrySet()) {
            status.put(entry.getKey(), entry.getValue().getStatus());
        }
        return status;
    }

    /**
     * 获取统计信息
     */
    public Map<String, Map<String, Object>> getHandlersStatistics() {
        Map<String, Map<String, Object>> stats = new HashMap<>();
        for (Map.Entry<String, ReportHandler> entry : handlers.entrySet()) {
            stats.put(entry.getKey(), entry.getValue().getStatistics());
        }
        return stats;
    }

    /**
     * 重置所有处理器统计
     */
    public void resetAllStatistics() {
        for (ReportHandler handler : handlers.values()) {
            try {
                handler.resetStatistics();
            } catch (Exception e) {
                log.error("重置处理器统计失败: {}", handler.getName(), e);
            }
        }
        log.info("所有处理器统计已重置");
    }

    @PreDestroy
    public void destroy() {
        log.info("销毁上报管理器...");

        // 关闭线程池
        if (reportExecutor != null) {
            reportExecutor.shutdown();
            try {
                if (!reportExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                    reportExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                reportExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        // 销毁所有处理器
        for (ReportHandler handler : handlers.values()) {
            try {
                handler.destroy();
            } catch (Exception e) {
                log.error("销毁处理器失败: {}", handler.getName(), e);
            }
        }
        handlers.clear();

        log.info("上报管理器销毁完成");
    }
}