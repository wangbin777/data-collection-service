package com.wangbin.collector.core.report.service;

import com.wangbin.collector.common.constant.ProtocolConstant;
import com.wangbin.collector.core.report.config.ReportProperties;
import com.wangbin.collector.core.report.handler.ReportHandler;
import com.wangbin.collector.core.report.model.ReportConfig;
import com.wangbin.collector.core.report.model.ReportData;
import com.wangbin.collector.core.report.model.ReportResult;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/**
 * ????? - ?????????????
 */
@Slf4j
@Service
public class ReportManager {

    private final Map<String, ReportHandler> handlers = new ConcurrentHashMap<>();
    private final Executor reportExecutor;
    private final ReportProperties reportProperties;
    private final List<ReportHandler> handlerCandidates;

    @Autowired
    public ReportManager(ReportProperties reportProperties,
                         @Qualifier("reportExecutor") Executor reportExecutor,
                         List<ReportHandler> handlerCandidates) {
        this.reportProperties = reportProperties;
        this.reportExecutor = reportExecutor;
        this.handlerCandidates = handlerCandidates;
    }

    @PostConstruct
    public void init() {
        log.info("????????...");
        registerHandlers();
        log.info("???????????????: {}", handlers.keySet());
    }

    private void registerHandlers() {
        if (!reportProperties.isEnabled()) {
            log.warn("collector.report.enabled=false????????");
            return;
        }
        for (ReportHandler candidate : handlerCandidates) {
            if (candidate == null) {
                continue;
            }
            String protocol = candidate.getProtocol();
            if (!reportProperties.isProtocolEnabled(protocol)) {
                log.info("?? {} ???????????", protocol);
                continue;
            }
            try {
                candidate.init();
                handlers.put(protocol.toUpperCase(Locale.ROOT), candidate);
                log.info("???????: {}", protocol);
            } catch (Exception e) {
                log.error("???????? [{}] ??", protocol, e);
            }
        }
    }

    public CompletableFuture<ReportResult> reportAsync(ReportData data, ReportConfig config) {
        if (!reportProperties.isEnabled()) {
            return CompletableFuture.completedFuture(disabledResult(data, config));
        }
        return CompletableFuture.supplyAsync(() -> report(data, config), reportExecutor);
    }

    public ReportResult report(ReportData data, ReportConfig config) {
        if (!reportProperties.isEnabled()) {
            return disabledResult(data, config);
        }
        if (data == null || config == null) {
            return ReportResult.error(
                    data != null ? data.getPointCode() : "unknown",
                    "??????",
                    config != null ? config.getTargetId() : "unknown"
            );
        }

        if (!config.validate()) {
            return ReportResult.error(data.getPointCode(),
                    "??????", config.getTargetId());
        }

        try {
            ReportHandler handler = getHandler(config.getProtocol());
            if (handler == null) {
                return ReportResult.error(data.getPointCode(),
                        "????????: " + config.getProtocol(), config.getTargetId());
            }

            return handler.report(data, config);

        } catch (Exception e) {
            log.error("??????: {} -> {}", data.getPointCode(), config.getTargetId(), e);
            return ReportResult.error(data.getPointCode(),
                    "????: " + e.getMessage(), config.getTargetId());
        }
    }

    public List<ReportResult> batchReport(List<ReportData> dataList, ReportConfig config) {
        if (!reportProperties.isEnabled()) {
            return Collections.emptyList();
        }
        if (dataList == null || dataList.isEmpty() || config == null) {
            return Collections.emptyList();
        }

        if (!config.validate()) {
            List<ReportResult> errors = new ArrayList<>();
            for (ReportData data : dataList) {
                errors.add(ReportResult.error(data.getPointCode(),
                        "??????", config.getTargetId()));
            }
            return errors;
        }

        try {
            ReportHandler handler = getHandler(config.getProtocol());
            if (handler == null) {
                List<ReportResult> errors = new ArrayList<>();
                for (ReportData data : dataList) {
                    errors.add(ReportResult.error(data.getPointCode(),
                            "????????: " + config.getProtocol(), config.getTargetId()));
                }
                return errors;
            }

            return handler.batchReport(dataList, config);

        } catch (Exception e) {
            log.error("??????: {}", config.getTargetId(), e);
            List<ReportResult> errors = new ArrayList<>();
            for (ReportData data : dataList) {
                errors.add(ReportResult.error(data.getPointCode(),
                        "??????: " + e.getMessage(), config.getTargetId()));
            }
            return errors;
        }
    }

    public List<ReportResult> reportWithMode(List<ReportData> dataList, ReportConfig config, String reportMode) {
        if (!reportProperties.isEnabled()) {
            return Collections.emptyList();
        }
        String normalizedMode = reportMode == null
                ? ProtocolConstant.REPORT_MODE_REALTIME
                : reportMode.toUpperCase(Locale.ROOT);

        return switch (normalizedMode) {
            case ProtocolConstant.REPORT_MODE_REALTIME ->
                    dataList.stream().map(data -> report(data, config)).toList();
            case ProtocolConstant.REPORT_MODE_BATCH -> batchReport(dataList, config);
            case ProtocolConstant.REPORT_MODE_COMPRESSED -> reportCompressed(dataList, config);
            default -> batchReport(dataList, config);
        };
    }

    private List<ReportResult> reportCompressed(List<ReportData> dataList, ReportConfig config) {
        log.debug("????????????: {}", dataList.size());
        return batchReport(dataList, config);
    }

    private ReportHandler getHandler(String protocol) {
        if (protocol == null) {
            return null;
        }
        return handlers.get(protocol.toUpperCase(Locale.ROOT));
    }

    public List<String> getSupportedProtocols() {
        return new ArrayList<>(handlers.keySet());
    }

    public void updateConfig(ReportConfig config) {
        if (config == null || !config.validate()) {
            log.warn("???????????");
            return;
        }

        try {
            ReportHandler handler = getHandler(config.getProtocol());
            if (handler != null) {
                handler.onConfigUpdate(config);
                log.info("???????: {}", config.getTargetId());
            } else {
                log.warn("?????????: {}", config.getProtocol());
            }
        } catch (Exception e) {
            log.error("????????: {}", config.getTargetId(), e);
        }
    }

    public void removeConfig(ReportConfig config) {
        if (config == null) {
            return;
        }

        try {
            ReportHandler handler = getHandler(config.getProtocol());
            if (handler != null) {
                handler.onConfigRemove(config);
                log.info("???????: {}", config.getTargetId());
            }
        } catch (Exception e) {
            log.error("????????: {}", config.getTargetId(), e);
        }
    }

    public Map<String, Map<String, Object>> getHandlersStatus() {
        Map<String, Map<String, Object>> status = new HashMap<>();
        for (Map.Entry<String, ReportHandler> entry : handlers.entrySet()) {
            status.put(entry.getKey(), entry.getValue().getStatus());
        }
        return status;
    }

    public Map<String, Map<String, Object>> getHandlersStatistics() {
        Map<String, Map<String, Object>> stats = new HashMap<>();
        for (Map.Entry<String, ReportHandler> entry : handlers.entrySet()) {
            stats.put(entry.getKey(), entry.getValue().getStatistics());
        }
        return stats;
    }

    public void resetAllStatistics() {
        for (ReportHandler handler : handlers.values()) {
            try {
                handler.resetStatistics();
            } catch (Exception e) {
                log.error("?????????: {}", handler.getName(), e);
            }
        }
        log.info("??????????");
    }

    @PreDestroy
    public void destroy() {
        log.info("???????...");

        for (ReportHandler handler : handlers.values()) {
            try {
                handler.destroy();
            } catch (Exception e) {
                log.error("???????: {}", handler.getName(), e);
            }
        }
        handlers.clear();

        log.info("?????????");
    }

    private ReportResult disabledResult(ReportData data, ReportConfig config) {
        String pointCode = data != null ? data.getPointCode() : "unknown";
        String targetId = config != null ? config.getTargetId() : "unknown";
        return ReportResult.error(pointCode, "???????", targetId);
    }
}
