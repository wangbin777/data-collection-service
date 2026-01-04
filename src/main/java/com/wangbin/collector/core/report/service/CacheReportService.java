package com.wangbin.collector.core.report.service;

import com.wangbin.collector.common.constant.ProtocolConstant;
import com.wangbin.collector.common.domain.entity.CollectionConfig;
import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.common.enums.QualityEnum;
import com.wangbin.collector.core.config.manager.ConfigManager;
import com.wangbin.collector.core.config.model.ConfigUpdateEvent;
import com.wangbin.collector.core.processor.ProcessResult;
import com.wangbin.collector.core.report.config.ReportProperties;
import com.wangbin.collector.core.report.model.ReportConfig;
import com.wangbin.collector.core.report.model.ReportData;
import com.wangbin.collector.core.report.model.ReportResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 将缓存中的实时数据转换为 MQTT 上报数据
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CacheReportService {

    private final ReportManager reportManager;
    private final ConfigManager configManager;
    private final ReportProperties reportProperties;

    private final ConcurrentMap<String, ReportConfig> deviceConfigCache = new ConcurrentHashMap<>();

    /**
     * 上报单个点位数据
     */
    public void reportPoint(String deviceId,String method, DataPoint point, Object cacheValue) {
        if (!isMqttEnabled() || point == null || cacheValue == null) {
            return;
        }

        ReportData reportData = ReportData.buildReportData(deviceId, method, point, cacheValue);
        if (reportData == null) {
            return;
        }

        ReportConfig reportConfig = resolveReportConfig(deviceId);
        if (reportConfig == null || !reportConfig.validate()) {
            log.warn("无法生成有效的MQTT上报配置，deviceId={}", deviceId);
            return;
        }

        CompletableFuture<ReportResult> future = reportManager.reportAsync(reportData, reportConfig);
        future.whenComplete((result, throwable) -> {
            if (throwable != null) {
                log.error("MQTT上报失败: {} -> {}", reportData.getPointCode(), reportConfig.getTargetId(), throwable);
                return;
            }
            if (!result.isSuccess()) {
                log.warn("MQTT上报返回失败: {} -> {} , err={}", reportData.getPointCode(),
                        reportConfig.getTargetId(), result.getErrorMessage());
            }
        });
    }

    @EventListener
    public void handleConfigUpdate(ConfigUpdateEvent event) {
        if (event.getDeviceId() != null) {
            deviceConfigCache.remove(event.getDeviceId());
        }
    }

    private boolean isMqttEnabled() {
        return reportProperties != null && reportProperties.mqttEnabled();
    }

    private ReportConfig resolveReportConfig(String deviceId) {
        return deviceConfigCache.compute(deviceId, (key, existing) -> {
            if (existing != null && existing.validate()) {
                return existing;
            }
            ReportConfig created = buildReportConfig(deviceId);
            if (created == null) {
                return null;
            }
            return created;
        });
    }

    private ReportConfig buildReportConfig(String deviceId) {
        ReportProperties.Mqtt mqtt = reportProperties.getMqtt();
        BrokerEndpoint endpoint = parseBrokerEndpoint(mqtt.getBrokerUrl());
        if (endpoint == null) {
            log.error("无法解析MQTT broker地址: {}", mqtt.getBrokerUrl());
            return null;
        }

        CollectionConfig collectionConfig = configManager.getCollectionConfig(deviceId);

        ReportConfig config = new ReportConfig();
        config.setProtocol(ProtocolConstant.PROTOCOL_MQTT);
        config.setHost(endpoint.host());
        config.setPort(endpoint.port());
        config.setTargetId(resolveTargetId(deviceId, collectionConfig));
        config.setMaxRetryCount(reportProperties.getRetryTimes());
        config.setRetryInterval((int) reportProperties.getInterval());
        config.setConnectTimeout(reportProperties.getTimeout());
        config.setReadTimeout(reportProperties.getTimeout());

        Map<String, Object> params = new HashMap<>();
        params.put(ProtocolConstant.MQTT_PARAM_CLIENT_ID, resolveClientId(deviceId, mqtt.getClientId()));
        params.put(ProtocolConstant.MQTT_PARAM_USERNAME, mqtt.getUsername());
        params.put(ProtocolConstant.MQTT_PARAM_PASSWORD, mqtt.getPassword());
        params.put(ProtocolConstant.MQTT_PARAM_KEEP_ALIVE, mqtt.getKeepAliveInterval());
        params.put(ProtocolConstant.MQTT_PARAM_CLEAN_SESSION, mqtt.isCleanSession());
        params.put(ProtocolConstant.MQTT_PARAM_PUBLISH_TOPIC, resolveTopicTemplate(deviceId, collectionConfig, mqtt));
        params.put("qos", resolveQos(collectionConfig, mqtt));
        params.put("retained", resolveRetained(collectionConfig, mqtt));
        config.setParams(params);

        return config;
    }



    private String resolveClientId(String deviceId, String template) {
        if (template == null || template.isEmpty()) {
            return "collector-" + deviceId;
        }
        return template
                .replace("{deviceId}", deviceId)
                .replace("${deviceId}", deviceId);
    }

    private String resolveTargetId(String deviceId, CollectionConfig config) {
        if (config != null && config.getTargetId() != null && !config.getTargetId().isEmpty()) {
            return config.getTargetId();
        }
        return deviceId;
    }

    private String resolveTopicTemplate(String deviceId, CollectionConfig config, ReportProperties.Mqtt mqtt) {
        String template = mqtt.getTopicTemplate();
        Map<String, Object> reportParams = config != null ? config.getReportParams() : null;
        if (reportParams != null && reportParams.get("reportTopic") != null) {
            template = String.valueOf(reportParams.get("reportTopic"));
        } else if (template == null || template.isEmpty()) {
            template = mqtt.getTopics().getOrDefault("property", "data/{deviceId}/{pointCode}");
        }
        return template
                .replace("{deviceId}", deviceId)
                .replace("${deviceId}", deviceId);
    }

    private int resolveQos(CollectionConfig config, ReportProperties.Mqtt mqtt) {
        Map<String, Object> reportParams = config != null ? config.getReportParams() : null;
        if (reportParams != null && reportParams.get("qos") != null) {
            Object qos = reportParams.get("qos");
            if (qos instanceof Number number) {
                return number.intValue();
            }
            try {
                return Integer.parseInt(qos.toString());
            } catch (NumberFormatException ignore) {
                // fall through
            }
        }
        return mqtt.getQos();
    }

    private boolean resolveRetained(CollectionConfig config, ReportProperties.Mqtt mqtt) {
        Map<String, Object> reportParams = config != null ? config.getReportParams() : null;
        if (reportParams != null && reportParams.get("retain") != null) {
            Object retain = reportParams.get("retain");
            if (retain instanceof Boolean bool) {
                return bool;
            }
            return Boolean.parseBoolean(retain.toString());
        }
        return mqtt.isRetained();
    }

    private BrokerEndpoint parseBrokerEndpoint(String brokerUrl) {
        if (brokerUrl == null || brokerUrl.isEmpty()) {
            return null;
        }
        try {
            URI uri = URI.create(brokerUrl);
            String host = Optional.ofNullable(uri.getHost()).orElse(uri.getPath());
            int port = uri.getPort();
            if (port <= 0) {
                port = Objects.equals("ssl", uri.getScheme()) || Objects.equals("tls", uri.getScheme())
                        ? 8883 : ProtocolConstant.DEFAULT_MQTT_PORT;
            }
            return new BrokerEndpoint(host, port);
        } catch (Exception e) {
            log.error("解析MQTT broker地址失败: {}", brokerUrl, e);
            return null;
        }
    }

    private record BrokerEndpoint(String host, int port) {
    }
}
