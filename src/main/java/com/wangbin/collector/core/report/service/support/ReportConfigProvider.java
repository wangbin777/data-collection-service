
package com.wangbin.collector.core.report.service.support;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.wangbin.collector.common.constant.ProtocolConstant;
import com.wangbin.collector.core.config.manager.ConfigManager;
import com.wangbin.collector.core.report.config.ReportProperties;
import com.wangbin.collector.core.report.model.ReportConfig;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Provides cached MQTT reporting configuration for a specific gateway device.
 */
@Component
public class ReportConfigProvider {

    private final ReportProperties reportProperties;
    private final Cache<String, ReportConfig> cache;

    public ReportConfigProvider(ConfigManager configManager, ReportProperties reportProperties) {
        this.reportProperties = reportProperties;
        this.cache = Caffeine.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(Duration.ofMinutes(10))
                .build();
    }

    public ReportConfig getConfig(String gatewayDeviceId) {
        if (gatewayDeviceId == null || !reportProperties.mqttEnabled()) {
            return null;
        }
        return cache.get(gatewayDeviceId, this::buildReportConfig);
    }

    public void evict(String gatewayDeviceId) {
        if (gatewayDeviceId != null) {
            cache.invalidate(gatewayDeviceId);
        }
    }

    private ReportConfig buildReportConfig(String gatewayDeviceId) {
        ReportProperties.Mqtt mqtt = reportProperties.getMqtt();
        BrokerEndpoint endpoint = parseBrokerEndpoint(mqtt.getBrokerUrl());
        if (endpoint == null) {
            return null;
        }

        ReportConfig config = new ReportConfig();
        config.setProtocol(ProtocolConstant.PROTOCOL_MQTT);
        config.setHost(endpoint.host());
        config.setPort(endpoint.port());
        config.setTargetId(gatewayDeviceId);
        config.setMaxRetryCount(reportProperties.getRetryTimes());
        config.setRetryInterval((int) reportProperties.getIntervalMs());
        config.setConnectTimeout(reportProperties.getTimeout());
        config.setReadTimeout(reportProperties.getTimeout());

        Map<String, Object> params = new HashMap<>();
        params.put(ProtocolConstant.MQTT_PARAM_CLIENT_ID, resolveClientId(gatewayDeviceId, mqtt.getClientId()));
        params.put(ProtocolConstant.MQTT_PARAM_USERNAME, mqtt.getUsername());
        params.put(ProtocolConstant.MQTT_PARAM_PASSWORD, mqtt.getPassword());
        params.put(ProtocolConstant.MQTT_PARAM_KEEP_ALIVE, mqtt.getKeepAliveInterval());
        params.put(ProtocolConstant.MQTT_PARAM_CLEAN_SESSION, mqtt.isCleanSession());
        params.put(ProtocolConstant.MQTT_PARAM_PUBLISH_TOPIC, mqtt.getDefaultTopicTemplate());
        params.put("qos", mqtt.getQos());
        params.put("retained", mqtt.isRetained());
        params.put(ProtocolConstant.MQTT_PARAM_ACK_TOPIC_TEMPLATE, mqtt.getAckTopicTemplate());
        params.put(ProtocolConstant.MQTT_PARAM_ACK_TIMEOUT, mqtt.getAckTimeoutMs());
        String fallbackProductKey = mqtt.getProductKey();
        if (fallbackProductKey != null && !fallbackProductKey.isEmpty()) {
            params.put("defaultProductKey", fallbackProductKey);
        }
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
            return null;
        }
    }

    private record BrokerEndpoint(String host, int port) {
    }
}
