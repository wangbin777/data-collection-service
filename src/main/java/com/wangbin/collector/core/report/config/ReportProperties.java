package com.wangbin.collector.core.report.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * collector.report 配置映射
 */
@Data
@Component
@ConfigurationProperties(prefix = "collector.report")
public class ReportProperties {

    /**
     * 是否开启上报
     */
    private boolean enabled = true;

    /**
     * 上报模式（http/mqtt/websocket等）
     */
    private String mode = "MQTT";

    /**
     * 单次批量上报数量
     */
    private int batchSize = 100;

    /**
     * 调度间隔（毫秒）
     */
    private long intervalMs = 10000;

    /**
     * 上报超时（毫秒）
     */
    private int timeout = 3000;

    /**
     * 最大重试次数
     */
    private int retryTimes = 3;

    /**
     * 缓存队列最大长度
     */
    private int maxQueueSize = 5000;

    /**
     * 刷新间隔（毫秒）
     */
    private long flushInterval = 1000;

    /**
     * 最小上报间隔（用于变化触发）
     */
    private long minReportIntervalMs = 2000;

    /**
     * 事件/告警触发默认最小间隔
     */
    private long eventMinIntervalMs = 5000;

    /**
     * 网关每秒最大发包数量（0 表示不限）
     */
    private int maxGatewayMessagesPerSecond = 200;

    /**
     * 单个快照消息包含的最大属性数量
     */
    private int maxPropertiesPerMessage = 200;

    /**
     * 单个快照消息允许的最大载荷字节数（粗略估算，0 表示不限制）
     */
    private int maxPayloadBytes = 128 * 1024;

    /**
     * 上报数据结构版本
     */
    private int schemaVersion = 2;

    /**
     * MQTT 相关配置
     */
    private final Mqtt mqtt = new Mqtt();

    public boolean mqttEnabled() {
        return enabled && "MQTT".equalsIgnoreCase(mode) && mqtt.isEnabled();
    }

    @Data
    public static class Mqtt {
        private boolean enabled = true;
        private String brokerUrl = "tcp://localhost:1883";
        private String clientId = "data-collector";
        /**
         * 对应云平台的产品 key，用于拼装 topic。
         */
        private String productKey = "";
        /**
         * topic 前缀，默认 iot/device。
         */
        private String topicPrefix = "iot/device";
        private String username;
        private String password;
        private int qos = 1;
        private boolean cleanSession = true;
        private int connectionTimeout = 30;
        private int keepAliveInterval = 60;
        private boolean retained = false;
        /**
         * 业务自定义主题
         */
        private Map<String, String> topics = new HashMap<>();

        public String getTopicPrefix() {
            if (topicPrefix == null || topicPrefix.isEmpty()) {
                return "iot/device";
            }
            return topicPrefix.endsWith("/") ? topicPrefix.substring(0, topicPrefix.length() - 1) : topicPrefix;
        }

        public String getDefaultTopicTemplate() {
            return getTopicPrefix() + "/{productKey}/{deviceName}/{method}";
        }
    }
}
