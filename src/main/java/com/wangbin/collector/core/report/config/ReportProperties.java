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
    private long interval = 1000;

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

        public String getTopicTemplate() {
            // 将 clientId 中的 "." 替换为 "/"
            String formattedClientId = clientId.replace('.', '/');
            // 返回 "iot/device/" + 格式化后的 clientId
            return "iot/device/" + formattedClientId;
        }
    }
}
