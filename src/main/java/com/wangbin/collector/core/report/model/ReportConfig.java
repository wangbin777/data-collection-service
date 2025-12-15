package com.wangbin.collector.core.report.model;

import com.wangbin.collector.common.constant.ProtocolConstant;
import lombok.Data;
import java.util.Map;

/**
 * 上报配置模型 - 简化版
 */
@Data
public class ReportConfig {
    private String targetId;          // 目标系统ID
    private String protocol;          // 协议类型
    private String host;              // 主机地址
    private Integer port;             // 端口
    private String url;               // URL（HTTP用）

    // 通用配置
    private Integer maxRetryCount;    // 最大重试次数
    private Integer retryInterval;       // 重试间隔（毫秒）
    private Integer connectTimeout;      // 连接超时（毫秒）
    private Integer readTimeout;      // 读取超时（毫秒）

    // 参数映射（包含所有需要的参数）
    private Map<String, Object> params;

    /**
     * 验证配置有效性
     */
    public boolean validate() {
        if (protocol == null || protocol.trim().isEmpty()) {
            return false;
        }

        String protocolUpper = protocol.toUpperCase();
        switch (protocolUpper) {
            case ProtocolConstant.PROTOCOL_HTTP:
                return url != null && !url.trim().isEmpty();
            case ProtocolConstant.PROTOCOL_MQTT:
            case ProtocolConstant.PROTOCOL_TCP:
            case ProtocolConstant.PROTOCOL_WEBSOCKET:
                return host != null && !host.trim().isEmpty() && getEffectivePort() > 0;
            default:
                return false;
        }
    }

    /**
     * 获取参数值
     */
    public Object getParam(String key) {
        return params != null ? params.get(key) : null;
    }

    /**
     * 获取字符串参数
     */
    public String getStringParam(String key) {
        Object value = getParam(key);
        return value != null ? value.toString() : null;
    }

    /**
     * 获取字符串参数（带默认值）
     */
    public String getStringParam(String key, String defaultValue) {
        String value = getStringParam(key);
        return value != null ? value : defaultValue;
    }

    /**
     * 获取整数参数
     */
    public Integer getIntParam(String key) {
        Object value = getParam(key);
        if (value instanceof Number) {
            return ((Number) value).intValue();
        } else if (value instanceof String) {
            try {
                return Integer.parseInt((String) value);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }

    /**
     * 获取整数参数（带默认值）
     */
    public int getIntParam(String key, int defaultValue) {
        Integer value = getIntParam(key);
        return value != null ? value : defaultValue;
    }

    /**
     * 获取长整数参数
     */
    public Long getLongParam(String key) {
        Object value = getParam(key);
        if (value instanceof Number) {
            return ((Number) value).longValue();
        } else if (value instanceof String) {
            try {
                return Long.parseLong((String) value);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }

    /**
     * 获取布尔参数
     */
    public Boolean getBooleanParam(String key) {
        Object value = getParam(key);
        if (value instanceof Boolean) {
            return (Boolean) value;
        } else if (value instanceof String) {
            return Boolean.parseBoolean((String) value);
        } else if (value instanceof Number) {
            return ((Number) value).intValue() != 0;
        }
        return null;
    }

    /**
     * 获取布尔参数（带默认值）
     */
    public boolean getBooleanParam(String key, boolean defaultValue) {
        Boolean value = getBooleanParam(key);
        return value != null ? value : defaultValue;
    }

    /**
     * 获取有效的端口
     */
    public int getEffectivePort() {
        if (port != null && port > 0) {
            return port;
        }

        // 根据协议返回默认端口
        String protocolUpper = protocol != null ? protocol.toUpperCase() : "";
        switch (protocolUpper) {
            case ProtocolConstant.PROTOCOL_MQTT:
                return ProtocolConstant.DEFAULT_MQTT_PORT;
            case ProtocolConstant.PROTOCOL_HTTP:
                return ProtocolConstant.DEFAULT_HTTP_PORT;
            case ProtocolConstant.PROTOCOL_TCP:
                return ProtocolConstant.DEFAULT_IOT_TCP_PORT;
            default:
                return ProtocolConstant.DEFAULT_HTTP_PORT;
        }
    }

    /**
     * 获取有效的连接超时
     */
    public int getEffectiveConnectTimeout() {
        if (connectTimeout != null && connectTimeout > 0) {
            return connectTimeout;
        }
        return ProtocolConstant.DEFAULT_CONNECT_TIMEOUT_MS;
    }

    /**
     * 获取有效的读取超时
     */
    public int getEffectiveReadTimeout() {
        if (readTimeout != null && readTimeout > 0) {
            return readTimeout;
        }
        return ProtocolConstant.DEFAULT_READ_TIMEOUT_MS;
    }

    /**
     * 获取有效的最大重试次数
     */
    public int getEffectiveMaxRetryCount() {
        if (maxRetryCount != null && maxRetryCount >= 0) {
            return maxRetryCount;
        }
        return ProtocolConstant.DEFAULT_MAX_RETRY_COUNT;
    }

    /**
     * 获取有效的重试间隔
     */
    public long getEffectiveRetryInterval() {
        if (retryInterval != null && retryInterval >= 0) {
            return retryInterval;
        }
        return ProtocolConstant.DEFAULT_RETRY_INTERVAL_MS;
    }

    /**
     * 获取MQTT客户端ID
     */
    public String getMqttClientId() {
        String clientId = getStringParam(ProtocolConstant.MQTT_PARAM_CLIENT_ID);
        if (clientId == null || clientId.trim().isEmpty()) {
            return "DataCollector_" + targetId + "_" + System.currentTimeMillis();
        }
        return clientId;
    }
}