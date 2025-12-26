package com.wangbin.collector.core.connection.model;

import lombok.Data;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * 连接配置
 */
@Data
public class ConnectionConfig {

    // 基础配置
    private String deviceId;
    private String deviceName;
    private String protocolType;
    private String connectionType;
    private String groupId;
    private Integer maxGroupConnections = 0;
    private String host;
    private Integer port;
    private String url;

    // 连接参数
    private Integer timeout = 30000;
    private Integer connectTimeout = 5000;
    private Integer readTimeout = 30000;
    private Integer writeTimeout = 30000;
    private Integer maxFrameLength = 65535;
    private Integer reconnectDelay = 5000;
    private Integer maxReconnectTimes = 3;
    private Integer heartbeatInterval = 30000;
    private Integer heartbeatTimeout = 90000;

    // 认证配置
    private String username;
    private String password;
    private String clientId;
    private String productKey;
    private String deviceSecret;
    private String authToken;
    private Map<String, String> authParams;

    // 协议特定配置
    private Map<String, Object> protocolConfig;

    // 其他配置
    private Boolean sslEnabled = false;
    private String sslCertPath;
    private String sslKeyPath;
    private Charset charset = StandardCharsets.UTF_8;
    private Boolean keepAlive = true;
    private Integer bufferSize = 8192;
    /**
     * 重连相关配置
     */
    private boolean autoReconnect = true; // 是否启用自动重连
    private long initialReconnectDelay = 1000; // 初始重连延迟（毫秒）
    private long maxReconnectDelay = 60000; // 最大重连延迟（毫秒）
    private int maxReconnectAttempts = -1; // 最大重连尝试次数（-1表示无限制）
    private double reconnectBackoffMultiplier = 2.0; // 重连延迟乘数因子

    // Getters and setters for reconnection configuration
    public boolean isAutoReconnect() {
        return autoReconnect;
    }

    public void setAutoReconnect(boolean autoReconnect) {
        this.autoReconnect = autoReconnect;
    }

    public long getInitialReconnectDelay() {
        return initialReconnectDelay;
    }

    public void setInitialReconnectDelay(long initialReconnectDelay) {
        this.initialReconnectDelay = initialReconnectDelay;
    }

    public long getMaxReconnectDelay() {
        return maxReconnectDelay;
    }

    public void setMaxReconnectDelay(long maxReconnectDelay) {
        this.maxReconnectDelay = maxReconnectDelay;
    }

    public int getMaxReconnectAttempts() {
        return maxReconnectAttempts;
    }

    public void setMaxReconnectAttempts(int maxReconnectAttempts) {
        this.maxReconnectAttempts = maxReconnectAttempts;
    }

    public double getReconnectBackoffMultiplier() {
        return reconnectBackoffMultiplier;
    }

    public void setReconnectBackoffMultiplier(double reconnectBackoffMultiplier) {
        this.reconnectBackoffMultiplier = reconnectBackoffMultiplier;
    }
    private Integer maxPendingMessages = 5000;
    private Integer dispatchBatchSize = 1;
    private Long dispatchFlushInterval = 0L;
    private String overflowStrategy = "BLOCK";
    private Map<String, Object> extraParams;

    // 获取完整地址
    public String getFullAddress() {
        if (url != null && !url.isEmpty()) {
            return url;
        }
        if (host != null && port != null) {
            return host + ":" + port;
        }
        return host;
    }

    // 验证配置
    public boolean isValid() {
        if (connectionType == null || connectionType.isEmpty()) {
            return false;
        }

        if ("TCP".equalsIgnoreCase(connectionType)
                || "HTTP".equalsIgnoreCase(connectionType)
                || "MODBUS_TCP".equalsIgnoreCase(connectionType)
                || "COAP".equalsIgnoreCase(connectionType)
                || "SNMP".equalsIgnoreCase(connectionType)) {
            return host != null && !host.isEmpty() && port != null && port > 0;
        }

        if ("MQTT".equalsIgnoreCase(connectionType)) {
            return host != null && !host.isEmpty() && port != null && port > 0;
        }

        if ("WEBSOCKET".equalsIgnoreCase(connectionType)) {
            return url != null && !url.isEmpty();
        }

        if ("MODBUS_RTU".equalsIgnoreCase(connectionType)) {
            // RTU 连接至少需要串口名
            return host != null && !host.isEmpty();
        }

        return true;
    }

    // 获取配置值
    public Object getConfigValue(String key) {
        if (protocolConfig != null && protocolConfig.containsKey(key)) {
            return protocolConfig.get(key);
        }
        if (extraParams != null && extraParams.containsKey(key)) {
            return extraParams.get(key);
        }
        if (authParams != null && authParams.containsKey(key)) {
            return authParams.get(key);
        }
        return null;
    }

    // 获取配置值（带默认值）
    public Object getConfigValue(String key, Object defaultValue) {
        Object value = getConfigValue(key);
        return value != null ? value : defaultValue;
    }

    // 获取指定类型的配置值（带默认值）
    @SuppressWarnings("unchecked")
    public <T> T getConfigValue(String key, Class<T> type, T defaultValue) {
        Object value = getConfigValue(key);
        if (value != null) {
            try {
                // 类型转换
                if (type.isInstance(value)) {
                    return (T) value;
                } else if (type == Integer.class) {
                    if (value instanceof Number) {
                        return (T) Integer.valueOf(((Number) value).intValue());
                    } else if (value instanceof String) {
                        return (T) Integer.valueOf((String) value);
                    }
                } else if (type == Long.class) {
                    if (value instanceof Number) {
                        return (T) Long.valueOf(((Number) value).longValue());
                    } else if (value instanceof String) {
                        return (T) Long.valueOf((String) value);
                    }
                } else if (type == Double.class) {
                    if (value instanceof Number) {
                        return (T) Double.valueOf(((Number) value).doubleValue());
                    } else if (value instanceof String) {
                        return (T) Double.valueOf((String) value);
                    }
                } else if (type == Boolean.class) {
                    if (value instanceof Boolean) {
                        return (T) value;
                    } else if (value instanceof String) {
                        return (T) Boolean.valueOf((String) value);
                    } else if (value instanceof Number) {
                        return (T) Boolean.valueOf(((Number) value).intValue() != 0);
                    }
                } else if (type == String.class) {
                    return (T) value.toString();
                }
            } catch (Exception e) {
                // 转换失败，返回默认值
            }
        }
        return defaultValue;
    }

    // 获取字符串配置值
    public String getStringConfig(String key, String defaultValue) {
        return getConfigValue(key, String.class, defaultValue);
    }

    // 获取整数配置值
    public Integer getIntConfig(String key, Integer defaultValue) {
        return getConfigValue(key, Integer.class, defaultValue);
    }

    // 获取布尔配置值
    public Boolean getBoolConfig(String key, Boolean defaultValue) {
        return getConfigValue(key, Boolean.class, defaultValue);
    }

    // 获取长整数配置值
    public Long getLongConfig(String key, Long defaultValue) {
        return getConfigValue(key, Long.class, defaultValue);
    }

    // 获取Map配置值
    @SuppressWarnings("unchecked")
    public Map<String, Object> getMapConfig(String key) {
        Object value = getConfigValue(key);
        if (value instanceof Map) {
            return (Map<String, Object>) value;
        }
        return null;
    }
}
