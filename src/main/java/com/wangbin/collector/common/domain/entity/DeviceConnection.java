package com.wangbin.collector.common.domain.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.wangbin.collector.common.enums.Parity;
import lombok.Data;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 设备连接信息
 */
@Data
public class DeviceConnection {

    private Long id;
    private String connectionType;
    private String connectionKey;
    private String host;
    private Integer port;
    private String url;
    @JsonProperty("connectTimeoutMs")
    private Integer connectTimeout = 5000;
    @JsonProperty("readTimeoutMs")
    private Integer readTimeout = 30000;
    @JsonProperty("writeTimeoutMs")
    private Integer writeTimeout = 30000;
    private Integer retries;
    @JsonDeserialize(using = StringObjectMapDeserializer.class)
    private Map<String, Object> extJson;
    private String remark;

    private String connectionId;
    private String deviceId;
    private String deviceName;



    private Integer timeout = 30000;
    private Integer heartbeatInterval = 30000;
    private Integer heartbeatTimeout = 90000;
    private Integer subscriptionInterval = 1000; // 订阅间隔
    private Integer maxFrameLength = 65535;
    private Integer reconnectDelay = 5000;
    private Integer maxReconnectTimes = 3;
    private Integer maxGroupConnections = 0;

    private String username;
    private String password;
    private String clientId;
    private String productKey;
    private String deviceSecret;
    private String authToken;
    //安全策略
    private String securityPolicy;
    @JsonDeserialize(using = StringStringMapDeserializer.class)
    private Map<String, String> authParams;

    private Boolean sslEnabled = false;
    private String sslCertPath;
    private String sslKeyPath;
    private Charset charset = StandardCharsets.UTF_8;
    private Boolean keepAlive = true;
    private Integer bufferSize = 8192;

    private boolean autoReconnect = true;
    private long initialReconnectDelay = 1000;
    private long maxReconnectDelay = 60000;
    private int maxReconnectAttempts = -1;
    private double reconnectBackoffMultiplier = 2.0;

    private Integer maxPendingMessages = 5000;
    private Integer dispatchBatchSize = 1;
    private Long dispatchFlushInterval = 0L;
    private String overflowStrategy = "BLOCK";

    private String status;
    private Date connectTime;
    private Date disconnectTime;
    private Long duration;
    private String lastError;
    @JsonDeserialize(using = StringObjectMapDeserializer.class)
    private Map<String, Object> stats;
    private Date lastHeartbeatTime;
    private Date lastDataTime;
    private Date createTime;
    private Date updateTime;

    private ConnectionStats connectionStats = new ConnectionStats();


    public Object getProperty(String key) {
        if (extJson != null && key != null) {
            return extJson.get(key);
        }
        if (authParams != null && key != null) {
            return authParams.get(key);
        }
        return null;
    }

    public Object getProperty(String key, Object defaultValue) {
        Object value = getProperty(key);
        return value != null ? value : defaultValue;
    }

    @SuppressWarnings("unchecked")
    public <T> T getProperty(String key, Class<T> type, T defaultValue) {
        Object value = getProperty(key);
        if (value == null) {
            return defaultValue;
        }
        try {
            if (type.isInstance(value)) {
                return (T) value;
            }
            if (type == Integer.class) {
                if (value instanceof Number number) {
                    return (T) Integer.valueOf(number.intValue());
                }
                return (T) Integer.valueOf(value.toString());
            }
            if (type == Long.class) {
                if (value instanceof Number number) {
                    return (T) Long.valueOf(number.longValue());
                }
                return (T) Long.valueOf(value.toString());
            }
            if (type == Double.class) {
                if (value instanceof Number number) {
                    return (T) Double.valueOf(number.doubleValue());
                }
                return (T) Double.valueOf(value.toString());
            }
            if (type == Boolean.class) {
                if (value instanceof Boolean bool) {
                    return (T) bool;
                }
                if (value instanceof Number number) {
                    return (T) Boolean.valueOf(number.intValue() != 0);
                }
                return (T) Boolean.valueOf(value.toString());
            }
            if (type == String.class) {
                return (T) value.toString();
            }
            if (type == Map.class && value instanceof Map<?, ?> map) {
                return (T) map;
            }
        } catch (Exception ignore) {
            // ignore conversion
        }
        return defaultValue;
    }

    public String getString(String key, String defaultValue) {
        return getProperty(key, String.class, defaultValue);
    }

    public Integer getInt(String key, Integer defaultValue) {
        return getProperty(key, Integer.class, defaultValue);
    }

    public Long getLong(String key, Long defaultValue) {
        return getProperty(key, Long.class, defaultValue);
    }

    public Boolean getBool(String key, Boolean defaultValue) {
        return getProperty(key, Boolean.class, defaultValue);
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> getMap(String key) {
        return getProperty(key, Map.class, null);
    }

    public Object getConfigValue(String key) {
        return getProperty(key);
    }

    public Object getConfigValue(String key, Object defaultValue) {
        return getProperty(key, defaultValue);
    }

    public <T> T getConfigValue(String key, Class<T> type, T defaultValue) {
        return getProperty(key, type, defaultValue);
    }

    public String getStringConfig(String key, String defaultValue) {
        return getString(key, defaultValue);
    }

    public Integer getIntConfig(String key, Integer defaultValue) {
        return getInt(key, defaultValue);
    }

    public Boolean getBoolConfig(String key, Boolean defaultValue) {
        return getBool(key, defaultValue);
    }

    public Long getLongConfig(String key, Long defaultValue) {
        return getLong(key, defaultValue);
    }

    public Map<String, Object> getMapConfig(String key) {
        return getMap(key);
    }

    public String getFullAddress() {
        if (url != null && !url.isEmpty()) {
            return url;
        }
        if (host != null && port != null) {
            return host + ":" + port;
        }
        return host;
    }

    public boolean isValid() {
        if (connectionType == null || connectionType.isEmpty()) {
            return false;
        }
        String type = connectionType.toUpperCase();
        switch (type) {
            case "TCP":
            case "HTTP":
            case "MODBUS_TCP":
            case "COAP":
            case "SNMP":
            case "MQTT":
                return host != null && !host.isEmpty() && port != null && port > 0;
            case "WEBSOCKET":
                return url != null && !url.isEmpty();
            case "MODBUS_RTU":
            default:
                return true;
        }
    }

    public boolean isActive() {
        return "CONNECTED".equals(status) && lastHeartbeatTime != null
                && (System.currentTimeMillis() - lastHeartbeatTime.getTime()) < 120000;
    }

    public void updateHeartbeat() {
        lastHeartbeatTime = new Date();
    }

    public void updateDataTime() {
        lastDataTime = new Date();
    }

    public void calculateDuration() {
        if (connectTime != null) {
            Date endTime = disconnectTime != null ? disconnectTime : new Date();
            duration = endTime.getTime() - connectTime.getTime();
        }
    }

    @Data
    public static class ConnectionStats {
        private Long totalBytesSent = 0L;
        private Long totalBytesReceived = 0L;
        private Long totalMessagesSent = 0L;
        private Long totalMessagesReceived = 0L;
        private Long totalErrors = 0L;
        private Double averageResponseTime;
        private Double maxResponseTime;
        private Date lastResponseTime;

        public void addBytesSent(long bytes) {
            totalBytesSent += bytes;
        }

        public void addBytesReceived(long bytes) {
            totalBytesReceived += bytes;
        }

        public void addMessageSent() {
            totalMessagesSent++;
        }

        public void addMessageReceived() {
            totalMessagesReceived++;
        }

        public void addError() {
            totalErrors++;
        }

        public double getSuccessRate() {
            long total = totalMessagesSent + totalMessagesReceived;
            if (total == 0) {
                return 0.0;
            }
            return (total - totalErrors) * 100.0 / total;
        }
    }

    /**
     * 支持 JSON 对象或 JSON 字符串的 Map 反序列化
     */
    public static class StringObjectMapDeserializer extends JsonDeserializer<Map<String, Object>> {
        private static final ObjectMapper MAPPER = new ObjectMapper();
        private static final TypeReference<LinkedHashMap<String, Object>> TYPE =
                new TypeReference<LinkedHashMap<String, Object>>() {};

        @Override
        public Map<String, Object> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            JsonNode node = p.getCodec().readTree(p);
            if (node == null || node.isNull()) {
                return new LinkedHashMap<>();
            }
            if (node.isObject()) {
                return MAPPER.convertValue(node, TYPE);
            }
            if (node.isTextual()) {
                String text = node.asText();
                if (text == null || text.trim().isEmpty()) {
                    return new LinkedHashMap<>();
                }
                return MAPPER.readValue(text, TYPE);
            }
            return new LinkedHashMap<>();
        }
    }

    public static class StringStringMapDeserializer extends JsonDeserializer<Map<String, String>> {
        private static final ObjectMapper MAPPER = new ObjectMapper();
        private static final TypeReference<LinkedHashMap<String, String>> TYPE =
                new TypeReference<LinkedHashMap<String, String>>() {};

        @Override
        public Map<String, String> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            JsonNode node = p.getCodec().readTree(p);
            if (node == null || node.isNull()) {
                return new LinkedHashMap<>();
            }
            if (node.isObject()) {
                return MAPPER.convertValue(node, TYPE);
            }
            if (node.isTextual()) {
                String text = node.asText();
                if (text == null || text.trim().isEmpty()) {
                    return new LinkedHashMap<>();
                }
                return MAPPER.readValue(text, TYPE);
            }
            return new LinkedHashMap<>();
        }
    }
}
