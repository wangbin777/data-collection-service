package com.wangbin.collector.common.domain.entity;

import com.wangbin.collector.common.enums.Parity;
import lombok.Data;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * �豸���Ӳ���
 */
@Data
public class DeviceConnection {

    private Long id;
    private String connectionType;
    private String host;
    private Integer port;
    private String url;

    private Integer connectTimeout = 5000;
    private Integer readTimeout = 30000;
    private Integer writeTimeout = 30000;
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

    private Map<String, Object> extraParams;

    private Integer slaveId = 1;
    private String serialPort = "COM4";
    private Integer baudRate = 9600;
    private Integer dataBits = 8;
    private Integer stopBits = 1;
    private String parity = Parity.none.name(); //enum Parity
    private Integer interFrameDelay = 5;//帧间延迟 两个数据帧（或数据包）之间强制插入的最小时间间隔
    private String flowControl; // 流量控制 协调发送方和接收方之间的数据传输速率

    private Integer retries;
    private String extJson;
    private String remark;

    public Object getProperty(String key) {
        if (extraParams != null && key != null) {
            return extraParams.get(key);
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
            case "SERIAL":
                return serialPort != null && !serialPort.isEmpty();
            default:
                return true;
        }
    }
}
