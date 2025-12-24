package com.wangbin.collector.core.collector.protocol.mqtt;

import com.wangbin.collector.common.domain.entity.DeviceInfo;
import com.wangbin.collector.core.config.CollectorProperties;
import lombok.Getter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Getter
public class MqttConnectionConfig {

    private final String brokerUrl;
    private final String clientId;
    private final String username;
    private final String password;
    private final boolean cleanSession;
    private final int connectionTimeoutSeconds;
    private final int keepAliveIntervalSeconds;
    private final boolean automaticReconnect;
    private final MqttProtocolVersion version;
    private final int defaultQos;
    private final List<MqttTopicSubscription> defaultTopics;
    private final int maxPendingMessages;
    private final int dispatchBatchSize;
    private final long dispatchFlushIntervalMillis;
    private final String overflowStrategy;
    private final String groupId;
    private final int maxGroupConnections;

    private MqttConnectionConfig(String brokerUrl,
                                 String clientId,
                                 String username,
                                 String password,
                                 boolean cleanSession,
                                 int connectionTimeoutSeconds,
                                 int keepAliveIntervalSeconds,
                                 boolean automaticReconnect,
                                 MqttProtocolVersion version,
                                 int defaultQos,
                                 List<MqttTopicSubscription> defaultTopics,
                                 int maxPendingMessages,
                                 int dispatchBatchSize,
                                 long dispatchFlushIntervalMillis,
                                 String overflowStrategy,
                                 String groupId,
                                 int maxGroupConnections) {
        this.brokerUrl = brokerUrl;
        this.clientId = clientId;
        this.username = username;
        this.password = password;
        this.cleanSession = cleanSession;
        this.connectionTimeoutSeconds = connectionTimeoutSeconds;
        this.keepAliveIntervalSeconds = keepAliveIntervalSeconds;
        this.automaticReconnect = automaticReconnect;
        this.version = version;
        this.defaultQos = defaultQos;
        this.defaultTopics = defaultTopics;
        this.maxPendingMessages = maxPendingMessages;
        this.dispatchBatchSize = dispatchBatchSize;
        this.dispatchFlushIntervalMillis = dispatchFlushIntervalMillis;
        this.overflowStrategy = overflowStrategy;
        this.groupId = groupId;
        this.maxGroupConnections = maxGroupConnections;
    }

    public static MqttConnectionConfig from(DeviceInfo deviceInfo, CollectorProperties.MqttConfig defaults) {
        Map<String, Object> protocol = deviceInfo.getProtocolConfig();
        String broker = getString(protocol, "brokerUrl", defaults.getBrokerUrl());
        String clientId = getString(protocol, "clientId", defaults.getClientId());
        if (clientId == null || clientId.isBlank()) {
            clientId = deviceInfo.getDeviceId() + "_mqtt";
        }
        String username = getString(protocol, "username", defaults.getUsername());
        String password = getString(protocol, "password", defaults.getPassword());
        boolean cleanSession = getBoolean(protocol, "cleanSession", defaults.isCleanSession());
        int timeout = getInt(protocol, "connectionTimeout", defaults.getConnectionTimeout());
        int keepAlive = getInt(protocol, "keepAliveInterval", defaults.getKeepAliveInterval());
        boolean autoReconnect = getBoolean(protocol, "autoReconnect", true);
        int qos = getInt(protocol, "qos", defaults.getQos());
        MqttProtocolVersion version = MqttProtocolVersion.fromText(getString(protocol, "version", "v5"));
        List<MqttTopicSubscription> topics = parseTopics(protocol != null ? protocol.get("subscribeTopics") : null, qos);
        int maxPending = getInt(protocol, "maxPendingMessages", defaults.getMaxPendingMessages());
        int batchSize = getInt(protocol, "dispatchBatchSize", defaults.getDispatchBatchSize());
        long flushInterval = getLong(protocol, "dispatchFlushInterval", defaults.getDispatchFlushInterval());
        String overflow = getString(protocol, "overflowStrategy", defaults.getOverflowStrategy());
        String groupId = getString(protocol, "groupId", deviceInfo.getGroupId());
        int maxGroupConnections = getInt(protocol, "maxGroupConnections", defaults.getMaxGroupConnections());
        return new MqttConnectionConfig(broker, clientId, username, password, cleanSession,
                timeout, keepAlive, autoReconnect, version, qos, topics,
                maxPending, batchSize, flushInterval, overflow, groupId, maxGroupConnections);
    }

    private static String getString(Map<String, Object> map, String key, String defaultValue) {
        if (map == null || key == null) {
            return defaultValue;
        }
        Object value = map.get(key);
        return value != null ? value.toString() : defaultValue;
    }

    private static int getInt(Map<String, Object> map, String key, int defaultValue) {
        if (map == null || key == null) {
            return defaultValue;
        }
        Object value = map.get(key);
        return value != null ? MqttCollectorUtils.asInt(value, defaultValue) : defaultValue;
    }

    private static boolean getBoolean(Map<String, Object> map, String key, boolean defaultValue) {
        if (map == null || key == null) {
            return defaultValue;
        }
        Object value = map.get(key);
        return value != null ? MqttCollectorUtils.asBoolean(value, defaultValue) : defaultValue;
    }

    private static long getLong(Map<String, Object> map, String key, long defaultValue) {
        if (map == null || key == null) {
            return defaultValue;
        }
        Object value = map.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number number) {
            return number.longValue();
        }
        try {
            return Long.parseLong(value.toString());
        } catch (NumberFormatException ex) {
            return defaultValue;
        }
    }

    private static List<MqttTopicSubscription> parseTopics(Object value, int defaultQos) {
        if (value == null) {
            return Collections.emptyList();
        }
        List<MqttTopicSubscription> topics = new ArrayList<>();
        if (value instanceof Collection<?> collection) {
            for (Object item : collection) {
                Optional.ofNullable(item)
                        .map(Object::toString)
                        .map(String::trim)
                        .filter(s -> !s.isEmpty())
                        .ifPresent(topic -> topics.add(new MqttTopicSubscription(topic, defaultQos)));
            }
        } else {
            String[] parts = value.toString().split(",");
            for (String part : parts) {
                String trimmed = part.trim();
                if (!trimmed.isEmpty()) {
                    topics.add(new MqttTopicSubscription(trimmed, defaultQos));
                }
            }
        }
        return topics;
    }
}
