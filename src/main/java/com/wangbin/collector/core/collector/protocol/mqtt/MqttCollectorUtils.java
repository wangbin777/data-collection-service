package com.wangbin.collector.core.collector.protocol.mqtt;

import java.util.Locale;

final class MqttCollectorUtils {
    private MqttCollectorUtils() {
    }

    static int asInt(Object value, int defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number number) {
            return number.intValue();
        }
        try {
            return Integer.parseInt(value.toString());
        } catch (NumberFormatException ex) {
            return defaultValue;
        }
    }

    static boolean asBoolean(Object value, boolean defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Boolean bool) {
            return bool;
        }
        String text = value.toString().trim().toLowerCase(Locale.ROOT);
        if (text.isEmpty()) {
            return defaultValue;
        }
        return "true".equals(text) || "1".equals(text) || "yes".equals(text) || "on".equals(text);
    }
}
