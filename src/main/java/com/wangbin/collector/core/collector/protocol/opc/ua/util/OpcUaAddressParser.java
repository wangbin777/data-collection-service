package com.wangbin.collector.core.collector.protocol.opc.ua.util;

import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.core.collector.protocol.opc.ua.domain.OpcUaAddress;
import com.wangbin.collector.core.collector.protocol.opc.ua.domain.OpcUaDataType;
import org.eclipse.milo.opcua.stack.core.types.builtin.ByteString;
import org.eclipse.milo.opcua.stack.core.types.builtin.DateTime;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Helpers for parsing OPC UA addressing metadata out of a {@link DataPoint}.
 */
public final class OpcUaAddressParser {

    private OpcUaAddressParser() {
    }

    public static OpcUaAddress parse(DataPoint point) {
        if (point == null) {
            throw new IllegalArgumentException("DataPoint cannot be null");
        }

        Map<String, Object> config = point.getAdditionalConfig() != null
                ? point.getAdditionalConfig()
                : Collections.emptyMap();

        NodeId nodeId = resolveNodeId(point, config);
        OpcUaDataType dataType = resolveDataType(point, config);
        double samplingInterval = parseDouble(config.get("samplingInterval"),
                parseDouble(config.get("publishingInterval"), -1d));
        int queueSize = parseInt(config.get("queueSize"), 10);
        double deadband = parseDouble(config.get("deadband"), -1d);
        boolean subscribe = resolveSubscription(point, config);

        return new OpcUaAddress(nodeId, dataType, samplingInterval, queueSize, deadband, subscribe);
    }

    public static Variant toVariant(Object value, OpcUaDataType dataType) {
        if (value == null) {
            return new Variant(null);
        }
        OpcUaDataType type = dataType != null ? dataType : OpcUaDataType.AUTO;

        return switch (type) {
            case BOOLEAN -> new Variant(toBoolean(value));
            case BYTE -> new Variant(Unsigned.ubyte((short) (toLong(value) & 0xFF)));
            case SBYTE -> new Variant((byte) toLong(value));
            case INT16 -> new Variant((short) toLong(value));
            case UINT16 -> new Variant(Unsigned.ushort((int) (toLong(value) & 0xFFFF)));
            case INT32 -> new Variant((int) toLong(value));
            case UINT32 -> new Variant(Unsigned.uint(toLong(value)));
            case INT64 -> new Variant(toLong(value));
            case UINT64 -> new Variant(Unsigned.ulong(toLong(value)));
            case FLOAT -> new Variant((float) toDouble(value));
            case DOUBLE -> new Variant(toDouble(value));
            case STRING -> new Variant(value.toString());
            case DATETIME -> new Variant(toDateTime(value));
            case BYTESTRING -> new Variant(toByteString(value));
            case AUTO -> new Variant(value);
        };
    }

    private static NodeId resolveNodeId(DataPoint point, Map<String, Object> config) {
        String explicit = firstNonBlank(
                asString(config.get("nodeId")),
                asString(config.get("id")),
                point.getAddress()
        );
        if (explicit != null && !explicit.isBlank()) {
            return parseNodeId(explicit.trim());
        }

        int namespace = parseInt(firstPresent(config, "namespace", "ns"), 0);
        Object identifier = firstPresent(config, "identifier", "id");
        if (identifier == null) {
            throw new IllegalArgumentException("OPC UA point missing nodeId/identifier: " + point.getPointId());
        }
        String identifierType = asString(firstPresent(config, "identifierType", "idType"));
        if (identifierType == null || identifierType.isBlank()) {
            identifierType = "s";
        }
        return buildNodeId(namespace, identifierType.trim(), identifier);
    }

    private static OpcUaDataType resolveDataType(DataPoint point, Map<String, Object> config) {
        Object type = firstPresent(config, "opcUaType", "opcType", "nodeType", "dataType");
        if (type == null) {
            type = point.getDataType();
        }
        return OpcUaDataType.fromText(type != null ? type.toString() : null);
    }

    private static boolean resolveSubscription(DataPoint point, Map<String, Object> config) {
        Object flag = firstPresent(config, "subscribe", "monitor");
        if (flag != null) {
            return parseBoolean(flag);
        }
        String mode = point.getCollectionMode();
        return mode != null && mode.equalsIgnoreCase("SUBSCRIPTION");
    }

    private static NodeId parseNodeId(String text) {
        try {
            return NodeId.parse(text);
        } catch (Exception ex) {
            if (text.matches("\\d+")) {
                return new NodeId(0, Integer.parseInt(text));
            }
            throw new IllegalArgumentException("Invalid OPC UA NodeId: " + text, ex);
        }
    }

    private static NodeId buildNodeId(int namespace, String identifierType, Object identifier) {
        String type = identifierType.toLowerCase();
        return switch (type) {
            case "i", "n", "numeric" -> {
                long value = toLong(identifier);
                if (value <= Integer.MAX_VALUE) {
                    yield new NodeId(namespace, (int) value);
                }
                yield new NodeId(namespace, Unsigned.uint(value));
            }
            case "g", "guid" -> new NodeId(namespace, UUID.fromString(identifier.toString()));
            case "b", "bytes", "binary" -> new NodeId(namespace, toByteString(identifier));
            case "s", "string" -> new NodeId(namespace, identifier.toString());
            default -> throw new IllegalArgumentException("Unsupported NodeId type: " + identifierType);
        };
    }

    private static Object firstPresent(Map<String, Object> map, String... keys) {
        for (String key : keys) {
            if (map.containsKey(key)) {
                return map.get(key);
            }
        }
        return null;
    }

    private static String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return null;
    }

    private static String asString(Object value) {
        return value != null ? value.toString() : null;
    }

    private static boolean parseBoolean(Object flag) {
        if (flag instanceof Boolean bool) {
            return bool;
        }
        if (flag instanceof Number number) {
            return number.intValue() != 0;
        }
        return Boolean.parseBoolean(flag.toString());
    }

    private static int parseInt(Object value, int defaultValue) {
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

    private static double parseDouble(Object value, double defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        try {
            return Double.parseDouble(value.toString());
        } catch (NumberFormatException ex) {
            return defaultValue;
        }
    }

    private static long toLong(Object value) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        return Long.parseLong(value.toString());
    }

    private static double toDouble(Object value) {
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        return Double.parseDouble(value.toString());
    }

    private static boolean toBoolean(Object value) {
        if (value instanceof Boolean bool) {
            return bool;
        }
        if (value instanceof Number number) {
            return number.intValue() != 0;
        }
        String text = value.toString().trim().toLowerCase();
        return text.equals("true") || text.equals("1") || text.equals("on");
    }

    private static DateTime toDateTime(Object value) {
        if (value instanceof DateTime dateTime) {
            return dateTime;
        }
        if (value instanceof java.util.Date date) {
            return new DateTime(date.getTime());
        }
        if (value instanceof Number number) {
            return new DateTime(number.longValue());
        }
        if (value instanceof Instant instant) {
            return new DateTime(instant.toEpochMilli());
        }
        String text = value.toString().trim();
        try {
            Instant instant = Instant.parse(text);
            return new DateTime(instant.toEpochMilli());
        } catch (DateTimeParseException ignored) {
        }
        return new DateTime(Long.parseLong(text));
    }

    private static ByteString toByteString(Object value) {
        if (value instanceof ByteString byteString) {
            return byteString;
        }
        if (value instanceof byte[] bytes) {
            return ByteString.of(bytes);
        }
        if (value instanceof String text) {
            String trimmed = text.trim();
            try {
                byte[] decoded = Base64.getDecoder().decode(trimmed);
                return ByteString.of(decoded);
            } catch (IllegalArgumentException ex) {
                return ByteString.of(trimmed.getBytes(StandardCharsets.UTF_8));
            }
        }
        throw new IllegalArgumentException("Cannot convert to ByteString: " + Objects.toString(value));
    }
}

