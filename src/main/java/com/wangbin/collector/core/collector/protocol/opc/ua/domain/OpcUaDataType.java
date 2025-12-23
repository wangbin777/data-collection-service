package com.wangbin.collector.core.collector.protocol.opc.ua.domain;

import java.util.Locale;

/**
 * OPC UA data type definition used when converting Java values to Variant.
 */
public enum OpcUaDataType {
    AUTO,
    BOOLEAN,
    BYTE,
    SBYTE,
    INT16,
    UINT16,
    INT32,
    UINT32,
    INT64,
    UINT64,
    FLOAT,
    DOUBLE,
    STRING,
    DATETIME,
    BYTESTRING;

    public static OpcUaDataType fromText(String text) {
        if (text == null || text.isBlank()) {
            return AUTO;
        }
        String normalized = text.trim().toUpperCase(Locale.ROOT);
        for (OpcUaDataType type : values()) {
            if (type.name().equals(normalized)) {
                return type;
            }
        }
        return AUTO;
    }
}

