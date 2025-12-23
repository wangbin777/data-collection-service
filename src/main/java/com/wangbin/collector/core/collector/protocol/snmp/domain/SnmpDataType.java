package com.wangbin.collector.core.collector.protocol.snmp.domain;

/**
 * SNMP 数据类型枚举。
 */
public enum SnmpDataType {
    INTEGER,
    COUNTER32,
    COUNTER64,
    GAUGE32,
    TIMETICKS,
    OCTET_STRING,
    STRING,
    IP_ADDRESS,
    OID,
    NULL,
    AUTO;

    public static SnmpDataType fromText(String text) {
        if (text == null || text.isBlank()) {
            return AUTO;
        }
        String normalized = text.trim().toUpperCase();
        return switch (normalized) {
            case "INTEGER", "INT32" -> INTEGER;
            case "COUNTER32" -> COUNTER32;
            case "COUNTER64" -> COUNTER64;
            case "GAUGE32" -> GAUGE32;
            case "TIMETICKS" -> TIMETICKS;
            case "OCTETSTRING", "OCTET_STRING" -> OCTET_STRING;
            case "STRING" -> STRING;
            case "IPADDRESS", "IP_ADDRESS" -> IP_ADDRESS;
            case "OID" -> OID;
            case "NULL" -> NULL;
            default -> AUTO;
        };
    }
}
