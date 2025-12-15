package com.wangbin.collector.common.enums;

import lombok.Getter;

@Getter
public enum DataType {
    INT16(2), UINT16(2),
    INT32(4), UINT32(4),
    FLOAT32(4), FLOAT32_SWAP(4), FLOAT32_LITTLE(4),
    FLOAT64(8), INT64(8), UINT64(8),
    BOOLEAN(1), STRING(1);

    /**
     * -- GETTER --
     *  返回数据类型最小字节长度
     */
    private final int minBytes;

    DataType(int minBytes) {
        this.minBytes = minBytes;
    }

    /**
     * 返回数据类型对应的寄存器数量（Modbus寄存器每个2字节）
     */
    public int getRegisterCount() {
        return (int) Math.ceil((double) minBytes / 2);
    }

    /**
     * 根据字符串获取枚举
     */
    public static DataType fromString(String type) {
        if (type == null) return null;
        return switch (type.toUpperCase()) {
            case "INT16" -> INT16;
            case "UINT16" -> UINT16;
            case "INT32" -> INT32;
            case "UINT32" -> UINT32;
            case "FLOAT32" -> FLOAT32;
            case "FLOAT32_SWAP" -> FLOAT32_SWAP;
            case "FLOAT32_LITTLE" -> FLOAT32_LITTLE;
            case "FLOAT64" -> FLOAT64;
            case "INT64" -> INT64;
            case "UINT64" -> UINT64;
            case "BOOLEAN" -> BOOLEAN;
            case "STRING" -> STRING;
            default -> null;
        };
    }
}
