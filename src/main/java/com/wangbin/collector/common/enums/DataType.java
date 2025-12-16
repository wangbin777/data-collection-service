package com.wangbin.collector.common.enums;

import lombok.Getter;

@Getter
public enum DataType {
    INT16(2), UINT16(2),
    INT32(4), UINT32(4),
    FLOAT(4), FLOAT32(4), FLOAT32_SWAP(4), FLOAT32_LITTLE(4),
    FLOAT64(8), INT64(8), UINT64(8),
    BOOLEAN(1), STRING(1),
    /*BYTE(1), UINT8(1), INT8(1),
    SHORT(2), SHORT_SWAP(2), UINT16_SWAP(2),
    LONG(4), LONG_SWAP(4), INT32_SWAP(4), UINT32_SWAP(4),
    DOUBLE(8), DOUBLE_SWAP(8), FLOAT64_SWAP(8), FLOAT64_LITTLE(8),
    LONGLONG(8), LONGLONG_SWAP(8), INT64_SWAP(8), UINT64_SWAP(8),
    DWORD(4), QWORD(8),
    BCD16(2), BCD32(4),
    BIT(1), BIT_ARRAY(1),*/
    ;

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
        if (type == null || type.trim().isEmpty()) {
            return INT16;
        }
        try {
            return DataType.valueOf(type.toUpperCase().trim());
        } catch (IllegalArgumentException e) {
            return INT16;
        }
    }
}
