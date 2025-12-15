package com.wangbin.collector.core.collector.protocol.modbus.domain;

public enum RegisterType {
    COIL(0, "线圈"),
    DISCRETE_INPUT(1, "离散输入"),
    HOLDING_REGISTER(3, "保持寄存器"),
    INPUT_REGISTER(4, "输入寄存器");

    private final int code;
    private final String description;

    RegisterType(int code, String description) {
        this.code = code;
        this.description = description;
    }

    public static RegisterType fromCode(int code) {
        for (RegisterType type : values()) {
            if (type.code == code) {
                return type;
            }
        }
        return null;
    }
}
