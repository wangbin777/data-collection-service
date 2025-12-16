package com.wangbin.collector.common.enums;

import lombok.Getter;

@Getter
public enum Parity {
    none(0, "无校验"),
    even(1, "偶校验"),
    odd(2, "奇校验");

    private final int value;
    private final String description;

    Parity(int value, String description) {
        this.value = value;
        this.description = description;
    }

    // 根据值获取枚举
    public static Parity fromValue(int value) {
        for (Parity parity : values()) {
            if (parity.value == value) {
                return parity;
            }
        }
        throw new IllegalArgumentException("无效的校验值: " + value);
    }

    // 根据名称获取枚举（已经是小写，直接匹配）
    public static Parity fromName(String name) {
        for (Parity parity : values()) {
            if (parity.name().equals(name)) {
                return parity;
            }
        }
        throw new IllegalArgumentException("无效的校验类型: " + name);
    }

    // 计算校验位
    public int calculateParityBit(byte data, int dataBits) {
        if (this == none) {
            return 0;
        }

        // 计算数据位中1的个数
        int onesCount = 0;
        for (int i = 0; i < dataBits; i++) {
            if (((data >> i) & 1) == 1) {
                onesCount++;
            }
        }

        if (this == even) {
            return (onesCount % 2 == 0) ? 0 : 1;
        } else { // odd
            return (onesCount % 2 == 0) ? 1 : 0;
        }
    }

    // 添加校验位到数据
    public byte addParity(byte data, int dataBits) {
        if (this == none) {
            return data;
        }

        int parityBit = calculateParityBit(data, dataBits);
        // 将校验位放在最高位（通常是第8位）
        return (byte) ((data & ((1 << dataBits) - 1)) | (parityBit << dataBits));
    }

    // 检查数据+校验位是否正确
    public boolean verifyParity(byte dataWithParity, int dataBits) {
        if (this == none) {
            return true; // 无校验总是返回true
        }

        // 分离数据和校验位
        byte data = (byte) (dataWithParity & ((1 << dataBits) - 1));
        int receivedParity = (dataWithParity >> dataBits) & 1;

        int calculatedParity = calculateParityBit(data, dataBits);
        return receivedParity == calculatedParity;
    }

    @Override
    public String toString() {
        return name() + "(" + value + ") - " + description;
    }
}
