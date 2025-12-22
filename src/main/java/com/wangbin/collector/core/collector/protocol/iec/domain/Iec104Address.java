package com.wangbin.collector.core.collector.protocol.iec.domain;

import lombok.Getter;

/**
 * IEC 104 地址对象，包含 Common Address 与 IOA。
 */
@Getter
public class Iec104Address {

    /** 公共地址（Common Address Of ASDU） */
    private final int commonAddress;

    /** 信息对象地址（IOA） */
    private final int ioAddress;

    /** 点位类型 ID，可用于写命令 */
    private final Integer typeId;

    public Iec104Address(int commonAddress, int ioAddress, Integer typeId) {
        this.commonAddress = commonAddress;
        this.ioAddress = ioAddress;
        this.typeId = typeId;
    }
}
