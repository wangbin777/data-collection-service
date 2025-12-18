package com.wangbin.collector.core.collector.protocol.iec.domain;

import lombok.Data;

/**
 * IEC 104地址对象
 */
@Data
public class Iec104Address {
    private final int typeId;
    private final int ioAddress;

    public Iec104Address(int typeId, int ioAddress) {
        this.typeId = typeId;
        this.ioAddress = ioAddress;
    }
}
