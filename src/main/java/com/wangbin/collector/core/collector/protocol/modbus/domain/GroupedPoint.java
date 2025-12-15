package com.wangbin.collector.core.collector.protocol.modbus.domain;

import com.wangbin.collector.common.domain.entity.DataPoint;
import lombok.Data;

@Data
public class GroupedPoint {
    final ModbusAddress address;
    final DataPoint point;

    public GroupedPoint(ModbusAddress address, DataPoint point) {
        this.address = address;
        this.point = point;
    }
}
