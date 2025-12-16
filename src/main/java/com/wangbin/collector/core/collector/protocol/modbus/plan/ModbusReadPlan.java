package com.wangbin.collector.core.collector.protocol.modbus.plan;

import com.wangbin.collector.core.collector.protocol.modbus.domain.RegisterType;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;

@Getter
@AllArgsConstructor
public class ModbusReadPlan {

    private final int unitId;
    private final RegisterType registerType;

    private final int startAddress;
    private final int quantity;

    private final List<PointOffset> points;
}
