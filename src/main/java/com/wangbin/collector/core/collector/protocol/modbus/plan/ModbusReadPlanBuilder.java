package com.wangbin.collector.core.collector.protocol.modbus.plan;

import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.core.collector.protocol.modbus.domain.*;
import com.wangbin.collector.core.collector.protocol.modbus.utils.ModbusGroupingUtil;

import java.util.*;
import java.util.function.Function;

public class ModbusReadPlanBuilder {

    public static List<ModbusReadPlan> build(String deviceId,List<DataPoint> points,
                                             Function<DataPoint, Integer> unitIdResolver,
                                             Function<String, ModbusAddress> addressParser) {
        Map<Integer, Map<RegisterType, List<GroupedPoint>>> grouped = new HashMap<>();
        for (DataPoint p : points) {
            ModbusAddress addr = addressParser.apply(p.getAddress());
            int unitId = unitIdResolver.apply(p);
            grouped.computeIfAbsent(unitId, k -> new EnumMap<>(RegisterType.class))
                    .computeIfAbsent(addr.getRegisterType(), k -> new ArrayList<>())
                    .add(new GroupedPoint(addr, p));
        }

        List<ModbusReadPlan> plans = new ArrayList<>();

        for (var unitEntry : grouped.entrySet()) {
            int unitId = unitEntry.getKey();

            for (var typeEntry : unitEntry.getValue().entrySet()) {
                RegisterType type = typeEntry.getKey();
                List<GroupedPoint> groupedPoints = typeEntry.getValue();

                // 🔴 线圈 / 离散量：强制单读
                if (type == RegisterType.COIL || type == RegisterType.DISCRETE_INPUT) {

                    for (GroupedPoint gp : groupedPoints) {
                        int start = gp.getAddress().getAddress();

                        plans.add(new ModbusReadPlan(
                                deviceId,
                                unitId,
                                type,
                                start,
                                1,
                                List.of(new PointOffset(
                                        gp.getPoint().getPointId(),
                                        0,
                                        gp.getPoint().getDataType()
                                ))
                        ));
                    }

                    continue;
                }

                // 🟢 寄存器：连续合并批量读
                List<List<GroupedPoint>> continuousGroups =
                        ModbusGroupingUtil.groupByContinuousAddress(groupedPoints);;

                for (List<GroupedPoint> group : continuousGroups) {

                    int[] range = ModbusGroupingUtil.getAddressRange(group);
                    int start = range[0];
                    int quantity = range[1];

                    List<PointOffset> offsets = new ArrayList<>();

                    for (GroupedPoint gp : group) {
                        offsets.add(new PointOffset(
                                gp.getPoint().getPointId(),
                                gp.getAddress().getAddress() - start,
                                gp.getPoint().getDataType()
                        ));
                    }

                    plans.add(new ModbusReadPlan(deviceId,
                            unitId,
                            type,
                            start,
                            quantity,
                            offsets
                    ));
                }


            }
        }

        return plans;
    }
}
