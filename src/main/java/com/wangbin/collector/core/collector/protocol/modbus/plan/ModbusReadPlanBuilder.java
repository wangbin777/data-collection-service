package com.wangbin.collector.core.collector.protocol.modbus.plan;

import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.common.enums.DataType;
import com.wangbin.collector.core.collector.protocol.modbus.domain.*;
import com.wangbin.collector.core.collector.protocol.modbus.utils.ModbusGroupingUtil;

import java.util.*;
import java.util.function.Function;

public class ModbusReadPlanBuilder {

    private static final int MAX_REGISTER_QUANTITY = 125;
    private static final int MAX_COIL_QUANTITY = 2000;

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

                List<List<GroupedPoint>> continuousGroups =
                        ModbusGroupingUtil.groupByContinuousAddress(groupedPoints);

                for (List<GroupedPoint> group : continuousGroups) {
                    buildPlansForGroup(deviceId, unitId, type, group, plans);
                }


            }
        }

        return plans;
    }

    private static void buildPlansForGroup(String deviceId,
                                           int unitId,
                                           RegisterType type,
                                           List<GroupedPoint> group,
                                           List<ModbusReadPlan> plans) {
        if (group.isEmpty()) {
            return;
        }

        int limit = getRegisterLimit(type);
        List<GroupedPoint> chunk = new ArrayList<>();
        int chunkStart = -1;
        int chunkQuantity = 0;

        for (GroupedPoint gp : group) {
            int address = gp.getAddress().getAddress();
            int registerCount = DataType.fromString(gp.getPoint().getDataType()).getRegisterCount();

            if (chunk.isEmpty()) {
                chunk.add(gp);
                chunkStart = address;
                chunkQuantity = registerCount;
                continue;
            }

            int expectedAddress = chunkStart + chunkQuantity;
            boolean contiguous = address == expectedAddress;
            boolean exceedsLimit = chunkQuantity + registerCount > limit;

            if (!contiguous || exceedsLimit) {
                plans.add(buildPlan(deviceId, unitId, type, chunkStart, chunkQuantity, chunk));
                chunk = new ArrayList<>();
                chunk.add(gp);
                chunkStart = address;
                chunkQuantity = registerCount;
            } else {
                chunk.add(gp);
                chunkQuantity += registerCount;
            }
        }

        if (!chunk.isEmpty()) {
            plans.add(buildPlan(deviceId, unitId, type, chunkStart, chunkQuantity, chunk));
        }
    }

    private static ModbusReadPlan buildPlan(String deviceId,
                                            int unitId,
                                            RegisterType type,
                                            int start,
                                            int quantity,
                                            List<GroupedPoint> chunk) {
        List<PointOffset> offsets = new ArrayList<>();
        for (GroupedPoint gp : chunk) {
            offsets.add(new PointOffset(
                    gp.getPoint().getPointId(),
                    gp.getAddress().getAddress() - start,
                    gp.getPoint().getDataType()
            ));
        }

        return new ModbusReadPlan(
                deviceId,
                unitId,
                type,
                start,
                quantity,
                offsets
        );
    }

    private static int getRegisterLimit(RegisterType type) {
        return switch (type) {
            case COIL, DISCRETE_INPUT -> MAX_COIL_QUANTITY;
            default -> MAX_REGISTER_QUANTITY;
        };
    }
}
