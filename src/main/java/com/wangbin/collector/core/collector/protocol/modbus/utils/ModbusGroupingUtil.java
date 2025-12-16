package com.wangbin.collector.core.collector.protocol.modbus.utils;

import com.wangbin.collector.common.enums.DataType;
import com.wangbin.collector.core.collector.protocol.modbus.domain.GroupedPoint;

import java.util.*;

/**
 * Modbus点位分组工具
 */
public class ModbusGroupingUtil {

    /**
     * 按连续地址分组
     */
    /**
     * 按连续地址分组（考虑数据类型长度）
     * 确保地址不重叠，连续读取
     */
    public static List<List<GroupedPoint>> groupByContinuousAddress(List<GroupedPoint> points) {
        if (points == null || points.isEmpty()) {
            return Collections.emptyList();
        }

        // 1. 按地址排序
        points.sort(Comparator.comparingInt(o -> o.getAddress().getAddress()));

        List<List<GroupedPoint>> groups = new ArrayList<>();
        List<GroupedPoint> currentGroup = new ArrayList<>();

        int groupStartAddress = -1;
        int groupEndAddress = -1;  // 当前组结束地址（最后一个字节地址）

        for (GroupedPoint gp : points) {
            int currentAddress = gp.getAddress().getAddress();
            int registerCount = DataType.valueOf(gp.getPoint().getDataType()).getRegisterCount();
            int dataEndAddress = currentAddress + registerCount - 1;  // 数据占用的最后一个寄存器地址

            if (currentGroup.isEmpty()) {
                // 第一个点，开始新组
                currentGroup.add(gp);
                groupStartAddress = currentAddress;
                groupEndAddress = dataEndAddress;
            } else {
                // 检查地址是否连续且不重叠
                if (currentAddress == groupEndAddress + 1) {
                    // 地址连续，加入当前组
                    currentGroup.add(gp);
                    groupEndAddress = dataEndAddress;
                } else {
                    // 地址不连续或重叠，开始新组
                    groups.add(currentGroup);
                    currentGroup = new ArrayList<>();
                    currentGroup.add(gp);
                    groupStartAddress = currentAddress;
                    groupEndAddress = dataEndAddress;
                }
            }
        }

        if (!currentGroup.isEmpty()) {
            groups.add(currentGroup);
        }

        return groups;
    }
    /*public static List<List<GroupedPoint>> groupByContinuousAddress(List<GroupedPoint> points) {
        if (points == null || points.isEmpty()) {
            return Collections.emptyList();
        }

        // 按地址排序
        points.sort(Comparator.comparingInt(o -> o.getAddress().getAddress()));

        List<List<GroupedPoint>> groups = new ArrayList<>();
        List<GroupedPoint> currentGroup = new ArrayList<>();
        int lastAddress = Integer.MIN_VALUE;

        for (GroupedPoint gp : points) {
            int addr = gp.getAddress().getAddress();

            if (currentGroup.isEmpty()) {
                currentGroup.add(gp);
            } else {
                if (addr == lastAddress + 1) {
                    currentGroup.add(gp);
                } else {
                    groups.add(currentGroup);
                    currentGroup = new ArrayList<>();
                    currentGroup.add(gp);
                }
            }

            lastAddress = addr;
        }

        if (!currentGroup.isEmpty()) {
            groups.add(currentGroup);
        }

        return groups;
    }*/

    /**
     * 获取组的最小和最大地址
     */
    public static int[] getAddressRange(List<GroupedPoint> pointGroup) {
        if (pointGroup.isEmpty()) {
            return new int[]{0, 0};
        }

        int minAddress = pointGroup.stream()
                .mapToInt(gp -> gp.getAddress().getAddress())
                .min()
                .orElse(0);
        /*int maxAddress = pointGroup.stream()
                .mapToInt(gp -> gp.getAddress().getAddress())
                .max()
                .orElse(0);*/

        int registerCount  = pointGroup.stream()
                .mapToInt(gp -> DataType.fromString(gp.getPoint().getDataType()).getRegisterCount())
                .max()
                .orElse(0);


        return new int[]{minAddress, registerCount};
    }
}