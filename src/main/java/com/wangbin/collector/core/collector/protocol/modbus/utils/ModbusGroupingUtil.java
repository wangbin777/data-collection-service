package com.wangbin.collector.core.collector.protocol.modbus.utils;

import com.wangbin.collector.core.collector.protocol.modbus.domain.GroupedPoint;

import java.util.*;

/**
 * Modbus点位分组工具
 */
public class ModbusGroupingUtil {

    /**
     * 按连续地址分组
     */
    public static List<List<GroupedPoint>> groupByContinuousAddress(List<GroupedPoint> points) {
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
    }

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
        int maxAddress = pointGroup.stream()
                .mapToInt(gp -> gp.getAddress().getAddress())
                .max()
                .orElse(0);

        return new int[]{minAddress, maxAddress};
    }
}