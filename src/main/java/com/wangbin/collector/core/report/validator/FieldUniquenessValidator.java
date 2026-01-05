package com.wangbin.collector.core.report.validator;

import com.wangbin.collector.common.domain.entity.DataPoint;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 校验同一设备下的 pointAlias（reportField）唯一性。
 */
@Slf4j
@Component
public class FieldUniquenessValidator {

    public void validate(String deviceId, List<DataPoint> points) {
        if (points == null || points.isEmpty()) {
            return;
        }
        Map<String, List<DataPoint>> grouped = new HashMap<>();
        for (DataPoint point : points) {
            if (point == null) {
                continue;
            }
            String field = point.getReportField();
            if (field == null) {
                continue;
            }
            grouped.computeIfAbsent(field, k -> new java.util.ArrayList<>()).add(point);
        }
        for (Map.Entry<String, List<DataPoint>> entry : grouped.entrySet()) {
            List<DataPoint> list = entry.getValue();
            if (list.size() <= 1) {
                continue;
            }
            String field = entry.getKey();
            StringBuilder sb = new StringBuilder();
            for (DataPoint point : list) {
                point.setReportFieldConflict(true);
                sb.append(String.format("[pointId=%s, pointCode=%s, alias=%s] ",
                        point.getPointId(), point.getPointCode(), point.getPointAlias()));
            }
            log.error("设备 {} 的报告字段 '{}' 存在冲突，以下点位被降级为 raw-only：{}", deviceId, field, sb);
        }
    }
}
