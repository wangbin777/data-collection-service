package com.wangbin.collector.core.report.model;

import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.common.enums.QualityEnum;
import com.wangbin.collector.core.processor.ProcessResult;
import lombok.Data;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * 上报数据模型
 */
@Data
public class ReportData {
    private String deviceId;          // 点位id
    private String pointId;          // 点位id
    private String pointCode;          // 点位编码
    private String pointName;          // 点位名称
    private Object value;             // 数据值
    private long timestamp;           // 时间戳
    private String method;            //方法
    private String quality = String.valueOf(QualityEnum.GOOD.getCode()); // 数据质量
    private Map<String, Object> metadata = new HashMap<>(); // 元数据

    // 获取数据大小（估算）
    public int getDataSize() {
        int size = 0;
        if (pointCode != null) size += pointCode.getBytes().length;
        if (pointName != null) size += pointName.getBytes().length;
        if (value != null) size += value.toString().getBytes().length;
        return size;
    }

    // 添加元数据
    public void addMetadata(String key, Object value) {
        if (metadata == null) {
            metadata = new HashMap<>();
        }
        metadata.put(key, value);
    }


    public static ReportData buildReportData(String deviceId, String method, DataPoint point, Object cacheValue) {
        Object value = extractValue(cacheValue);
        if (value == null) {
            return null;
        }

        ReportData reportData = new ReportData();
        reportData.setDeviceId(deviceId);
        reportData.setPointId(point.getPointId());
        reportData.setPointCode(Optional.ofNullable(point.getPointCode()).orElse(point.getPointId()));
        reportData.setPointName(point.getPointName());
        reportData.setValue(value);
        reportData.setTimestamp(System.currentTimeMillis());

        reportData.addMetadata("pointId", point.getPointId());
        reportData.addMetadata("address", point.getAddress());
        reportData.addMetadata("unit", point.getUnit());
        reportData.addMetadata("dataType", point.getDataType());
        reportData.addMetadata("pointCode", point.getPointCode());
        reportData.addMetadata("pointName", point.getPointName());

        reportData.setMethod(method);

        ProcessResult result = (ProcessResult) cacheValue;
        reportData.addMetadata("val", result.getFinalValue());
        //reportData.addMetadata("timestamp", result.getProcessingTime());
        //reportData.addMetadata("timestamp", System.currentTimeMillis());
        reportData.addMetadata("quality", QualityEnum.fromCode(result.getQuality()).getText());
        reportData.setQuality(QualityEnum.fromCode(result.getQuality()).getText());

        if (result.getMetadata() != null && !result.getMetadata().isEmpty()) {
            reportData.getMetadata().putAll(result.getMetadata());
        }
        if (result.getMessage() != null) {
            reportData.addMetadata("message", result.getMessage());
        }
        return reportData;
    }

    private static Object extractValue(Object cacheValue) {
        if (cacheValue instanceof ProcessResult processResult) {
            return processResult.getFinalValue();
        }
        return cacheValue;
    }
}