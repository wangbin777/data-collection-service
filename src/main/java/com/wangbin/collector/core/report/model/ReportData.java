package com.wangbin.collector.core.report.model;

import com.wangbin.collector.common.constant.MessageConstant;
import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.common.enums.QualityEnum;
import com.wangbin.collector.core.processor.ProcessResult;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * 统一的设备上报数据模型：支持单点与快照。
 */
@Data
public class ReportData {

    private static final Logger log = LoggerFactory.getLogger(ReportData.class);

    private String deviceId;
    private String pointCode;
    private String pointId;
    private String pointName;
    private Object value;
    private long timestamp;
    private String method = MessageConstant.MESSAGE_TYPE_PROPERTY_POST;
    private String quality = QualityEnum.GOOD.getText();
    private Map<String, Object> metadata = new LinkedHashMap<>();
    private final Map<String, Object> properties = new LinkedHashMap<>();
    private final Map<String, String> propertyQuality = new LinkedHashMap<>();
    private final Map<String, Long> propertyTs = new LinkedHashMap<>();

    public void addMetadata(String key, Object v) {
        metadata.put(key, v);
    }

    public void addProperty(String field, Object fieldValue, long fieldTimestamp, String qualityText) {
        if (field == null || field.trim().isEmpty()) {
            return;
        }
        if (properties.containsKey(field)) {
            log.warn("重复字段 {}，忽略后续写入", field);
            return;
        }
        properties.put(field, fieldValue);
        propertyTs.put(field, fieldTimestamp);
        if (qualityText != null) {
            propertyQuality.put(field, qualityText);
        }
    }

    public boolean hasProperties() {
        return !properties.isEmpty();
    }

    public int size() {
        return properties.size();
    }

    public int estimatePayloadSize() {
        int size = 0;
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            size += entry.getKey().getBytes(StandardCharsets.UTF_8).length;
            Object v = entry.getValue();
            if (v != null) {
                size += v.toString().getBytes(StandardCharsets.UTF_8).length;
            }
        }
        return size;
    }

    /**
     * 兼容旧逻辑的大小估算
     */
    public int getDataSize() {
        if (hasProperties()) {
            return estimatePayloadSize();
        }
        int size = 0;
        if (pointCode != null) {
            size += pointCode.getBytes(StandardCharsets.UTF_8).length;
        }
        if (pointName != null) {
            size += pointName.getBytes(StandardCharsets.UTF_8).length;
        }
        if (value != null) {
            size += value.toString().getBytes(StandardCharsets.UTF_8).length;
        }
        return size;
    }

    public ReportData shallowCopy() {
        ReportData copy = new ReportData();
        copy.setDeviceId(deviceId);
        copy.setPointCode(pointCode);
        copy.setPointId(pointId);
        copy.setPointName(pointName);
        copy.setValue(value);
        copy.setTimestamp(timestamp);
        copy.setMethod(method);
        copy.setQuality(quality);
        copy.getMetadata().putAll(metadata);
        return copy;
    }

    public Map<String, Object> getProperties() {
        return Collections.unmodifiableMap(properties);
    }

    public Map<String, String> getPropertyQuality() {
        return Collections.unmodifiableMap(propertyQuality);
    }

    public Map<String, Long> getPropertyTs() {
        return Collections.unmodifiableMap(propertyTs);
    }

    public void applyChunkMetadata(String batchId, int chunkIndex, int chunkTotal) {
        metadata.put("batchId", batchId == null ? UUID.randomUUID().toString() : batchId);
        metadata.put("chunkIndex", chunkIndex);
        metadata.put("chunkTotal", chunkTotal);
    }

    public String getBatchId() {
        Object id = metadata.get("batchId");
        return id != null ? String.valueOf(id) : null;
    }

    public static ReportData buildReportData(String deviceId, String method, DataPoint point, Object cacheValue) {
        Object value = extractValue(cacheValue);
        if (value == null) {
            return null;
        }

        ReportData reportData = new ReportData();
        reportData.setDeviceId(deviceId);
        applyPointInfo(reportData, point);
        reportData.setValue(value);
        reportData.setTimestamp(System.currentTimeMillis());
        reportData.setMethod(method);

        ProcessResult result = cacheValue instanceof ProcessResult processResult ? processResult : null;
        QualityEnum qualityEnum = result != null ? QualityEnum.fromCode(result.getQuality()) : QualityEnum.GOOD;
        reportData.setQuality(qualityEnum.getText());

        reportData.addMetadata("pointId", point.getPointId());
        reportData.addMetadata("address", point.getAddress());
        reportData.addMetadata("unit", point.getUnit());
        reportData.addMetadata("dataType", point.getDataType());
        reportData.addMetadata("pointCode", resolvePointCode(point));
        reportData.addMetadata("pointName", point.getPointName());

        long valueTimestamp = System.currentTimeMillis();
        if (result != null) {
            reportData.addMetadata("val", result.getFinalValue());
            reportData.addMetadata("quality", qualityEnum.getText());
            if (result.getMetadata() != null && !result.getMetadata().isEmpty()) {
                reportData.getMetadata().putAll(result.getMetadata());
            }
            if (result.getMessage() != null) {
                reportData.addMetadata("message", result.getMessage());
            }
        }

        String field = Optional.ofNullable(point.getReportField()).orElse(reportData.getPointCode());
        reportData.addProperty(field, value, valueTimestamp, qualityEnum.getText());
        return reportData;
    }

    public static void applyPointInfo(ReportData target, DataPoint point) {
        if (target == null || point == null) {
            return;
        }
        applyPointInfo(target, point.getPointId(), resolvePointCode(point), point.getPointName());
    }

    public static void applyPointInfo(ReportData target, String pointId, String pointCode, String pointName) {
        if (target == null) {
            return;
        }
        target.setPointId(pointId);
        target.setPointCode(pointCode);
        target.setPointName(pointName);
    }

    private static String resolvePointCode(DataPoint point) {
        if (point == null) {
            return null;
        }
        String pointCode = point.getPointCode();
        if (pointCode != null && !pointCode.trim().isEmpty()) {
            return pointCode;
        }
        return point.getPointId();
    }

    private static Object extractValue(Object cacheValue) {
        if (cacheValue instanceof ProcessResult processResult) {
            return processResult.getFinalValue();
        }
        return cacheValue;
    }
}
