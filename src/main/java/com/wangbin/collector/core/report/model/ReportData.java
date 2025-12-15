package com.wangbin.collector.core.report.model;

import lombok.Data;
import java.util.HashMap;
import java.util.Map;

/**
 * 上报数据模型
 */
@Data
public class ReportData {
    private String pointCode;          // 点位编码
    private String pointName;          // 点位名称
    private Object value;             // 数据值
    private long timestamp;           // 时间戳
    private DataQuality quality = DataQuality.GOOD; // 数据质量
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

    public enum DataQuality {
        GOOD, BAD, UNCERTAIN
    }
}