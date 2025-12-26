package com.wangbin.collector.core.cache.model;

import lombok.Data;
import java.io.Serializable;
import java.util.Map;

/**
 * 点位数据模型
 * 用于封装采集到的单点位数据
 */
@Data
public class PointData implements Serializable {

    private static final long serialVersionUID = 1L;

    // 设备ID
    private String deviceId;

    // 点位ID
    private String pointId;

    // 点位名称
    private String pointName;

    // 采集值
    private Object value;

    // 采集时间戳（毫秒）
    private long timestamp;

    // 数据质量（0-100，100表示完全正常）
    private int quality = 100;

    // 单位
    private String unit;

    // 元数据
    private Map<String, Object> metadata;
}