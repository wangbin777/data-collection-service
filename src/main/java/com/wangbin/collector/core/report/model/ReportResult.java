package com.wangbin.collector.core.report.model;

import lombok.Data;
import java.util.HashMap;
import java.util.Map;

/**
 * 上报结果模型
 */
@Data
public class ReportResult {
    private String pointCode;          // 点位编码
    private String targetId;           // 目标系统ID
    private boolean success;           // 是否成功
    private String errorMessage;       // 错误信息
    private long costTime;            // 耗时（毫秒）
    private Integer statusCode;       // 状态码（HTTP等）
    private String responseData;      // 响应数据
    private Map<String, Object> metadata = new HashMap<>(); // 附加信息

    // 成功结果
    public static ReportResult success(String pointCode, String targetId) {
        ReportResult result = new ReportResult();
        result.setPointCode(pointCode);
        result.setTargetId(targetId);
        result.setSuccess(true);
        result.setCostTime(0);
        return result;
    }

    // 错误结果
    public static ReportResult error(String pointCode, String errorMessage, String targetId) {
        ReportResult result = new ReportResult();
        result.setPointCode(pointCode);
        result.setTargetId(targetId);
        result.setSuccess(false);
        result.setErrorMessage(errorMessage);
        result.setCostTime(0);
        return result;
    }

    // 添加元数据
    public void addMetadata(String key, Object value) {
        if (metadata == null) {
            metadata = new HashMap<>();
        }
        metadata.put(key, value);
    }
}