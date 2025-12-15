package com.wangbin.collector.core.processor;

import com.wangbin.collector.common.domain.enums.DataQuality;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;

/**
 * 数据处理结果
 */
@Data
public class ProcessResult {

    // 处理状态
    private boolean success;

    // 原始值
    private Object rawValue;

    // 处理后的值
    private Object processedValue;

    // 数据质量
    private int quality = 100;

    // 质量描述
    private String qualityDescription;

    // 处理器名称
    private String processorName;

    // 处理时间（毫秒）
    private long processingTime;

    // 处理消息
    private String message;

    // 错误信息
    private String error;

    // 错误码
    private String errorCode;

    // 附加数据
    private Map<String, Object> metadata = new HashMap<>();

    public ProcessResult() {
        this.processorName = "Unknown";
        this.processingTime = 0;
    }

    public ProcessResult(boolean success, Object rawValue, Object processedValue) {
        this();
        this.success = success;
        this.rawValue = rawValue;
        this.processedValue = processedValue;
        this.quality = success ? 100 : 0;
    }

    /**
     * 创建成功结果
     */
    public static ProcessResult success(Object rawValue, Object processedValue) {
        ProcessResult result = new ProcessResult(true, rawValue, processedValue);
        result.setMessage("处理成功");
        return result;
    }

    public static ProcessResult success(Object rawValue, Object processedValue, String message) {
        ProcessResult result = success(rawValue, processedValue);
        result.setMessage(message);
        return result;
    }

    /**
     * 创建错误结果
     */
    public static ProcessResult error(Object rawValue, String error) {
        ProcessResult result = new ProcessResult(false, rawValue, rawValue);
        result.setError(error);
        result.setMessage("处理失败: " + error);
        result.setQuality(0);
        return result;
    }

    public static ProcessResult error(Object rawValue, String error, DataQuality dataQuality) {
        ProcessResult result = error(rawValue, error);
        result.setQuality(dataQuality.getCode());
        result.setQualityDescription(dataQuality.getDescription());
        return result;
    }

    /**
     * 创建跳过结果
     */
    public static ProcessResult skip(Object rawValue, String reason) {
        ProcessResult result = new ProcessResult(true, rawValue, rawValue);
        result.setMessage("跳过处理: " + reason);
        result.setQuality(100);
        return result;
    }

    /**
     * 创建警告结果
     */
    public static ProcessResult warning(Object rawValue, Object processedValue, String warning) {
        ProcessResult result = new ProcessResult(true, rawValue, processedValue);
        result.setMessage("处理警告: " + warning);
        result.setQuality(80); // 警告质量降低
        return result;
    }

    /**
     * 获取最终值（如果处理成功返回处理后的值，否则返回原始值）
     */
    public Object getFinalValue() {
        return success ? processedValue : rawValue;
    }

    /**
     * 获取最终值（带默认值）
     */
    public Object getFinalValue(Object defaultValue) {
        Object value = getFinalValue();
        return value != null ? value : defaultValue;
    }

    /**
     * 添加元数据
     */
    public void addMetadata(String key, Object value) {
        metadata.put(key, value);
    }

    /**
     * 获取元数据
     */
    @SuppressWarnings("unchecked")
    public <T> T getMetadata(String key) {
        return (T) metadata.get(key);
    }

    /**
     * 获取元数据（带默认值）
     */
    @SuppressWarnings("unchecked")
    public <T> T getMetadata(String key, T defaultValue) {
        T value = (T) metadata.get(key);
        return value != null ? value : defaultValue;
    }

    /**
     * 获取质量等级
     */
    public String getQualityLevel() {
        if (quality >= 90) return "A";
        if (quality >= 80) return "B";
        if (quality >= 70) return "C";
        if (quality >= 60) return "D";
        return "F";
    }

    /**
     * 判断质量是否可接受
     */
    public boolean isQualityAcceptable() {
        return quality >= 60;
    }

    /**
     * 判断是否为跳过结果
     */
    public boolean isSkipped() {
        return success && rawValue == processedValue && message != null && message.startsWith("跳过处理");
    }

    /**
     * 合并结果
     */
    public ProcessResult merge(ProcessResult other) {
        if (other == null) {
            return this;
        }

        ProcessResult merged = new ProcessResult();
        merged.success = this.success && other.success;
        merged.rawValue = this.rawValue;
        merged.processedValue = other.processedValue;
        merged.quality = Math.min(this.quality, other.quality);
        merged.processorName = this.processorName + " -> " + other.processorName;
        merged.processingTime = this.processingTime + other.processingTime;

        // 合并消息
        if (this.message != null && other.message != null) {
            merged.message = this.message + " | " + other.message;
        } else if (this.message != null) {
            merged.message = this.message;
        } else {
            merged.message = other.message;
        }

        // 合并错误
        if (this.error != null && other.error != null) {
            merged.error = this.error + " | " + other.error;
        } else if (this.error != null) {
            merged.error = this.error;
        } else {
            merged.error = other.error;
        }

        // 合并元数据
        merged.metadata.putAll(this.metadata);
        merged.metadata.putAll(other.metadata);

        return merged;
    }

    /**
     * 创建结果摘要
     */
    public String getSummary() {
        return String.format(
                "ProcessResult{success=%s, quality=%d(%s), raw=%s, processed=%s, time=%dms, message=%s}",
                success, quality, getQualityLevel(),
                formatValue(rawValue), formatValue(processedValue),
                processingTime, message
        );
    }

    private String formatValue(Object value) {
        if (value == null) return "null";
        if (value instanceof String) {
            String str = (String) value;
            return str.length() > 20 ? str.substring(0, 20) + "..." : str;
        }
        return value.toString();
    }
}
