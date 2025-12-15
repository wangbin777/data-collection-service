package com.wangbin.collector.core.processor;

import com.wangbin.collector.common.domain.entity.DeviceInfo;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;

/**
 * 数据处理上下文
 */
@Data
public class ProcessContext {

    // 设备信息
    private DeviceInfo deviceInfo;

    // 采集时间
    private long collectTime;

    // 处理时间
    private long processTime;

    // 原始数据质量
    private int rawQuality = 100;

    // 处理链标识
    private String chainId;

    // 处理阶段
    private String stage;

    // 附加属性
    private Map<String, Object> attributes = new HashMap<>();

    // 处理历史
    private Map<String, ProcessResult> processHistory = new HashMap<>();

    // 处理选项
    private Map<String, Object> options = new HashMap<>();

    public ProcessContext() {
        this.processTime = System.currentTimeMillis();
    }

    public ProcessContext(DeviceInfo deviceInfo, long collectTime) {
        this();
        this.deviceInfo = deviceInfo;
        this.collectTime = collectTime;
    }

    /**
     * 添加上下文属性
     */
    public void addAttribute(String key, Object value) {
        attributes.put(key, value);
    }

    /**
     * 获取上下文属性
     */
    @SuppressWarnings("unchecked")
    public <T> T getAttribute(String key) {
        return (T) attributes.get(key);
    }

    /**
     * 获取上下文属性（带默认值）
     */
    @SuppressWarnings("unchecked")
    public <T> T getAttribute(String key, T defaultValue) {
        T value = (T) attributes.get(key);
        return value != null ? value : defaultValue;
    }

    /**
     * 添加处理历史
     */
    public void addProcessHistory(String processorName, ProcessResult result) {
        processHistory.put(processorName, result);
    }

    /**
     * 获取处理历史
     */
    public ProcessResult getProcessHistory(String processorName) {
        return processHistory.get(processorName);
    }

    /**
     * 设置处理选项
     */
    public void setOption(String key, Object value) {
        options.put(key, value);
    }

    /**
     * 获取处理选项
     */
    @SuppressWarnings("unchecked")
    public <T> T getOption(String key) {
        return (T) options.get(key);
    }

    /**
     * 获取处理选项（带默认值）
     */
    @SuppressWarnings("unchecked")
    public <T> T getOption(String key, T defaultValue) {
        T value = (T) options.get(key);
        return value != null ? value : defaultValue;
    }

    /**
     * 获取延迟时间（从采集到当前处理的时间）
     */
    public long getLatency() {
        return processTime - collectTime;
    }

    /**
     * 克隆上下文（用于并行处理）
     */
    public ProcessContext clone() {
        ProcessContext clone = new ProcessContext();
        clone.deviceInfo = this.deviceInfo;
        clone.collectTime = this.collectTime;
        clone.processTime = this.processTime;
        clone.rawQuality = this.rawQuality;
        clone.chainId = this.chainId;
        clone.stage = this.stage;
        clone.attributes = new HashMap<>(this.attributes);
        clone.processHistory = new HashMap<>(this.processHistory);
        clone.options = new HashMap<>(this.options);
        return clone;
    }

    /**
     * 创建简单上下文
     */
    public static ProcessContext simpleContext(String deviceId) {
        ProcessContext context = new ProcessContext();
        context.addAttribute("deviceId", deviceId);
        return context;
    }

    /**
     * 创建设备上下文
     */
    public static ProcessContext deviceContext(DeviceInfo deviceInfo) {
        return new ProcessContext(deviceInfo, System.currentTimeMillis());
    }
}
