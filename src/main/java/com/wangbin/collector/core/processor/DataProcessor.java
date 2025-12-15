package com.wangbin.collector.core.processor;

import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.common.domain.enums.DataQuality;

import java.util.Map;

/**
 * 数据处理器接口
 */
public interface DataProcessor {

    /**
     * 处理器名称
     */
    String getName();

    /**
     * 处理器类型
     */
    String getType();

    /**
     * 处理器描述
     */
    String getDescription();

    /**
     * 处理器优先级（数值越小优先级越高）
     */
    int getPriority();

    /**
     * 初始化处理器
     */
    void init(Map<String, Object> config);

    /**
     * 处理单个数据点
     */
    ProcessResult process(ProcessContext context, DataPoint point, Object rawValue);

    /**
     * 批量处理数据点
     */
    Map<String, ProcessResult> batchProcess(ProcessContext context,
                                            Map<DataPoint, Object> dataPoints);

    /**
     * 处理器是否启用
     */
    boolean isEnabled();

    /**
     * 启用/禁用处理器
     */
    void setEnabled(boolean enabled);

    /**
     * 获取处理器统计信息
     */
    Map<String, Object> getStatistics();

    /**
     * 重置统计信息
     */
    void resetStatistics();

    /**
     * 销毁处理器
     */
    void destroy();
}
