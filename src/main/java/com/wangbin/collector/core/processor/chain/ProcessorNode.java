package com.wangbin.collector.core.processor.chain;

import lombok.Data;

/**
 * 处理器节点
 * 定义处理器链中的一个处理节点
 */
@Data
public class ProcessorNode {

    /**
     * 处理器名称（在DataProcessorManager中注册的名称）
     */
    private String processorName;

    /**
     * 节点别名（用于显示）
     */
    private String alias;

    /**
     * 节点优先级（越小越先执行）
     */
    private int priority;

    /**
     * 是否启用
     */
    private boolean enabled = true;

    /**
     * 节点配置（会传递给处理器）
     */
    private Object config;

    public ProcessorNode() {
    }

    public ProcessorNode(String processorName, String alias, int priority) {
        this.processorName = processorName;
        this.alias = alias;
        this.priority = priority;
    }

    public ProcessorNode(String processorName, String alias, int priority, boolean enabled) {
        this.processorName = processorName;
        this.alias = alias;
        this.priority = priority;
        this.enabled = enabled;
    }
}
