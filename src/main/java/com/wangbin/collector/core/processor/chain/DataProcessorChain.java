package com.wangbin.collector.core.processor.chain;

import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.core.processor.DataProcessor;
import com.wangbin.collector.core.processor.ProcessContext;
import com.wangbin.collector.core.processor.ProcessResult;
import com.wangbin.collector.core.processor.manager.DataProcessorManager;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 数据处理器链
 * 负责按照顺序执行处理器节点
 */
@Slf4j
@Data
public class DataProcessorChain {

    /**
     * 链名称
     */
    private String name;

    /**
     * 链描述
     */
    private String description;

    /**
     * 是否启用
     */
    private boolean enabled = true;

    /**
     * 是否正在运行
     */
    private volatile boolean running = false;

    /**
     * 出错时是否继续执行
     */
    private boolean continueOnError = false;

    /**
     * 处理器节点列表
     */
    private List<ProcessorNode> nodes = new CopyOnWriteArrayList<>();

    /**
     * 处理器管理器
     */
    private DataProcessorManager dataProcessorManager;

    // 统计信息
    private AtomicLong totalExecutions = new AtomicLong(0);
    private AtomicLong successfulExecutions = new AtomicLong(0);
    private AtomicLong failedExecutions = new AtomicLong(0);
    private AtomicLong totalExecutionTime = new AtomicLong(0);
    private AtomicInteger currentConcurrency = new AtomicInteger(0);
    private AtomicInteger maxConcurrency = new AtomicInteger(0);

    /**
     * 默认构造函数
     */
    public DataProcessorChain() {
    }

    /**
     * 带参数的构造函数
     */
    public DataProcessorChain(String name, String description) {
        this.name = name;
        this.description = description;
    }

    /**
     * 初始化链
     */
    public void init() {
        if (running) {
            log.warn("处理器链已经在运行中: {}", name);
            return;
        }

        try {
            // 对节点按优先级排序
            nodes.sort(Comparator.comparingInt(ProcessorNode::getPriority));

            // 验证所有节点
            validateNodes();

            running = true;

            log.info("处理器链初始化成功: {}, 节点数: {}", name, nodes.size());

        } catch (Exception e) {
            log.error("处理器链初始化失败: {}", name, e);
            throw new RuntimeException("处理器链初始化失败: " + name, e);
        }
    }

    /**
     * 停止链
     */
    public void stop() {
        if (!running) {
            return;
        }

        running = false;
        log.info("处理器链已停止: {}", name);
    }

    /**
     * 验证节点配置
     */
    private void validateNodes() {
        if (dataProcessorManager == null) {
            throw new IllegalStateException("处理器管理器未设置");
        }

        for (ProcessorNode node : nodes) {
            if (node.getProcessorName() == null || node.getProcessorName().isEmpty()) {
                throw new IllegalArgumentException("处理器节点名称不能为空");
            }

            DataProcessor processor = dataProcessorManager.getProcessor(node.getProcessorName());
            if (processor == null) {
                throw new IllegalArgumentException("处理器不存在: " + node.getProcessorName());
            }

            if (!processor.isEnabled()) {
                log.warn("处理器已禁用，但仍在链中: {}", node.getProcessorName());
            }
        }
    }

    /**
     * 执行处理器链
     */
    public ProcessResult execute(ProcessContext context, DataPoint point, Object rawValue) {
        if (!enabled || !running) {
            return ProcessResult.skip(rawValue, "处理器链未启用或未运行");
        }

        long startTime = System.currentTimeMillis();
        currentConcurrency.incrementAndGet();
        maxConcurrency.updateAndGet(curr -> Math.max(curr, currentConcurrency.get()));

        try {
            log.debug("开始执行处理器链: {}, 数据点: {}", name, point.getPointName());

            ProcessResult finalResult = null;
            int processedNodes = 0;
            boolean chainSuccess = true;

            // 依次执行所有启用的处理器节点
            for (ProcessorNode node : nodes) {
                if (!node.isEnabled()) {
                    log.debug("处理器节点已禁用，跳过: {}", node.getProcessorName());
                    continue;
                }

                DataProcessor processor = dataProcessorManager.getProcessor(node.getProcessorName());
                if (processor == null || !processor.isEnabled()) {
                    log.warn("处理器不存在或已禁用: {}", node.getProcessorName());
                    if (!continueOnError) {
                        chainSuccess = false;
                        break;
                    }
                    continue;
                }

                // 准备当前处理的输入值
                Object inputValue = finalResult != null ? finalResult.getFinalValue() : rawValue;

                // 执行处理器
                ProcessResult nodeResult = processor.process(context, point, inputValue);
                processedNodes++;

                // 记录处理历史
                if (nodeResult != null) {
                    context.addProcessHistory(processor.getName(), nodeResult);

                    // 合并结果
                    if (finalResult == null) {
                        finalResult = nodeResult;
                    } else {
                        finalResult = finalResult.merge(nodeResult);
                    }

                    // 检查处理结果
                    if (!nodeResult.isSuccess() && !nodeResult.isSkipped()) {
                        log.warn("处理器执行失败: processor={}, point={}, error={}",
                                processor.getName(), point.getPointName(), nodeResult.getError());

                        if (!continueOnError) {
                            chainSuccess = false;
                            break;
                        }
                    }
                } else {
                    log.warn("处理器返回空结果: {}", processor.getName());
                    if (!continueOnError) {
                        chainSuccess = false;
                        break;
                    }
                }
            }

            // 更新统计信息
            totalExecutions.incrementAndGet();
            if (chainSuccess) {
                successfulExecutions.incrementAndGet();
            } else {
                failedExecutions.incrementAndGet();
            }

            long executionTime = System.currentTimeMillis() - startTime;
            totalExecutionTime.addAndGet(executionTime);

            // 创建最终结果
            if (finalResult == null) {
                finalResult = ProcessResult.success(rawValue, rawValue,
                        String.format("处理器链执行完成，处理节点数: %d", processedNodes));
            }

            finalResult.setProcessorName(name);
            finalResult.setProcessingTime(executionTime);

            log.debug("处理器链执行完成: {}, 成功: {}, 耗时: {}ms",
                    name, chainSuccess, executionTime);

            return finalResult;

        } catch (Exception e) {
            failedExecutions.incrementAndGet();
            log.error("处理器链执行异常: {}, point={}", name, point.getPointName(), e);
            return ProcessResult.error(rawValue, "处理器链执行异常: " + e.getMessage());

        } finally {
            currentConcurrency.decrementAndGet();
        }
    }

    /**
     * 批量执行处理器链
     */
    public Map<String, ProcessResult> batchExecute(ProcessContext context,
                                                   Map<DataPoint, Object> dataPoints) {
        if (!enabled || !running || dataPoints == null || dataPoints.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, ProcessResult> results = new HashMap<>(dataPoints.size());

        log.debug("批量执行处理器链: {}, 数据点数量: {}", name, dataPoints.size());

        for (Map.Entry<DataPoint, Object> entry : dataPoints.entrySet()) {
            ProcessResult result = execute(context, entry.getKey(), entry.getValue());
            results.put(entry.getKey().getPointId(), result);
        }

        log.debug("处理器链批量执行完成: {}, 成功数量: {}",
                name, results.values().stream().filter(ProcessResult::isSuccess).count());

        return results;
    }

    /**
     * 添加处理器节点
     */
    public void addNode(ProcessorNode node) {
        if (node == null) {
            throw new IllegalArgumentException("处理器节点不能为空");
        }

        nodes.add(node);
        // 重新排序
        nodes.sort(Comparator.comparingInt(ProcessorNode::getPriority));

        log.debug("添加处理器节点: chain={}, node={}, priority={}",
                name, node.getProcessorName(), node.getPriority());
    }

    /**
     * 删除处理器节点
     */
    public boolean removeNode(String processorName) {
        boolean removed = nodes.removeIf(node -> processorName.equals(node.getProcessorName()));
        if (removed) {
            log.debug("删除处理器节点: chain={}, node={}", name, processorName);
        }
        return removed;
    }

    /**
     * 启用/禁用处理器节点
     */
    public void setNodeEnabled(String processorName, boolean enabled) {
        for (ProcessorNode node : nodes) {
            if (processorName.equals(node.getProcessorName())) {
                node.setEnabled(enabled);
                log.debug("设置处理器节点状态: chain={}, node={}, enabled={}",
                        name, processorName, enabled);
                break;
            }
        }
    }

    /**
     * 获取处理器节点
     */
    public ProcessorNode getNode(String processorName) {
        for (ProcessorNode node : nodes) {
            if (processorName.equals(node.getProcessorName())) {
                return node;
            }
        }
        return null;
    }

    /**
     * 获取节点数量
     */
    public int getNodeCount() {
        return nodes.size();
    }

    /**
     * 获取启用的节点数量
     */
    public int getEnabledNodeCount() {
        return (int) nodes.stream().filter(ProcessorNode::isEnabled).count();
    }

    /**
     * 获取链统计信息
     */
    public Map<String, Object> getStatistics() {
        Map<String, Object> stats = new HashMap<>();

        stats.put("name", name);
        stats.put("description", description);
        stats.put("enabled", enabled);
        stats.put("running", running);
        stats.put("continueOnError", continueOnError);
        stats.put("nodeCount", getNodeCount());
        stats.put("enabledNodeCount", getEnabledNodeCount());
        stats.put("totalExecutions", totalExecutions.get());
        stats.put("successfulExecutions", successfulExecutions.get());
        stats.put("failedExecutions", failedExecutions.get());
        stats.put("successRate", getSuccessRate());
        stats.put("totalExecutionTime", totalExecutionTime.get());
        stats.put("averageExecutionTime", getAverageExecutionTime());
        stats.put("currentConcurrency", currentConcurrency.get());
        stats.put("maxConcurrency", maxConcurrency.get());

        // 节点详情
        List<Map<String, Object>> nodeStats = new ArrayList<>();
        for (ProcessorNode node : nodes) {
            Map<String, Object> nodeInfo = new HashMap<>();
            nodeInfo.put("processorName", node.getProcessorName());
            nodeInfo.put("alias", node.getAlias());
            nodeInfo.put("priority", node.getPriority());
            nodeInfo.put("enabled", node.isEnabled());
            nodeStats.add(nodeInfo);
        }
        stats.put("nodes", nodeStats);

        return stats;
    }

    /**
     * 获取成功率
     */
    private double getSuccessRate() {
        long total = totalExecutions.get();
        if (total == 0) {
            return 0.0;
        }
        return (double) successfulExecutions.get() / total * 100;
    }

    /**
     * 获取平均执行时间
     */
    private long getAverageExecutionTime() {
        long total = totalExecutions.get();
        if (total == 0) {
            return 0;
        }
        return totalExecutionTime.get() / total;
    }

    /**
     * 重置统计信息
     */
    public void resetStatistics() {
        totalExecutions.set(0);
        successfulExecutions.set(0);
        failedExecutions.set(0);
        totalExecutionTime.set(0);
        currentConcurrency.set(0);
        maxConcurrency.set(0);
        log.info("处理器链统计重置: {}", name);
    }

    /**
     * 检查链是否有效
     */
    public boolean isValid() {
        if (name == null || name.isEmpty()) {
            return false;
        }

        if (nodes.isEmpty()) {
            return false;
        }

        return true;
    }

    /**
     * 克隆链（用于创建副本）
     */
    public DataProcessorChain clone() {
        DataProcessorChain clone = new DataProcessorChain();
        clone.name = this.name;
        clone.description = this.description;
        clone.enabled = this.enabled;
        clone.continueOnError = this.continueOnError;
        clone.dataProcessorManager = this.dataProcessorManager;

        // 深拷贝节点
        for (ProcessorNode node : this.nodes) {
            ProcessorNode nodeClone = new ProcessorNode();
            nodeClone.setProcessorName(node.getProcessorName());
            nodeClone.setAlias(node.getAlias());
            nodeClone.setPriority(node.getPriority());
            nodeClone.setEnabled(node.isEnabled());
            Object config = node.getConfig();
            if (config instanceof Map<?, ?> map) {
                nodeClone.setConfig(new HashMap<>(map));
            } else {
                nodeClone.setConfig(config);
            }
            clone.nodes.add(nodeClone);
        }

        return clone;
    }
}
