package com.wangbin.collector.core.processor.manager;

import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.core.processor.DataProcessor;
import com.wangbin.collector.core.processor.ProcessContext;
import com.wangbin.collector.core.processor.ProcessResult;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.core.annotation.AnnotationAwareOrderComparator;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 数据处理器管理器
 */
@Slf4j
@Component
public class DataProcessorManager {

    @Autowired
    private ApplicationContext applicationContext;

    /**
     * 所有处理器映射
     */
    private Map<String, DataProcessor> processorMap = new ConcurrentHashMap<>();

    /**
     * 已排序的处理器列表（优先级从高到低）
     */
    private List<DataProcessor> sortedProcessors = new ArrayList<>();

    /**
     * 处理器配置
     */
    private Map<String, Map<String, Object>> processorConfigs = new ConcurrentHashMap<>();

    /**
     * 初始化管理器
     */
    @PostConstruct
    public void init() {
        log.info("开始初始化数据处理器管理器...");

        // 从Spring容器中获取所有DataProcessor实例
        Map<String, DataProcessor> processors = applicationContext.getBeansOfType(DataProcessor.class);

        for (Map.Entry<String, DataProcessor> entry : processors.entrySet()) {
            DataProcessor processor = entry.getValue();
            String processorName = processor.getName();

            try {
                // 获取处理器配置
                Map<String, Object> config = processorConfigs.getOrDefault(processorName, new HashMap<>());

                // 初始化处理器
                processor.init(config);

                // 注册处理器
                registerProcessor(processor);

                log.info("数据处理器初始化成功: {} [{}]", processorName, processor.getType());
            } catch (Exception e) {
                log.error("数据处理器初始化失败: {}", processorName, e);
            }
        }

        // 排序处理器（优先级从高到低）
        sortProcessors();

        log.info("数据处理器管理器初始化完成，共加载 {} 个处理器", sortedProcessors.size());
    }

    /**
     * 销毁管理器
     */
    @PreDestroy
    public void destroy() {
        log.info("开始销毁数据处理器管理器...");

        // 销毁所有处理器
        for (DataProcessor processor : sortedProcessors) {
            try {
                processor.destroy();
                log.info("数据处理器销毁成功: {}", processor.getName());
            } catch (Exception e) {
                log.error("数据处理器销毁失败: {}", processor.getName(), e);
            }
        }

        processorMap.clear();
        sortedProcessors.clear();
        log.info("数据处理器管理器销毁完成");
    }

    /**
     * 注册处理器
     */
    public void registerProcessor(DataProcessor processor) {
        String processorName = processor.getName();

        if (processorMap.containsKey(processorName)) {
            log.warn("数据处理器已存在，将替换: {}", processorName);
        }

        processorMap.put(processorName, processor);

        // 重新排序
        if (!sortedProcessors.contains(processor)) {
            sortedProcessors.add(processor);
            sortProcessors();
        }

        log.info("数据处理器注册成功: {} [优先级: {}]", processorName, processor.getPriority());
    }

    /**
     * 注销处理器
     */
    public void unregisterProcessor(String processorName) {
        DataProcessor processor = processorMap.remove(processorName);
        if (processor != null) {
            sortedProcessors.remove(processor);
            log.info("数据处理器注销成功: {}", processorName);
        }
    }

    /**
     * 获取处理器
     */
    public DataProcessor getProcessor(String processorName) {
        return processorMap.get(processorName);
    }

    /**
     * 获取所有处理器
     */
    public List<DataProcessor> getAllProcessors() {
        return new ArrayList<>(sortedProcessors);
    }

    /**
     * 根据类型获取处理器
     */
    public List<DataProcessor> getProcessorsByType(String type) {
        List<DataProcessor> result = new ArrayList<>();
        for (DataProcessor processor : sortedProcessors) {
            if (type.equals(processor.getType())) {
                result.add(processor);
            }
        }
        return result;
    }

    /**
     * 启用/禁用处理器
     */
    public void setProcessorEnabled(String processorName, boolean enabled) {
        DataProcessor processor = processorMap.get(processorName);
        if (processor != null) {
            processor.setEnabled(enabled);
        }
    }

    /**
     * 处理数据点
     */
    public ProcessResult process(ProcessContext context, DataPoint point, Object rawValue) {
        log.debug("开始处理数据点: point={}, rawValue={}", point.getPointName(), rawValue);

        ProcessResult finalResult = null;

        // 依次执行所有启用的处理器
        for (DataProcessor processor : sortedProcessors) {
            if (!processor.isEnabled()) {
                log.debug("处理器已禁用，跳过: {}", processor.getName());
                continue;
            }

            ProcessResult result = processor.process(context, point,
                    finalResult != null ? finalResult.getFinalValue() : rawValue);

            // 记录处理历史
            if (result != null) {
                context.addProcessHistory(processor.getName(), result);

                // 更新最终结果
                if (finalResult == null) {
                    finalResult = result;
                } else {
                    finalResult = finalResult.merge(result);
                }

                // 如果处理失败，可以决定是否继续处理
                if (!result.isSuccess() && !result.isSkipped()) {
                    log.warn("处理器执行失败，停止处理链: processor={}, point={}",
                            processor.getName(), point.getPointName());
                    break;
                }
            }
        }

        if (finalResult == null) {
            finalResult = ProcessResult.success(rawValue, rawValue, "无可用处理器");
        }

        log.debug("数据点处理完成: point={}, success={}, quality={}",
                point.getPointName(), finalResult.isSuccess(), finalResult.getQuality());

        return finalResult;
    }

    /**
     * 批量处理数据点
     */
    public Map<String, ProcessResult> batchProcess(ProcessContext context,
                                                   Map<DataPoint, Object> dataPoints) {
        log.debug("开始批量处理数据点，数量: {}", dataPoints.size());

        Map<String, ProcessResult> results = new HashMap<>();

        for (Map.Entry<DataPoint, Object> entry : dataPoints.entrySet()) {
            ProcessResult result = process(context, entry.getKey(), entry.getValue());
            results.put(entry.getKey().getPointId(), result);
        }

        log.debug("批量数据处理完成，成功数量: {}",
                results.values().stream().filter(ProcessResult::isSuccess).count());

        return results;
    }

    /**
     * 获取所有处理器统计信息
     */
    public Map<String, Object> getAllStatistics() {
        Map<String, Object> allStats = new HashMap<>();

        for (DataProcessor processor : sortedProcessors) {
            Map<String, Object> stats = processor.getStatistics();
            allStats.put(processor.getName(), stats);
        }

        return allStats;
    }

    /**
     * 重置所有处理器统计
     */
    public void resetAllStatistics() {
        for (DataProcessor processor : sortedProcessors) {
            processor.resetStatistics();
        }
        log.info("所有数据处理器统计已重置");
    }

    /**
     * 设置处理器配置
     */
    public void setProcessorConfig(String processorName, Map<String, Object> config) {
        processorConfigs.put(processorName, config);

        DataProcessor processor = processorMap.get(processorName);
        if (processor != null) {
            try {
                processor.init(config);
                log.info("数据处理器配置更新成功: {}", processorName);
            } catch (Exception e) {
                log.error("数据处理器配置更新失败: {}", processorName, e);
            }
        }
    }

    /**
     * 排序处理器（按优先级升序，数值越小优先级越高）
     */
    private void sortProcessors() {
        sortedProcessors.sort((p1, p2) -> {
            int priority1 = p1.getPriority();
            int priority2 = p2.getPriority();
            return Integer.compare(priority1, priority2);
        });
    }
}